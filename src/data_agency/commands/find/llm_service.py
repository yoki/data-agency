"""LLM service for find functionality."""

import json
from typing import Union, cast

from autogen_core import FunctionCall
from autogen_core.models import UserMessage

from ...common.llm_client import FullLogChatClientCache
from .models import (
    CategorySelectionAccessment,
    FindDataSeriesParams,
    FilterSet,
    RaiseAlertParam,
    SynthesisSearchResult,
    FindMetadataResult,
)
from .search_service import SearchService

# Prompt templates as constants
DECOMPOSE_QUERY_PROMPT_TEMPLATE = """
You are a specialized agent responsible for the first step of data discovery. Your sole purpose is to translate a user's natural language query into a structured function call.

**Process:**
1.  **Analyze the Request**: Carefully examine the user's query to understand their data requirements. This is first stage of the data discovery process, and your goal is to be as comprehensive as possible in identifying the user's potential needs.
2. Only categories in the "available categories" are in the dataset. Do not assume any other categories exists.
2.  **Invoke Tool for Search**: If the request is clear, call the `find_series_from_manifest` function.
3.  **Invoke Tool for Clarification**: If the user's request is not avaiable in dataset(e.g., "health"), you can call the `raise_fatal_alert` tool with a direct reason. It must be used only when the user's request is obviously not available in the dataset. If user's request is somewhat broad, assume and proceed. 

You must call one of these two functions.

**User Query:** "{query}"

{additional_prompt}

**Available Categories for Filtering:**
{categories}
"""

SYNTHESIZE_SEARCH_RESULTS_PROMPT_TEMPLATE = """
You are an expert macro-financial data analyst serving for 20 years experienced macro-financial data analyst.

**Task:**
Analyze the provided `raw_series_results` and `Available Categories` to find the best data for the `User's Original Query`. Since deleting extra few variables are much easier than adding new variables for user, be as broad as possible. User might have various needs, such as using nominal value to calculate the ratio of nominal raw data, or using non-seasonally adjusted data for broader coverage. 

** Variable name rule **
Use small letters and underscores.
Refer level names as well as variable names to construct the variable name. For example, if variable name is "debt security" and level2 is liability, the variable name should be "liab_debt", not "debt".
*Must* be as short as possible. Use abbreviations as appropriate.


**Process:**
1.  **Critique & Select:** First, determine if the `raw_series_results` are appropriate to access the user's query. If somewhat appropriate, select the most relevant series. If not, return zero results.
2.  **Report:** Provide a technical explanation for your final choice. Focus on technical limitation of the current choice and your ability, and detailed data you decided to omit but which could be relevant for user's analysis. Never flag standard data transformations or common statistical practices. Suggestion must be based on the data available in the broad dataset. 

**User's Original Query:** 
"{query}"

**Raw Search Results (tsv):**
{series_table}

**Available Categories for Filtering:**
{categories}
"""


SYNTHESIZE_EXPLAIN_RESULTS_PROMPT_TEMPLATE = """
You are an expert macro-financial data analyst serving for 20 years experienced macro-financial data analyst.

**Task:**
Given provided `raw_series_results` and `Available Categories`, select the variables based on `User's Original Query`. 

**User's Original Query:** 
"{query}"

**Raw Search Results (tsv):**
{series_table}

**Available Categories for Filtering:**
{categories}
"""

CATEGORY_SELECTION_ASSESSMENT_PROMPT_TEMPLATE = """
You are an expert macro-financial data analyst.

**Task:**
Analyze the provided `raw_series_results` and `Available Categories` to determine if the results are appropriate for the `User's Original Query`. Your only output is a decision on how to proceed.

**Process:**
1.  **Analyze Query:** Understand the core economic or financial concept in the `User's Original Query`.
2.  **Evaluate Results:** Examine the metadata of the variables in `raw_series_results`. Do these variables directly address the user's query?
3.  **Decide Action:**
    * If the `raw_series_results` are relevant and sufficient for selection, decide to **ACCEPT_VARIABLES** them.
    * If the variables are from a conceptually incorrect category, decide to **SWITCH_CATEGOREIS** and identify the correct category from the `Available Categories` list.
4.  Provide a concise technical justification for your decision in markdown text.

**Inputs:**
* `User's Original Query`: "{query}"

* `Raw Search Results (tsv)`: 
{series_table}

* `Available Categories`: 
{categories}
"""


FIND_SEREIS_MANIFEST = {
    "type": "function",
    "name": "find_series",
    "description": "Find data series identifiers based on specified categorization and source file filters. ",
    "parameters": {
        "type": "object",
        "properties": {
            "filters": {
                "description": 'A list of dictionaries, where each dictionary represents a set of filtering criteria for data series. Each filter dictionary can include "source_file" (e.g., "bis_locational_a5.csv", "fed_z1.csv") and up to three categorical levels: "level1", "level2", and "level3". All specified criteria within a single filter dictionary must be met for a series to be included. Multiple filter dictionaries allow for OR conditions across different sets of criteria.',
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "source_file": {
                            "type": "string",
                            "description": 'The name of the source data file (e.g., "bis_locational_a5.csv").',
                        },
                        "level1": {
                            "type": "string",
                            "description": 'The primary classification level (e.g., "BIS", "DebtSecurity").',
                        },
                        "level2": {
                            "type": "string",
                            "description": 'The secondary classification level (e.g., "liability", "interest").',
                        },
                        "level3": {
                            "type": "string",
                            "description": 'The tertiary classification level (e.g., "PrimaryIncome", "MaturitySector").',
                        },
                    },
                    "additionalProperties": False,
                },
            }
        },
        "required": ["filters"],
        "additionalProperties": False,
    },
}

RAISE_FATAL_ERRORS_MANIFEST = {
    "type": "function",
    "name": "raise_fatal_alert",
    "description": "Raise critical issues from system to the user. It must be used only when the user's request is obviously not available in the dataset. If user's request is somewhat broad, assume and proceed.",
    "parameters": {
        "type": "object",
        "properties": {
            "message": {
                "type": "string",
                "description": "The clarification message to be displayed to the user.",
            },
            "reason": {
                "type": "string",
                "description": "The underlying reason why clarification is necessary.",
            },
        },
        "required": ["message", "reason"],
    },
}


class FindDataLLMService:
    """Service for LLM interactions specific to find functionality."""

    def __init__(self, client: FullLogChatClientCache):
        """Initialize the LLM service."""
        self.client = client
        self.avaiable_categories = SearchService().get_available_categories().to_csv(index=False)

    def additional_prompt_for_no_results(self, search_result: FindMetadataResult) -> str:
        out = f"**warning**\nyour previous search attempt returned zero result because you chose nonexisting filter conditions. You must stick to exiting compbination.\nprevious filters: \n{search_result.search_params}"

        return out

    async def decompose_query(
        self, query: str, additional_prompt: str
    ) -> Union[FindDataSeriesParams, RaiseAlertParam]:
        """Convert natural language to structured search parameters.

        Args:
            query: User's natural language query
            categories: Available categories as CSV string

        Returns:
            Either search parameters or an alert to raise

        Raises:
            QueryDecompositionError: If LLM query decomposition fails
        """
        if additional_prompt is None:
            query = (
                query
                + "\n"
                + "your previous search attempt returned zero result because you chose nonexisting file and category combination. You must stick to exiting compbination."
            )
        prompt = DECOMPOSE_QUERY_PROMPT_TEMPLATE.format(
            query=query, categories=self.avaiable_categories, additional_prompt=additional_prompt
        )

        tools = [FIND_SEREIS_MANIFEST, RAISE_FATAL_ERRORS_MANIFEST]

        response = await self.client.create(
            messages=[UserMessage(content=prompt, source="user")],
            tools=tools,
            tool_choice="required",
        )

        tool_call = cast(FunctionCall, response.content[0])
        function_name = tool_call.name

        if function_name == "find_series":
            filter_sets = []
            for cont in response.content:
                obj = json.loads(cont.arguments)  # type: ignore

                for filt_obj in obj.get("filters", []):
                    filterset = FilterSet(**filt_obj)
                    filter_sets.append(filterset)

            return FindDataSeriesParams(filters=filter_sets)
        elif function_name == "raise_fatal_alert":
            arguments = json.loads(tool_call.arguments)
            return RaiseAlertParam(**arguments)  # type: ignore
        else:
            raise RuntimeError(f"Unexpected tool call: {function_name} from LLM.")

    async def assess_results(self, query: str, results: FindMetadataResult) -> CategorySelectionAccessment:
        """Analyze and curate search results.

        Args:
            query: User's original query
            results: Search results to analyze

        Returns:
            Synthesized results with recommendations

        Raises:
            SynthesisError: If LLM synthesis fails
        """
        series_table = results.get_tsv_printout_string()
        prompt = CATEGORY_SELECTION_ASSESSMENT_PROMPT_TEMPLATE.format(
            query=query,
            series_table=series_table,
            categories=self.avaiable_categories,
        )
        response = await self.client.create(
            messages=[UserMessage(content=prompt, source="user")], json_output=CategorySelectionAccessment
        )

        args = json.loads(response.content)  # type: ignore
        return CategorySelectionAccessment(**args)

    async def synthesize_search_results(self, query: str, results: FindMetadataResult) -> SynthesisSearchResult:
        """Analyze and curate search results.

        Args:
            query: User's original query
            results: Search results to analyze

        Returns:
            Synthesized results with recommendations

        Raises:
            SynthesisError: If LLM synthesis fails
        """
        series_table = results.get_tsv_printout_string()
        prompt = SYNTHESIZE_SEARCH_RESULTS_PROMPT_TEMPLATE.format(
            query=query,
            series_table=series_table,
            categories=self.avaiable_categories,
        )
        response = await self.client.create(
            messages=[UserMessage(content=prompt, source="user")], json_output=SynthesisSearchResult
        )

        args = json.loads(response.content)  # type: ignore
        return SynthesisSearchResult(**args)

    async def synthesize_explain_results(self, query: str, results: FindMetadataResult) -> str:
        """Analyze and curate search results.

        Args:
            query: User's original query
            results: Search results to analyze

        Returns:
            Synthesized results with recommendations

        Raises:
            SynthesisError: If LLM synthesis fails
        """
        series_table = results.get_tsv_printout_string()
        prompt = SYNTHESIZE_EXPLAIN_RESULTS_PROMPT_TEMPLATE.format(
            query=query,
            series_table=series_table,
            categories=self.avaiable_categories,
        )
        response = await self.client.create(messages=[UserMessage(content=prompt, source="user")])

        return response.content  # type: ignore
