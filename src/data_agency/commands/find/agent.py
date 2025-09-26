"""
Agent module for the find command.
Orchestrates the entire workflow for data discovery.
"""

from typing import Union, cast


from .display_service import DisplayService
from .search_service import SearchService
from .llm_service import FindDataLLMService
from .models import FilterSet, FindMetadataResult, FindDataSeriesParams, RaiseAlertParam
from data_agency.common.llm_client import create_client, LLMModels, FullLogChatClientCache


class DataFindAgent:
    """
    Main orchestrator for the find command.
    Coordinates search, LLM, and display services to process user queries.
    """

    def __init__(self, client: FullLogChatClientCache = None):  # type: ignore
        """Initialize the agent with required services."""
        if client is None:
            client = create_client(model=LLMModels.GEMINI25_FLASH)

        self.search_service = SearchService()
        self.llm_service = FindDataLLMService(client=client)
        self.display_service = DisplayService()

    async def run(self, line: str, cell: str) -> None:
        """
        Main entry point for data discovery workflow.

        Args:
            query: User's query string
        """
        # Apply custom styles for better readability
        self.display_service.apply_custom_styles()

        # Handle utility commands first
        if await self._handle_utility_commands(line, cell):
            return

        # Process natural language query
        await self._process_natural_language_query(line + " " + cell)

    async def _handle_utility_commands(self, line: str, cell: str) -> bool:
        """
        Handle utility commands like help, database, categories, variables.

        Args:
            query: User's query string

        Returns:
            True if a utility command was handled, False otherwise
        """
        command = line.lower().strip().split(" ")[0]

        if command in ["help", "h", "?"] or not (line.strip() or cell.strip()):
            self.display_service.show_help()
            return True

        if command in ["database", "source", "databases", "sources"]:
            sources_df = self.search_service.get_sources_dataframe()
            self.display_service.show_database_list(sources_df)
            return True

        if command in ["categories", "category"]:
            categories_df = self.search_service.get_available_categories()
            self.display_service.show_categories(categories_df)
            return True

        if command in ["keyword", "keywords"]:
            await self._run_keywords(line, cell)
            return True

        if command in ["explain", "metadata"]:
            await self._run_explain(line, cell)
            return True

        # Variables command
        parts = line.strip().split()
        if len(parts) == 2 and "variable" in parts[0].lower():
            file_name = parts[1]
            result = self.search_service.get_variables_by_source(file_name)

            if not result.series:
                self.display_service.show_file_not_found(file_name)
                return True

            df = result.to_df()
            self.display_service.show_variables(df, file_name, result.count)
            return True

        return False

    async def _process_search_single(self, query: str, additional_prompt: str = "") -> Union[FindMetadataResult, None]:
        decomposition_result = await self.llm_service.decompose_query(query, additional_prompt)

        # Handle clarification requests
        if isinstance(decomposition_result, RaiseAlertParam):
            self.display_service.show_clarification_needed(decomposition_result.message)
            return

        # # Show search filters
        search_params = cast(FindDataSeriesParams, decomposition_result)
        self.display_service.show_search_filters(search_params.to_markdown())

        # 2. Search for matching series
        search_results = self.search_service.find_series(search_params)

        # Show result count
        self.display_service.show_search_results_count(search_results.count)

        return search_results

    async def _process_natural_language_query(self, query: str) -> None:
        """
        Process a natural language query through the full workflow.

        Args:
            query: User's natural language query
        """

        # 1. Decompose query into structured parameters
        # first try
        search_results = await self._process_search_single(query)

        if search_results is None:
            # User clarificaion requestd
            return

        if search_results.count == 0:
            # retry with different query
            additional_prompt = self.llm_service.additional_prompt_for_no_results(search_results)
            search_results = await self._process_search_single(query, additional_prompt=additional_prompt)
            if search_results is None:
                # User clarificaion requestd
                return

        # 3. Assess the selection of categories and redo category selection if needed
        selection_assessment = await self.llm_service.assess_results(query, search_results)
        self.display_service.show_selection_assessment(selection_assessment)
        if selection_assessment.action == "SWITCH_CATEGOREIS":
            search_results = await self._process_search_single(
                query,
                additional_prompt=selection_assessment.guidance_for_category_select_agent,  # type: ignore
            )

        if search_results is None:
            # User clarificaion requestd
            return

        synthesis_result = await self.llm_service.synthesize_search_results(query, search_results)  # type: ignore

        # 4. Format and display results
        final_output = self.display_service.format_and_display_search_results(synthesis_result, search_results.series)  # type: ignore

        # 5. Display final output
        self.display_service.display_final_output(final_output)

    async def _run_explain(self, line: str, cell: str) -> None:
        query = line.strip() + " " + cell.strip()

        # first find category
        search_results = await self._process_search_single(query)

        if search_results is None:
            # User clarificaion requestd
            return

        if search_results.count == 0:
            # retry with different query
            additional_prompt = self.llm_service.additional_prompt_for_no_results(search_results)
            search_results = await self._process_search_single(query, additional_prompt=additional_prompt)
            if search_results is None:
                # User clarificaion requestd
                return

        # 3. Assess the selection of categories and redo category selection if needed
        selection_assessment = await self.llm_service.assess_results(query, search_results)
        self.display_service.show_selection_assessment(selection_assessment)
        if selection_assessment.action == "SWITCH_CATEGOREIS":
            search_results = await self._process_search_single(
                query,
                additional_prompt=selection_assessment.guidance_for_category_select_agent,  # type: ignore
            )

        synthesis_result = await self.llm_service.synthesize_explain_results(query, search_results)  # type: ignore

        # 4. Format and display results
        final_output = self.display_service.format_and_display_explain_results(synthesis_result, search_results.series)  # type: ignore

        # 5. Display final output
        self.display_service.display_final_output(final_output)

    async def _run_keywords(self, line: str, cell: str) -> None:
        keywords = (" ".join(line.split(" ")[1:]) + "\n" + cell).splitlines()
        keywords_list = [k.strip().split(" ") for k in keywords if k.strip()]
        if not keywords_list:
            return

        keywords_list = self.search_service.normalize_keywords_for_category(keywords_list)
        # keywords = keywords[:4]

        field_names = ["source_file", "level1", "level2", "level3"]
        keyword_filterset = [
            FilterSet(**{field: keywords[i] if i < len(keywords) else None for i, field in enumerate(field_names)})
            for keywords in keywords_list
        ]
        # keyword_filterset = [FilterSet(*keywords[:4]) for keywords in keywords_list]  # type: ignore

        params = FindDataSeriesParams(filters=keyword_filterset)
        searched = self.search_service.find_series(params)
        output = self.display_service.format_and_display_keyword_search_results(searched.series)
        self.display_service.display_final_output(output)
