"""
Display service for the find command.
Handles all Jupyter display logic and output formatting.
"""

import pandas as pd
from typing import List
from IPython.display import display, Markdown, HTML
from IPython.core.getipython import get_ipython

from .models import CategorySelectionAccessment, SeriesMetadata, SynthesisSearchResult, FinalOutput


class DisplayService:
    """
    Handles all display and formatting operations for the find command.
    Responsible for showing help documentation, database lists, and formatting results.
    """

    def __init__(self):
        """Initialize the display service."""
        self.ipython_shell = get_ipython()

    def apply_custom_styles(self):
        """Apply custom styles to improve readability in Jupyter notebooks."""
        style = """
        <style>
            .jp-RenderedMarkdown p, .jp-RenderedMarkdown li {
                font-size: 16px; /* Adjust paragraph and list item font size */
            }
            .jp-RenderedMarkdown h1 {
                font-size: 32px; /* Adjust H1 font size */
            }
            .jp-RenderedMarkdown h2 {
                font-size: 28px; /* Adjust H2 font size */
            }
        </style>
        """
        display(HTML(style))

    def show_help(self):
        """Display help documentation for the find command."""
        display(Markdown(HELP_TEXT))

    def show_database_list(self, sources_df: pd.DataFrame):
        styles = {
            "white-space": "normal",
            "word-wrap": "break-word",  # For older browser compatibility
            "overflow-wrap": "break-word",
            "max-width": "300px",  # Adjust this width as needed
        }
        styled_df = sources_df.style.set_properties(subset=["description"], **styles)
        display(styled_df)

    def show_categories(self, categories_df: pd.DataFrame):
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            display(categories_df)

    def show_variables(self, variables_df: pd.DataFrame, file_name: str, count: int):
        display(variables_df)
        display(Markdown(f"`_all_meta` DataFrame has been created with all {count} variables from `{file_name}`."))

    def show_search_filters(self, filters_markdown: str):
        display(Markdown(f"**Search Filters:**\n{filters_markdown}"))

    def show_search_results_count(self, count: int):
        display(Markdown(f"Found {count} series matching query."))

    def show_no_results(self):
        """Display a message when no results are found."""
        display(Markdown("No data series found matching your query."))

    def show_clarification_needed(self, message: str):
        display(Markdown(f"**Clarification Needed:** {message}"))

    def show_file_not_found(self, file_name: str):
        display(Markdown(f"File not found for source file `{file_name}`."))

    def show_selection_assessment(self, selection_assessment: CategorySelectionAccessment):
        if selection_assessment.action == "SWITCH_CATEGOREIS":
            display(
                Markdown(
                    f"**Category Selection Assessment:** {selection_assessment.justification}\n\n\n**Guidance For Category Select Agent:** {selection_assessment.guidance_for_category_select_agent}"
                )
            )
        else:
            display(Markdown(f"Category Selection is appropriate."))

    def format_and_display_search_results(
        self, synthesis_result: SynthesisSearchResult, all_series: List[SeriesMetadata]
    ) -> FinalOutput:
        # Create _all_meta DataFrame in user namespace
        if len(synthesis_result.recommended_series) == 0:
            # self.show_no_results()
            return FinalOutput(display_markdown="No data series found matching your query.")

        _all_meta = pd.DataFrame([s.dict() for s in all_series])
        if self.ipython_shell is not None:
            self.ipython_shell.user_ns["_all_meta"] = _all_meta

        # Build a mapping from code to friendly_name for quick lookup
        code_to_friendly = {item.code: item.friendly_name for item in synthesis_result.recommended_series}
        code_to_cname = {item.code: item.column_name for item in synthesis_result.recommended_series}
        recommended_codes = set(code_to_friendly.keys())

        # Filter and update metadata
        recommended_metadata = [s for s in all_series if s.series_code in recommended_codes]
        for s in recommended_metadata:
            if s.series_code in code_to_friendly:
                # original metadata description is replaced with friendly name
                s.description = code_to_friendly[s.series_code]
                s.variable_name = code_to_cname[s.series_code]

        # Create DataFrame from recommended_metadata
        df_meta = pd.DataFrame([s.dict() for s in recommended_metadata])

        # Create DataFrame from recommended_series
        df_recommended = pd.DataFrame([item.model_dump() for item in synthesis_result.recommended_series])
        df_recommended = df_recommended.rename(columns={"code": "series_code"})

        # Merge to add columns from RecommendedSeriesItem
        if len(df_recommended) > 0:
            if len(df_meta) == 0:
                raise ValueError(
                    "No metadata available to merge with recommended series. synthesize_results might not be returning proper code."
                )
            df_meta = pd.merge(df_meta, df_recommended, on="series_code", how="left")
        df_meta = pd.merge(df_meta, df_recommended, on="series_code", how="left")

        cols = [
            "series_code",
            "variable_name",
            "description",
            "frequency",
            "source_file",
            "bilateral",
        ]
        df_meta = df_meta[[c for c in cols]]

        variable_name = synthesis_result.variable_name
        csv_str = df_meta.to_csv(index=False)
        next_cell = (
            "import pandas as pd\n"
            "import io\n"
            f"{variable_name}_csv = '''{csv_str}'''\n"
            f"{variable_name} = pd.read_csv(io.StringIO({variable_name}_csv))\n"
            f"{variable_name}"
        )
        markdown_text = (
            f"#### Analysis of Data Series for Query: \n{synthesis_result.detailed_analysis}\n\n"
            f"#### Reason for selection:\n {synthesis_result.reason_for_selection}\n\n"
            f"**Recommended Series:**\n"
        )

        return FinalOutput(display_markdown=markdown_text, display_dataframe=df_meta, next_cell_code=next_cell)

    def format_and_display_explain_results(
        self, synthesis_result_str: str, all_series: List[SeriesMetadata]
    ) -> FinalOutput:
        # Create _all_meta DataFrame in user namespace

        _all_meta = pd.DataFrame([s.dict() for s in all_series])
        if self.ipython_shell is not None:
            self.ipython_shell.user_ns["_all_meta"] = _all_meta

        markdown_text = synthesis_result_str
        return FinalOutput(display_markdown=markdown_text)

    def format_and_display_keyword_search_results(self, all_series: List[SeriesMetadata]) -> FinalOutput:
        # Create _all_meta DataFrame in user namespace

        _all_meta = pd.DataFrame([s.dict() for s in all_series])
        if self.ipython_shell is not None:
            self.ipython_shell.user_ns["_all_meta"] = _all_meta

        return FinalOutput(display_markdown="", display_dataframe=_all_meta)

    def display_final_output(self, final_output: FinalOutput):
        display(Markdown(final_output.display_markdown))
        if final_output.display_dataframe is not None:
            display(final_output.display_dataframe)
        display(Markdown("_all_meta dataframe has been created with all series metadata."))

        # Set next cell code if available
        if final_output.next_cell_code and self.ipython_shell is not None:
            self.ipython_shell.set_next_input(final_output.next_cell_code, replace=False)


HELP_TEXT = """
### `$data find` Magic Command
**Usage**

The `$data find` command is a flexible magic command for searching avaiable variables in the data catalog. For a natural language search, you can type your query on the first line or expand it across multiple lines within the cell. For specific, non-AI operations, you can use one of the built-in utility commands.



**Examples**

**Natural Language Query:**
```
$data find 
Bilateral service trade balance for G7 countries
```

**Checking what's in the dataset:**
```
$data find explain bis data related to extenral lending by currency
```

**Keyword Search:**
```
$data find keywords bis credit
# => searches for bis AND credit
```

```
$data find keywords 
bis credit
bop
# => searches for (bis AND credit) or (bop)
```


**Available SubCommands**

  * `$data find explain <natural language question>`
    Answers a natural language question about what kind of data is avaiable. It just checks metadata, and does not know country-specific avaiablility. 

  * `$data find keywords <file_name or category>`
    Lists all available data which matches categories or filename with keywords. Only searches the category data, not variable names. Use `$data find categories` to see full list of keywords.

  * `$data find categories`
    Returns a full list of all available filtering categories, useful for constructing precise queries.

  * `$data find database`
    Displays a description of the data sources (e.g., BIS, IMF, WB) available in the catalog.


**How It Works**

For natural language queries, the command uses an AI to derive structured search parameters, which are displayed for transparency. It queries a local data catalog, then uses another AI process to analyze and curate the most relevant series.


"""
