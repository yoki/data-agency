"""Pydantic models for find functionality."""

from pydantic import BaseModel, Field
from typing import List, Dict, Literal, Optional, Any
import pandas as pd
import io
import csv
from collections import defaultdict


class SeriesMetadata(BaseModel):
    """Metadata for a single data series."""

    series_code: str
    source_file: str
    variable_name: str
    description: str
    categories: Dict[str, str]
    source_metadata: Dict[str, Any]
    friendly_name: str = ""

    def dict(self):
        """Convert the SeriesMetadata instance to a dictionary."""
        return {
            "series_code": self.series_code,
            "source_file": self.source_file,
            "variable_name": self.variable_name,
            "description": self.description,
            "level1": self.categories.get("level1", ""),
            "level2": self.categories.get("level2", ""),
            "level3": self.categories.get("level3", ""),
            "frequency": self.source_metadata.get("frequency", ""),
            "bilateral": self.source_metadata.get("bilateral", False),
        }


class FilterSet(BaseModel):
    """A single filter set specifying criteria for selecting data series."""

    source_file: Optional[str] = Field(None, description="Name of the source file (e.g., 'bis_locational.csv').")
    level1: Optional[str] = Field(None, description="First-level category value (e.g., 'BIS', 'Federal Reserve').")
    level2: Optional[str] = Field(None, description="Second-level category value (e.g., 'liability').")
    level3: Optional[str] = Field(None, description="Third-level category value (optional).")


class FindDataSeriesParams(BaseModel):
    """Parameters for finding data series."""

    filters: List[FilterSet] = Field(..., description="List of filter sets for data series selection")

    def to_markdown(self) -> str:
        """Convert filters to markdown table format."""
        table = "| Source File | Level 1 | Level 2 | Level 3 |\n| --- | --- | --- | --- |\n"
        for fs in self.filters:
            table += f"| {fs.source_file or ''} | {fs.level1 or ''} | {fs.level2 or ''} | {fs.level3 or ''} |\n"
        return table


class FindMetadataResult(BaseModel):
    """Result of metadata search operation."""

    status: str
    count: int
    series: List[SeriesMetadata]
    error: Optional[str] = None
    available_values: Optional[Dict[str, Any]] = None
    search_params: FindDataSeriesParams

    def create_tsv(self) -> Dict[str, Dict[str, str]]:
        """Groups series by unified key and transforms each group into TSV format."""
        if not self.series:
            return {}

        grouped_series = defaultdict(list)
        key_contexts = {}

        for item in self.series:
            filename_base = item.source_file.split(".")[0]
            sorted_categories = sorted(item.categories.items())
            category_values = [v for k, v in sorted_categories]
            key_parts = [filename_base] + category_values
            unified_key = "_".join(key_parts)
            grouped_series[unified_key].append(item)

            # Build context string for header
            meta = item.source_metadata
            context_parts = []
            context_parts += [f"{k}={v}" for k, v in sorted_categories]
            if "frequency" in meta and meta["frequency"]:
                context_parts.append(f"frequency={meta['frequency']}")
            if meta.get("bilateral", False):
                context_parts.append("bilateral=true")
            context_str = f"{filename_base} [" + ", ".join(context_parts) + "]"
            key_contexts[unified_key] = context_str

        def to_tsv_string(headers: List[str], rows: List[List[str]]) -> str:
            output = io.StringIO()
            writer = csv.writer(output, delimiter="\t", lineterminator="\n")
            writer.writerow(headers)
            if rows:
                writer.writerows(rows)
            return output.getvalue()

        final_tables = {}
        for key, items_in_group in grouped_series.items():
            series_rows = []
            for item in items_in_group:
                series_rows.append([item.series_code, item.variable_name, item.description])
            final_tables[key] = {
                "header": key_contexts[key],
                "tsv": to_tsv_string(["series_code", "variable_name", "description"], series_rows),
            }

        return final_tables

    def get_tsv_printout_string(self) -> str:
        """Generates printable TSV tables with headers."""
        tables = self.create_tsv()
        if not tables:
            return ""
        tmp = []
        for i, (key, table) in enumerate(tables.items()):
            tmp.append(f"{table['header']}:")
            tmp.append(table["tsv"] + "\n")
        return "\n".join(tmp)

    def to_df(self) -> pd.DataFrame:
        """Converts the series data to a pandas DataFrame."""
        if not self.series:
            return pd.DataFrame()

        data = [s.dict() for s in self.series]
        df = pd.DataFrame(data)
        return df


class RecommendedSeriesItem(BaseModel):
    """A recommended data series item."""

    code: str = Field(..., description="Series code identifier.")
    column_name: str = Field(..., description="descriptive column name of final dataframe.")
    friendly_name: str = Field(..., description="Human-friendly name for the series.")


class RaiseAlertParam(BaseModel):
    """Parameters for raising alerts to users."""

    message: str = Field(..., description="The message to display to the user asking for clarification.")
    reason: str = Field(..., description="The reason why clarification is needed.")


class SynthesisSearchResult(BaseModel):
    """Result of LLM synthesis and recommendation."""

    detailed_analysis: str = Field(..., description="Detailed analysis of variable passed.")
    reason_for_selection: str = Field(..., description="Reasons for variables selected.")
    recommended_series: List[RecommendedSeriesItem] = Field(
        ..., description="List of recommended data series with code and friendly name."
    )
    variable_name: str = Field(..., description="Descriptive Python variable name for the dataset, ending in '_meta'.")


class CategorySelectionAccessment(BaseModel):
    """Decision on whether to proceed with current variables or switch to a new category."""

    action: Literal["ACCEPT_VARIABLES", "SWITCH_CATEGOREIS"] = Field(
        ..., description="The decision to either accept the provided variables or to switch to a new data categories."
    )
    justification: str = Field(..., description="Concise technical rationale for the decision.")
    guidance_for_category_select_agent: str = Field(
        ...,
        description="Guidance message for category select agent to redo search. Use empty string if not applicable.",
    )


class FinalOutput:
    """Final output for display."""

    def __init__(
        self,
        display_markdown: str,
        display_dataframe: Optional[pd.DataFrame] = None,
        next_cell_code: Optional[str] = None,
    ):
        self.display_markdown = display_markdown
        self.display_dataframe = display_dataframe
        self.next_cell_code = next_cell_code
