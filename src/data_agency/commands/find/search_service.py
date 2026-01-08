"""Search service for find functionality."""

import json
import re
import pandas as pd
from typing import Dict, List, Any
import csv
import io
from ...common.load_env import METADATA_PATH, raise_if_no_data_root
from .models import SeriesMetadata, FindMetadataResult, FindDataSeriesParams, FilterSet

# Constants for file paths
if METADATA_PATH is not None:
    SOURCES_PATH = METADATA_PATH / "sources.json"
    CATEGORIES_PATH = METADATA_PATH / "unique_categories.csv"
    MANIFEST_PATH = METADATA_PATH / "manifest.csv"


class SearchService:
    """Service for searching and retrieving data series metadata."""

    def __init__(self, max_results: int = 1000):
        self.max_results = max_results

    def normalize_text(self, text: str) -> str:
        if not text:
            return ""
        # Remove non-ASCII characters and convert to lowercase
        normalized = re.sub(r"[^\x00-\x7F]+", "", text).lower()
        # Remove extra whitespace
        return re.sub(r"\s+", " ", normalized).strip()

    def normalize_keywords_for_category(self, keywords: List[List[str]]) -> List[str]:
        keyword_dict = {
            "assets": "asset",
            "liabilities": "liab",
            "liability": "liab",
            "account": "acct",
            "accounts": "acct",
            "investment": "inv",
            "investments": "inv",
            "foreignexchange": "fx",
            "forex": "fx",
            "interestrate": "interest",
            "interestrates": "interest",
            "indicators": "ind",
            "indicator": "ind",
            "financialstability": "finstab",
            "internationalinvestmentposition": "iip",
        }

        short_keys = {
            "di": "directinv",
            "pi": "portfolioinv",
            "oi": "otherinv",
            "ca": "curacct",
            "pa": "primaryincome",
            "sa": "secondaryincome",
        }

        out = []

        for keyword_list in keywords:
            normalized_keywords = []
            for keyword in keyword_list:
                normalized_keyword = self.normalize_text(keyword)

                # allow partial matches for keyword_dict
                for key, value in keyword_dict.items():
                    normalized_keyword = normalized_keyword.replace(key, value)
                if normalized_keyword in short_keys:
                    normalized_keyword = short_keys[normalized_keyword]

                normalized_keywords.append(normalized_keyword)
            out.append(normalized_keywords)

        return out

    def find_series(self, params: FindDataSeriesParams) -> FindMetadataResult:
        """Find data series based on filter parameters.

        Args:
            params: Search parameters with filter sets

        Returns:
            Search results with matching series
        """
        filter_sets = params.filters

        try:
            manifest = self.load_manifest()
            sources = self.load_sources()

            matching_series = []

            for series_data in manifest:
                categories = series_data.get("categories", {})
                searchable_fields = [
                    series_data.get("source_file", ""),
                    categories.get("level1", ""),
                    categories.get("level2", ""),
                    categories.get("level3", ""),
                ]
                combined_text = self.normalize_text(" ".join(filter(None, searchable_fields)))

                match = False
                for filter_set in filter_sets:
                    terms = [val for val in filter_set.__dict__.values() if val is not None]
                    if not terms:
                        continue
                    normalized_terms = [self.normalize_text(term) for term in terms]

                    set_match = all(term in combined_text for term in normalized_terms)

                    if set_match:
                        match = True
                        break  # A single matching filter set is sufficient
                if match:
                    source_file = series_data.get("source_file", "")
                    source_metadata = sources.get(source_file, {})

                    series = SeriesMetadata(
                        series_code=series_data["series_id"],
                        source_file=source_file,
                        variable_name=series_data["variable_name"],
                        description=series_data["description"],
                        categories=categories,
                        source_metadata={
                            "frequency": source_metadata.get("frequency"),
                            "bilateral": source_metadata.get("bilateral"),
                            "description": source_metadata.get("description"),
                        },
                    )
                    matching_series.append(series)

                    if len(matching_series) >= self.max_results:
                        break

            result = FindMetadataResult(
                status="success", count=len(matching_series), series=matching_series, search_params=params
            )
            return result

        except FileNotFoundError as e:
            raise e

    def load_manifest(self) -> List[Dict[str, Any]]:
        raise_if_no_data_root()
        with open(MANIFEST_PATH, "r") as f:
            csv_data = f.read()
            data_list = []
            f = io.StringIO(csv_data)
            reader = csv.DictReader(f)

            for row in reader:
                new_row = {
                    "series_id": row["series_id"],
                    "source_file": row["source_file"],
                    "variable_name": row["variable_name"],
                    "description": row["description"],
                    "categories": {"level1": row["level1"], "level2": row["level2"], "level3": row["level3"]},
                }
                data_list.append(new_row)
            return data_list

    def load_sources(self) -> Dict[str, Any]:
        raise_if_no_data_root()
        with open(SOURCES_PATH, "r") as f:
            return json.load(f)

    def get_sources_dataframe(self) -> pd.DataFrame:
        raise_if_no_data_root()
        return pd.read_json(SOURCES_PATH).T

    def get_available_categories(self) -> pd.DataFrame:
        raise_if_no_data_root()
        df = pd.read_csv(MANIFEST_PATH)
        df = df[["source_file", "level1", "level2", "level3"]].drop_duplicates()
        df.rename(
            columns={
                "source_file": "csv",
                "level1": "category 1",
                "level2": "category 2",
                "level3": "category 3",
            },
            inplace=True,
        )

        return df

    def get_variables_by_source(self, source_file: str) -> FindMetadataResult:
        params = FindDataSeriesParams(filters=[FilterSet(source_file=source_file)])  # type: ignore
        return self.find_series(params)
