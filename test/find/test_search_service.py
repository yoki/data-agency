"""Tests for the search service."""

import pandas as pd
from data_agency.commands.find.search_service import SearchService
from data_agency.commands.find.models import FilterSet, FindDataSeriesParams


def test_normalize_text():
    """Test text normalization function."""
    service = SearchService()

    # Test basic normalization
    assert service.normalize_text("Test String") == "test string"

    # Test with non-ASCII characters
    assert service.normalize_text("Tést Strîng") == "tst strng"

    # Test with extra whitespace
    assert service.normalize_text("  Test   String  ") == "test string"

    # Test with empty string
    assert service.normalize_text("") == ""

    # Test with None
    assert service.normalize_text(None) == ""


def test_load_metadata_files():
    """Test loading metadata files."""
    service = SearchService()

    # Test loading manifest
    manifest = service.load_manifest()
    assert isinstance(manifest, list)
    assert len(manifest) > 0

    # Test loading sources
    sources = service.load_sources()
    assert isinstance(sources, dict)
    assert len(sources) > 0

    # Test loading categories
    categories = service.get_available_categories()
    assert isinstance(categories, pd.DataFrame)
    assert len(categories) > 0


def test_get_sources_dataframe():
    """Test getting sources as DataFrame."""
    service = SearchService()
    df = service.get_sources_dataframe()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_get_available_categories():
    """Test getting available categories."""
    service = SearchService()
    df = service.get_available_categories()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_find_series():
    """Test finding data series with different filter combinations."""
    service = SearchService()

    # Test 1: Filter by source file only
    params = FindDataSeriesParams(filters=[FilterSet(source_file="imf_fsi_q.csv")])
    result = service.find_series(params)

    assert result.status == "success"
    assert result.count > 40
    assert len(result.series) > 40

    # Test 2: Filter by level1 category only
    params = FindDataSeriesParams(filters=[FilterSet(level1="FinAcct")])
    result = service.find_series(params)

    assert result.status == "success"
    assert result.count > 40

    # Test 3: Filter by source file and level2 category
    params = FindDataSeriesParams(filters=[FilterSet(source_file="imf_cpis.csv", level2="Liab")])
    result = service.find_series(params)

    assert result.status == "success"

    # Test 4: Filter by multiple filter sets (OR logic)
    params = FindDataSeriesParams(
        filters=[
            FilterSet(source_file="imf_ifs_monthly.csv", level2="Inflation"),
            FilterSet(source_file="bis_locational_a5.csv", level3="A5.ByBankLocationExternal", level2="General"),
            FilterSet(source_file="fed_z1.csv", level1="Federal Reserve"),
        ]
    )
    result = service.find_series(params)

    assert result.status == "success"
    print(result.series[0])
    assert result.count == 3


def test_get_variables_by_source():
    """Test getting variables by source file."""
    service = SearchService()

    result = service.get_variables_by_source("imf_fsi_q.csv")

    assert result.status == "success"
    assert result.count > 0
    assert len(result.series) > 0

    # All series should be from the specified source file
    for series in result.series:
        assert series.source_file == "imf_fsi_q.csv"


if __name__ == "__main__":
    # Run tests
    test_normalize_text()
    test_load_metadata_files()
    test_get_sources_dataframe()
    test_get_available_categories()
    test_find_series()
    test_get_variables_by_source()
    print("All search service tests passed!")
