import pandas as pd
import pytest
import io
import sys
from pathlib import Path
from data_agency.commands.load.load_service import load
from lettuce_logger import pp

# Metadata provided by the user, representing the structure of the actual data.
METADATA_CSV = """series_code,variable_name,description,frequency,source_file,bilateral
imf_bop-l_h00y,portfolio_investment_liabilities_all,Portfolio Investment: All Sec & Mat Liabilities,Q,imf_bop.csv,False
imf_bop-a_h00y,portfolio_investment_assets_all,Portfolio Investment: All Sec & Mat Assets,Q,imf_bop.csv,False
imf_dot-t_b,trade_balance,Trade Balance,A,imf_dot.csv,True
imf_dot-m_f,imports_cif,Imports c.i.f. from,A,imf_dot.csv,True
imf_dot-x_t,exports_to,Exports to,A,imf_dot.csv,True
imf_ifs-s_gdp,gdp_sa_bil_lcu,"GDP (SA, Bil.LCU)",Q,imf_ifs.csv,False
imf_ifs-c_gdp,gdp_nsa_bil_lcu,"Gross Domestic Product (NSA, Bil.LCU)",Q,imf_ifs.csv,False
imf_ifs-c_gdpr,real_gdp_nsa_bil_chn_lcu,"Real GDP (NSA, Bil Chn.2015.LCU)",Q,imf_ifs.csv,False
imf_ifs-s_gdpr,real_gdp_sa_bil_chn_lcu,"Real GDP (SA, Bil.Chn.2015.LCU)",Q,imf_ifs.csv,False
imf_ifs_monthly-c_pc,consumer_prices,Consumer Prices (2010=100),M,imf_ifs_monthly.csv,False
"""


@pytest.fixture(scope="module")
def full_metadata():
    """
    Parses the global METADATA_CSV string into a pandas DataFrame for use in tests.
    """
    return pd.read_csv(io.StringIO(METADATA_CSV))


def test_load_single_source_quarterly(full_metadata):
    """
    Tests loading data from a single quarterly, non-bilateral source file.
    """
    meta_df = full_metadata[full_metadata.source_file == "imf_bop.csv"]
    result = load(meta_df)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    expected_cols = ["time", "ccode", "portfolio_investment_liabilities_all", "portfolio_investment_assets_all"]
    assert all(col in result.columns for col in expected_cols)
    assert result.attrs["frequency"] == "Q"
    assert not result.attrs["bilateral"]
    assert result.attrs["source_files"] == ["imf_bop.csv"]
    assert "portfolio_investment_liabilities_all" in result.attrs["column_description"]


def test_load_single_source_annual_bilateral(full_metadata):
    """
    Tests loading data from a single annual, bilateral source file.
    """
    meta_df = full_metadata[full_metadata.source_file == "imf_dot.csv"]
    result = load(meta_df)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    expected_cols = ["time", "reporter", "counterpart", "trade_balance", "imports_cif", "exports_to"]
    assert all(col in result.columns for col in expected_cols)
    assert result.attrs["frequency"] == "A"
    assert result.attrs["bilateral"]
    assert result.attrs["source_files"] == ["imf_dot.csv"]
    assert "trade_balance" in result.attrs["column_description"]


def test_load_merged_quarterly_non_bilateral(full_metadata):
    """
    Tests merging data from two quarterly, non-bilateral source files (imf_bop and imf_ifs).
    """
    meta_df = full_metadata[full_metadata.frequency == "Q"]
    result = load(meta_df)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert result.attrs["frequency"] == "Q"
    assert not result.attrs["bilateral"]
    assert sorted(result.attrs["source_files"]) == ["imf_bop.csv", "imf_ifs.csv"]

    # Check for columns from both source files
    bop_cols = ["portfolio_investment_liabilities_all", "portfolio_investment_assets_all"]
    ifs_cols = ["gdp_sa_bil_lcu", "gdp_nsa_bil_lcu", "real_gdp_nsa_bil_chn_lcu", "real_gdp_sa_bil_chn_lcu"]
    structural_cols = ["time", "ccode"]
    expected_cols = structural_cols + bop_cols + ifs_cols
    assert all(col in result.columns for col in expected_cols)
    assert "gdp_sa_bil_lcu" in result.attrs["column_description"]


def test_load_multiple_frequencies_and_bilateral(full_metadata):
    """
    Tests loading data with mixed frequencies and bilateral flags.
    This should return a dictionary of DataFrames.
    """
    result = load(full_metadata)

    assert isinstance(result, dict)
    expected_keys = ["Q", "A_bilateral", "M"]
    assert all(key in result for key in expected_keys)

    # Check the quarterly non-bilateral data
    df_q = result["Q"]
    assert isinstance(df_q, pd.DataFrame)
    assert df_q.attrs["frequency"] == "Q"
    assert not df_q.attrs["bilateral"]
    assert len(df_q.columns) == 10
    assert len(df_q) > 1000

    # Check the annual bilateral data
    df_a = result["A_bilateral"]
    assert isinstance(df_a, pd.DataFrame)
    assert df_a.attrs["frequency"] == "A"
    assert df_a.attrs["bilateral"]
    assert len(df_a.columns) == 12
    assert len(df_a) > 1000

    # Check the monthly non-bilateral data
    df_m = result["M"]
    assert isinstance(df_m, pd.DataFrame)
    assert df_m.attrs["frequency"] == "M"
    assert not df_m.attrs["bilateral"]
    assert "consumer_prices" in df_m.columns
    assert len(df_m.columns) == 5
    assert len(df_m) > 1000


def test_file_not_found_error():
    """
    Tests that a FileNotFoundError is raised when a source file does not exist.
    """
    bad_metadata = pd.DataFrame(
        [
            {
                "series_code": "test-c_test",
                "variable_name": "test_var",
                "description": "A test variable",
                "frequency": "Q",
                "source_file": "non_existent_file.csv",
                "bilateral": False,
            }
        ]
    )

    with pytest.raises(FileNotFoundError, match="Data file '.*non_existent_file.csv' not found"):
        load(bad_metadata)
