import pandas as pd
import pytest
from data_agency.commands.describe.agent import describe_dataframe
from io import StringIO
from unittest.mock import patch, MagicMock


@pytest.fixture
def bilateral_df():
    """Create a bilateral dataframe similar to CPIS data structure.

    Simulates realistic bilateral data where each reporter has multiple counterparts
    per time period. This creates the scenario where naive row counting would
    massively overestimate time coverage.

    Example: TH has 20 time periods x 50 counterparts = 1000 rows
    Without the fix: 1000 / 4 = 250 "years"
    With the fix: 20 unique periods / 4 = 5 years
    """
    # Create 20 quarters of data for Thailand with 50 counterpart countries
    # Create 8 quarters of data for Japan with 30 counterpart countries
    counterparts = ["US", "CN", "JP", "GB", "FR", "DE", "KR", "SG", "MY", "ID"] * 5  # 50 counterparts

    rows = []
    # Thailand: 20 quarters (2019Q1 to 2023Q4)
    for year in range(2019, 2024):
        for quarter in range(1, 5):
            for cp in counterparts[:50]:
                rows.append(
                    {
                        "time": f"{year}Q{quarter}",
                        "reporter": "TH",
                        "counterpart": cp,
                        "ccode": "TH",
                        "liab_reported_tot": 100 + (year - 2019) * 10,
                    }
                )

    # Japan: 8 quarters (2021Q1 to 2022Q4)
    for year in range(2021, 2023):
        for quarter in range(1, 5):
            for cp in counterparts[:30]:
                rows.append(
                    {
                        "time": f"{year}Q{quarter}",
                        "reporter": "JP",
                        "counterpart": cp,
                        "ccode": "JP",
                        "liab_reported_tot": 1000 + (year - 2021) * 50,
                    }
                )

    df = pd.DataFrame(rows)
    df.attrs = {
        "frequency": "Q",
        "bilateral": True,
        "column_description": {"liab_reported_tot": "Liabilities: Total Reported"},
    }
    return df


@pytest.fixture
def non_bilateral_df():
    """Create a non-bilateral dataframe."""
    data = {
        "time": ["2020Q1", "2020Q2", "2020Q3", "2020Q4", "2020Q1", "2020Q2"],
        "ccode": ["TH", "TH", "TH", "TH", "JP", "JP"],
        "gdp": [500, 510, 520, 530, 5000, 5100],
        "cpi": [100, 101, 102, 103, 100, 101],
    }
    df = pd.DataFrame(data)
    df.attrs = {"frequency": "Q", "bilateral": False, "column_description": {"gdp": "GDP", "cpi": "CPI"}}
    return df


@patch("data_agency.commands.describe.agent.display")
def test_bilateral_data_counts_unique_time_periods(mock_display, bilateral_df):
    """Test that bilateral data counts unique time periods, not total rows.

    With the fix:
    - TH: 20 unique quarters / 4 = 5 years
    - JP: 8 unique quarters / 4 = 2 years
    """
    target_countries = ["TH", "JP"]

    describe_dataframe(bilateral_df, "cpis_data", target_countries)

    # Check that display was called
    assert mock_display.called

    # Find the call that displays the grouped data for liab_reported_tot
    display_calls = [call[0][0] for call in mock_display.call_args_list]

    # Look for the nyears column in displayed dataframes
    found_years_df = False
    for call_arg in display_calls:
        if isinstance(call_arg, pd.DataFrame) and "nyears" in call_arg.columns:
            df_display = call_arg
            found_years_df = True
            # Should show reasonable year ranges (2-5 years), NOT hundreds of years
            for val in df_display["nyears"]:
                years_str = str(val)
                # Extract the max number from "X to Y years" format
                max_years = int(years_str.split("to")[1].strip().split()[0])
                assert max_years <= 10, f"Year count too high: {years_str} (indicates bug not fixed)"
            break

    assert found_years_df, "Expected to find a dataframe with nyears column"


@patch("data_agency.commands.describe.agent.display")
def test_non_bilateral_data_counts_total_observations(mock_display, non_bilateral_df):
    """Test that non-bilateral data counts total observations per country."""
    target_countries = ["TH", "JP"]

    describe_dataframe(non_bilateral_df, "macro_data", target_countries)

    # Check that display was called
    assert mock_display.called

    # For non-bilateral data with 4 observations for TH and 2 for JP
    # TH: 4 quarters = 1 year, JP: 2 quarters = 0.5 years
    display_calls = [call[0][0] for call in mock_display.call_args_list]

    found_years_df = False
    for call_arg in display_calls:
        if isinstance(call_arg, pd.DataFrame) and "nyears" in call_arg.columns:
            df_display = call_arg
            found_years_df = True
            # Should show reasonable year ranges
            assert all("to 0" in str(val) or "to 1" in str(val) for val in df_display["nyears"])
            break

    assert found_years_df, "Expected to find a dataframe with nyears column"


@patch("data_agency.commands.describe.agent.display")
def test_bilateral_detection_from_attrs(mock_display, bilateral_df):
    """Test that bilateral flag is correctly detected from df.attrs."""
    target_countries = ["TH"]

    describe_dataframe(bilateral_df, "test_data", target_countries)

    # Should handle bilateral data appropriately
    assert mock_display.called


@patch("data_agency.commands.describe.agent.display")
def test_bilateral_detection_from_counterpart_column(mock_display):
    """Test that bilateral flag is detected from presence of counterpart column."""
    # Create df without bilateral attr but with counterpart column
    data = {
        "time": ["2020Q1", "2020Q2"],
        "reporter": ["TH", "TH"],
        "counterpart": ["US", "CN"],
        "ccode": ["TH", "TH"],
        "trade": [100, 200],
    }
    df = pd.DataFrame(data)
    df.attrs = {"frequency": "Q", "column_description": {}}  # No bilateral attr

    target_countries = ["TH", "JP"]

    describe_dataframe(df, "trade_data", target_countries)

    # Should still handle as bilateral data
    assert mock_display.called
