"""
Tests for the display service.
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from data_agency.commands.find.display_service import DisplayService
from data_agency.commands.find.models import (
    SynthesisSearchResult,
    SeriesMetadata,
    RecommendedSeriesItem,
    FinalOutput,
)


class TestDisplayService(unittest.TestCase):
    """Test cases for the DisplayService class."""

    def setUp(self):
        """Set up test fixtures."""
        self.display_service = DisplayService()
        self.display_service.ipython_shell = MagicMock()

    def test_format_and_display_results(self):
        """Test format_and_display_results method."""
        # Create test data
        synthesis_result = SynthesisSearchResult(
            detailed_analysis="Test analysis",
            reason_for_selection="Test reason",
            recommended_series=[
                RecommendedSeriesItem(code="test1", friendly_name="Test Series 1", column_name="test_col1"),
                RecommendedSeriesItem(code="test2", friendly_name="Test Series 2", column_name="test_col2"),
            ],
            variable_name="test_meta",
        )

        series_metadata = [
            SeriesMetadata(
                series_code="test1",
                source_file="test.csv",
                variable_name="Test Var 1",
                description="Test Description 1",
                categories={"level1": "Test", "level2": "Test2"},
                source_metadata={"frequency": "monthly", "bilateral": False},
            ),
            SeriesMetadata(
                series_code="test2",
                source_file="test.csv",
                variable_name="Test Var 2",
                description="Test Description 2",
                categories={"level1": "Test", "level2": "Test2"},
                source_metadata={"frequency": "monthly", "bilateral": False},
            ),
            SeriesMetadata(
                series_code="test3",
                source_file="test.csv",
                variable_name="Test Var 3",
                description="Test Description 3",
                categories={"level1": "Test", "level2": "Test2"},
                source_metadata={"frequency": "monthly", "bilateral": False},
            ),
        ]

        # Call the method
        result = self.display_service.format_and_display_search_results(synthesis_result, series_metadata)

        # Check the result
        self.assertIsInstance(result, FinalOutput)
        self.assertIn("Test analysis", result.display_markdown)
        self.assertIn("Test reason", result.display_markdown)
        self.assertEqual(len(result.display_dataframe), 2)  # type: ignore
        self.assertIn("test_meta", result.next_cell_code)  # type: ignore

        # Check that _all_meta was set in user namespace
        self.display_service.ipython_shell.user_ns.__setitem__.assert_called_once()  # type: ignore
        self.assertEqual(self.display_service.ipython_shell.user_ns.__setitem__.call_args[0][0], "_all_meta")  # type: ignore

    @patch("data_agency.commands.find.display_service.display")
    def test_display_final_output(self, mock_display):
        """Test display_final_output method."""
        final_output = FinalOutput(
            display_markdown="Test markdown",
            display_dataframe=pd.DataFrame({"test": [1, 2, 3]}),
            next_cell_code='print("test")',
        )

        self.display_service.display_final_output(final_output)

        # Check display was called 3 times (markdown, dataframe, all_meta message)
        self.assertEqual(mock_display.call_count, 3)

        # Check set_next_input was called
        self.display_service.ipython_shell.set_next_input.assert_called_once_with('print("test")', replace=False)  # type: ignore


if __name__ == "__main__":
    unittest.main()
