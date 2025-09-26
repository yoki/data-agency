# type: ignore
"""
Tests for the DataFindAgent class.
"""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import pandas as pd


from data_agency.commands.find.agent import DataFindAgent
from data_agency.commands.find.models import (
    FindDataSeriesParams,
    FilterSet,
    SynthesisSearchResult,
    FindMetadataResult,
    SeriesMetadata,
    RecommendedSeriesItem,
    FinalOutput,
)


@pytest.fixture
def agent():
    """Set up test fixtures."""
    agent = DataFindAgent()
    agent.search_service = MagicMock()
    agent.llm_service = MagicMock()
    agent.display_service = MagicMock()
    agent.ipython_shell = MagicMock()  # type: ignore
    return agent


# E       pydantic_core._pydantic_core.ValidationError: 1 validation error for FindMetadataResult
# E       search_params
# E         Field required [type=missing, input_value={'status': 'success', 'co...se}, friendly_name='')]}, input_type=dict]
# E           For further information visit https://errors.pydantic.dev/2.11/v/missing


@pytest.mark.asyncio
async def test_handle_utility_commands_variables(agent: DataFindAgent):
    """Test handling variables command."""
    # Mock search service response
    mock_result = FindMetadataResult(
        status="success",
        count=2,
        series=[
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
        ],
        search_params=FindDataSeriesParams(filters=[FilterSet(level1="test")]),  # pyright: ignore[reportCallIssue]
    )
    agent.search_service.get_variables_by_source.return_value = mock_result  # type: ignore

    result = await agent._handle_utility_commands("variables test.csv", "")
    assert result is True
    agent.search_service.get_variables_by_source.assert_called_once_with("test.csv")  # type: ignore
    agent.display_service.show_variables.assert_called_once()  # type: ignore


@pytest.mark.asyncio
async def test_process_natural_language_query_success(agent: DataFindAgent):
    """Test processing query with successful results."""
    # Mock services for successful query
    agent.search_service.get_available_categories.return_value = "test categories"  # type: ignore

    # Mock decompose query
    search_params = FindDataSeriesParams(filters=[FilterSet(level1="test")])  # type: ignore
    agent.llm_service.decompose_query = AsyncMock(return_value=search_params)

    # Mock search results
    mock_series = [
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
    ]
    mock_search_results = FindMetadataResult(
        status="success", count=2, series=mock_series, search_params=search_params
    )
    agent.search_service.find_series.return_value = mock_search_results  # type: ignore

    # Mock assessment results (new step in the workflow)
    from data_agency.commands.find.models import CategorySelectionAccessment

    mock_assessment = CategorySelectionAccessment(
        action="ACCEPT_VARIABLES", justification="Good selection", guidance_for_category_select_agent=""
    )
    agent.llm_service.assess_results = AsyncMock(return_value=mock_assessment)

    # Mock synthesis results
    mock_synthesis = SynthesisSearchResult(
        detailed_analysis="Test analysis",
        reason_for_selection="Test reason",
        recommended_series=[RecommendedSeriesItem(code="test1", column_name="col1", friendly_name="Test Series 1")],
        variable_name="test_meta",
    )
    agent.llm_service.synthesize_search_results = AsyncMock(return_value=mock_synthesis)

    # Mock final output
    mock_final_output = FinalOutput(
        display_markdown="Test markdown",
        display_dataframe=pd.DataFrame({"test": [1]}),
        next_cell_code="print('test')",
    )
    agent.display_service.format_and_display_search_results.return_value = mock_final_output

    await agent._process_natural_language_query("successful query")

    # Verify key steps were called
    agent.llm_service.decompose_query.assert_called_once()
    agent.display_service.show_search_filters.assert_called_once()
    agent.search_service.find_series.assert_called_once()
    agent.display_service.show_search_results_count.assert_called_once_with(2)
    agent.llm_service.assess_results.assert_called_once()
    agent.display_service.show_selection_assessment.assert_called_once()
    agent.llm_service.synthesize_search_results.assert_called_once()
    agent.display_service.format_and_display_search_results.assert_called_once()
    # agent.llm_service.synthesize_search_results.assert_called_once_with("successful query", mock_search_results)
    # agent.display_service.format_and_display_search_results.assert_called_once_with(mock_synthesis, mock_series)
    # agent.display_service.display_final_output.assert_called_once_with(mock_final_output)
