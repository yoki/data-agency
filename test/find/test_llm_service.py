"""Tests for the LLM service."""

import pytest


from data_agency.commands.find.llm_service import FindDataLLMService
from data_agency.commands.find.models import (
    FindDataSeriesParams,
    RaiseAlertParam,
)
from data_agency.common.llm_client import create_client, LLMModels


@pytest.fixture
def llm_service():
    """Create an LLM service instance."""
    client = create_client(model=LLMModels.GEMINI25_FLASH)
    return FindDataLLMService(client=client)


@pytest.mark.asyncio
async def test_decompose_query_find_series(llm_service):
    """Test decomposing query into FindDataSeriesParams using real LLM call."""
    # Use a clear query that should return search parameters
    result = await llm_service.decompose_query("Find BIS banking statistics", "")

    assert isinstance(result, FindDataSeriesParams)
    assert len(result.filters) > 0
    # At least one filter should mention BIS
    bis_found = False
    for filter_set in result.filters:
        if filter_set.source_file and "bis" in filter_set.source_file.lower():
            bis_found = True
            break
        if filter_set.level1 and "bis" in filter_set.level1.lower():
            bis_found = True
            break
    assert bis_found, "Expected to find BIS in at least one filter"


@pytest.mark.asyncio
async def test_decompose_query_raise_alert(llm_service):
    """Test decomposing query into RaiseAlertParam using real LLM call."""
    # Use a query that's clearly not in the financial dataset
    result = await llm_service.decompose_query("Find data about Mars colonization projects", "")

    assert isinstance(result, RaiseAlertParam)
    assert result.message, "Alert message should not be empty"
    assert result.reason, "Alert reason should not be empty"


if __name__ == "__main__":
    # Run tests using pytest
    pytest.main(["-xvs", __file__])
