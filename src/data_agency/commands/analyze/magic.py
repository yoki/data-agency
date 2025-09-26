"""
Magic command registration for the analyze command.
"""

import asyncio
import nest_asyncio
from IPython.core.magic import Magics, magics_class, line_magic, cell_magic

from .agent import DataAnalysisAgent


def analyze(line="", cell=""):
    nest_asyncio.apply()
    out = asyncio.run(analyze_async(line, cell))
    return out


async def analyze_async(line, cell):
    """
    Asynchronous handler for the analyze magic command.

    Args:
        line: The command line arguments
        cell: The cell content for cell magic

    Returns:
        The result of the command execution
    """
    agent = DataAnalysisAgent()
    await agent.run(line, cell)
