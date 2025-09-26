# my_complex_extension/my_complex_extension/magics.py

from IPython.core.magic import Magics, magics_class, line_magic, cell_magic
import asyncio
import nest_asyncio
from .agent import DataFindAgent


def find(line="", cell=""):
    nest_asyncio.apply()
    out = asyncio.run(find_async(line, cell))
    return out


async def find_async(line, cell):
    agent = DataFindAgent()
    await agent.run(line, cell)
