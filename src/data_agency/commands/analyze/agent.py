from __future__ import annotations
import asyncio
import nest_asyncio
from IPython.display import Markdown, display
from IPython.core.getipython import get_ipython

from .models import CodeGenerationRequest
from .workflow import AgentWorkflow
from .notebopok_ui import DisplayService
from data_agency.common.llm_client import create_client, LLMModels, FullLogChatClientCache


class DataAnalysisAgent:
    def __init__(self, client: FullLogChatClientCache = None):  # type: ignore
        if client is None:
            client = create_client(model=LLMModels.GEMINI25_FLASH)

        self.client = client
        self.MAX_CODE_GENERATION = 3

    async def run(self, line: str, cell: str = ""):
        if not cell:
            display(Markdown("Please provide a request for analysis in the second line."))
            return
        user_vars = self._collect_user_vars(line)
        display(Markdown("Starting to generate code for you..."))
        req = CodeGenerationRequest(request_text=cell, user_variables=user_vars)

        ui = DisplayService()
        wf = AgentWorkflow(
            request=req,
            client=self.client,
            ui=ui,
            max_code_generation=self.MAX_CODE_GENERATION,
        )
        await wf.run()

    def _collect_user_vars(self, line: str):
        ip = get_ipython()
        ns = getattr(ip, "user_ns", {}) if ip else {}
        names = line.strip().split()
        out = {}
        for n in names:
            if n in ns:
                v = ns[n]
                if isinstance(v, dict):
                    for k, vv in v.items():
                        out[f"{n}_{k}"] = vv
                else:
                    out[n] = v
        return out
