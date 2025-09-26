# codegen_agent/core/llm_service.py
from __future__ import annotations

"""
LLM services for analysis:
- CodeGenerationService: generate Python code from a natural-language request
- AssessmentService: assess execution results and optionally produce a retry
This module is framework-agnostic (no IPython imports).

Notes on prompt strings:
- Prompt templates use raw triple-quoted strings so Markdown and backslashes
  are preserved exactly as written.
"""

import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Protocol, Type
from autogen_core.models import UserMessage, SystemMessage, AssistantMessage

import pandas as pd

from .models import (
    CodeGenerationRequest,
    CodeGenerationResult,
    ExecutionResult,
    CodeAssessmentResult,
    ExecutionAssessmentHistoryItem,
)
from ...common.llm_client import FullLogChatClientCache
# -----------------------------
# Prompt templates (raw strings)
# -----------------------------

CODE_GENERATOR_SYSTEM_PROMPT = """
You are an expert-level Python data analysis agent. Your purpose is to write, debug, and refine Python code to answer user requests using a provided dataset.

## **Core Directives**
1.  **Accuracy & Correctness:** This is your primary goal. Your final output (stdout) must directly and correctly satisfy the user's request.
2.  **Robustness:** Write defensive code that anticipates potential issues like empty dataframes, missing values, or incorrect data types.
3.  **Safety:** You **MUST NOT** attempt to access any files, libraries, or network resources. Use only `pandas`, `numpy`, `matplotlib` and `seaborn`.

## **Protocols for Handling Failures**
If you are provided with an `error traceback` (`stderr`), you **MUST** follow this three-step protocol:

1.  **Analyze:** State the specific technical error in one sentence.
2.  **Rationale:** Briefly explain the flaw and your plan to fix it.
3.  **Corrected Code:** Provide the complete, new block of code.
"""

OUTPUT_ASSESMENT_SYSTEM_PROMPT = """
You are an expert-level code assessment agent. Your purpose is to meticulously analyze the results of executed Python code to determine if it successfully fulfilled the original user's request.

## **Core Directives**
* **Be a Critical Analyst:** Your primary role is to be a critic, not a simple code generator. Scrutinize the execution output for subtle logical errors or failures to meet the user's full intent.
* **History Matters:** Refer to the provided conversation history to ensure your suggestions are novel and not repeating previous failed attempts.
* **Follow a Strict Schema:** Your entire output must be a single JSON object that strictly conforms to the provided `CodeAssessmentResult` schema. Do not output any other text or explanation outside of this JSON structure.
"""

OUTPUT_ASSESSMENT_PROMPT_TEMPLATE = """
Your task is to assess the result of a code execution against the original user request and conversation history. Based on your assessment, you will determine if the request was fulfilled and, if not, generate a plan and new code for the next attempt.

## **Context**
* **User Request:** "{request_text}"

## **Execution Artifacts**
```python
{code}
```

  * **Execution Result (stdout):**
```
{stdout}
```

  * **Execution Result (stderr):**
```
{stderr}
```

**Available Data:**
{data_description}

  * **Current date** :{today}.


## **Your Task**

Analyze the execution artifacts in the context of the user request and history. If the user request was not fulfilled, generate a plan and new code for the next attempt.
"""

CODE_GENERATION_PROMPT_TEMPLATE = """
**User Request:** "Today is {today}. {request_text}"

**Available Data:**
{data_description}

Generate the Python code to fulfill the request.
"""

CODE_REGENERATION_PROMPT_TEMPLATE = """
Your previous code attempt failed. Analyze the error and generate a new version of the code.

# **User Request:** "Today is {today}. {request_text}"

# **Your Previous Code:**
```python
{code}
```

  * **Execution Result (stdout):**
```
{stdout}
```

  * **Execution Result (stderr):**
```
{stderr}
```

**Available Data:**
{data_description}

**Your Task:**
Follow your debugging protocol precisely to generate the corrected, complete Python code.
"""

# -----------------------------
# LLM client protocol
# -----------------------------


class LLMServiceBase:
    """Base class for LLM services, providing a client and data description utility."""

    def __init__(self, client: FullLogChatClientCache):
        self.client = client

    def prepare_data_description(self, user_variables: Dict[str, Any]) -> str:
        """Create descriptions of available dataframes/series for the prompt."""
        descriptions: List[str] = []
        for var_name, var_value in user_variables.items():
            try:
                if isinstance(var_value, pd.DataFrame):
                    description = (
                        f"Variable: {var_name}\n"
                        f"Type: DataFrame\n"
                        f"Shape: {var_value.shape}\n"
                        f"Columns: {list(var_value.columns)}\n"
                        f"Dataframe Description: {var_value.attrs}\n"
                        f"Sample data (first 3 rows):\n{var_value.head(3)}\n"
                    )
                    descriptions.append(description)
                elif isinstance(var_value, pd.Series):
                    description = (
                        f"Variable: {var_name}\n"
                        f"Type: Series\n"
                        f"Length: {len(var_value)}\n"
                        f"Name: {var_value.name}\n"
                        f"Sample data (first 3 values):\n{var_value.head(3)}\n"
                    )
                    descriptions.append(description)
            except Exception as e:
                raise RuntimeError(f"Variable '{var_name}' is not a DataFrame or Series: {str(e)}") from None
        if not descriptions:
            return "No data variables available."
        return "\n\n".join(descriptions)


class CodeGenerationService(LLMServiceBase):
    """Service for initial code generation."""

    async def generate_code(self, request: CodeGenerationRequest) -> CodeGenerationResult:
        """Generates Python code based on a user query."""
        data_description = self.prepare_data_description(request.user_variables)
        prompt = CODE_GENERATION_PROMPT_TEMPLATE.format(
            today=datetime.today().date(),
            request_text=request.request_text,
            data_description=data_description,
        )
        response = await self.client.create(
            messages=[
                SystemMessage(content=CODE_GENERATOR_SYSTEM_PROMPT),
                UserMessage(content=prompt, source="user"),
            ],
            json_output=CodeGenerationResult,
        )
        # Expecting a JSON-stringifiable payload in response.content
        args = json.loads(response.content)  # type: ignore[attr-defined]
        return CodeGenerationResult(**args)


class AssessmentService(LLMServiceBase):
    """Service for assessing code execution results."""

    async def assess_code_output(
        self,
        request: CodeGenerationRequest,
        execution_result: ExecutionResult,
        code: str,
        previous_actions: List[ExecutionAssessmentHistoryItem],
    ) -> CodeAssessmentResult:
        """Analyzes execution results to determine success and next steps."""
        data_description = self.prepare_data_description(request.user_variables)

        if execution_result.success:
            template = OUTPUT_ASSESSMENT_PROMPT_TEMPLATE
            system_prompt = OUTPUT_ASSESMENT_SYSTEM_PROMPT
        else:
            template = CODE_REGENERATION_PROMPT_TEMPLATE
            system_prompt = CODE_GENERATOR_SYSTEM_PROMPT

        prompt = template.format(
            today=datetime.today().date(),
            request_text=request.request_text,
            code=code,
            stdout=execution_result.stdout,
            stderr=execution_result.stderr,
            data_description=data_description,
        )

        messages = [
            SystemMessage(content=system_prompt),
        ]
        messages += [item.generate_agent_message() for item in previous_actions]

        messages += [UserMessage(content=prompt, source="user")]

        # messages = [
        #     SystemMessage(content=system_prompt),
        # ]
        # # Thread prior attempts into context
        # for item in previous_actions:
        #     # Use only the textual content of the generated assistant message
        #     try:
        #         content = item.generate_agent_message().content  # type: ignore[attr-defined]
        #     except Exception:
        #         content = f"Plan:\n{item.plan}\n\nExecution Result:\n{item.execution_result.stdout}\n{item.execution_result.stderr}\n\nAnalysis:\n{item.assessment.analysis}"
        #     messages.append({"role": "assistant", "content": content})

        # messages.append({"role": "user", "content": prompt})

        response = await self.client.create(
            messages=messages,
            json_output=CodeAssessmentResult,
        )
        args = json.loads(response.content)  # type: ignore[attr-defined]
        return CodeAssessmentResult(**args)
