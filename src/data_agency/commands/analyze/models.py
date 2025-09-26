from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

from autogen_core.models import AssistantMessage


class CodeGenerationRequest(BaseModel):
    request_text: str
    user_variables: Dict[str, Any] = {}

    @classmethod
    def empty_request(cls):
        return cls(request_text="", user_variables={})


class CodeGenerationResult(BaseModel):
    code: str

    @classmethod
    def empty_result(cls) -> "CodeGenerationResult":
        return cls(code="")


class ExecutionResult(BaseModel):
    stdout: str
    stderr: str
    returncode: int

    @property
    def success(self) -> bool:
        return self.returncode == 0

    @classmethod
    def empty_result(cls) -> "ExecutionResult":
        return cls(stdout="", stderr="", returncode=0)


class CodeAssessmentResult(BaseModel):
    analysis: str = Field(...)
    success: bool = Field(...)
    should_retry: bool = Field(...)
    plan: str = Field(default="")
    code: str = Field(default="")

    @classmethod
    def empty_assessment(cls) -> "CodeAssessmentResult":
        return cls(analysis="", success=False, should_retry=False, plan="", code="")

    def to_markdown(self) -> str:
        md = f"**Analysis:** {self.analysis}\n\n"
        md += f"**Success:** {'Yes' if self.success else 'No'}\n\n"
        md += f"**Should Retry:** {'Yes' if self.should_retry else 'No'}\n\n"
        if self.plan:
            md += f"**Plan for Next Attempt:** {self.plan}\n"
        return md


class ExecutionAssessmentHistoryItem(BaseModel):
    code: str
    execution_result: ExecutionResult
    plan: str
    assessment: CodeAssessmentResult

    def generate_agent_message(self) -> AssistantMessage:
        return AssistantMessage(
            content=(
                f"Plan:\n{self.plan}\n\n"
                + f"Execution Result:\n```\n{self.execution_result.stdout}\n{self.execution_result.stderr}\n```\n\n"
                + f"Analysis:\n{self.assessment.analysis}"
            ),
            source="execution_history",
        )
