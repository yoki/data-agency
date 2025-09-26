from typing import List

from .models import (
    CodeGenerationRequest,
    CodeGenerationResult,
    ExecutionResult,
    CodeAssessmentResult,
    ExecutionAssessmentHistoryItem,
)
from .llm_service import CodeGenerationService, AssessmentService
from ...common.llm_client import FullLogChatClientCache
from .sandbox.runner import execute as sandbox_execute
from .workflow_ui import UI, ConsoleUI


class AgentWorkflow:
    """Straightforward generate→execute→assess loop, no external state machine dependency."""

    def __init__(
        self,
        client: FullLogChatClientCache,
        request: CodeGenerationRequest,
        *,
        ui: UI = ConsoleUI(),
        max_code_generation: int = 3,
    ):
        self.request = request
        self.codegen = CodeGenerationService(client)
        self.assessor = AssessmentService(client)
        self.ui = ui
        self.max_code_generation = max_code_generation
        self.code_generation_count = 0

        self.current_code: str = ""
        self.code_result: CodeGenerationResult = CodeGenerationResult.empty_result()
        self.execution_result: ExecutionResult = ExecutionResult.empty_result()
        self.assessment: CodeAssessmentResult = CodeAssessmentResult.empty_assessment()
        self.history: List[ExecutionAssessmentHistoryItem] = []

    async def run(self) -> str:
        # First generation
        self.code_result = await self.codegen.generate_code(self.request)
        self.current_code = self.code_result.code
        self.ui.show_generated_code(self.current_code, trial_number=self.code_generation_count + 1)

        while True:
            # Execute
            self.execution_result = sandbox_execute(self.current_code, self.request.user_variables)
            self.ui.show_results(self.execution_result, trial_number=self.code_generation_count + 1)

            # Assess and regenerate if needed
            orig_plan = self.assessment.plan
            self.assessment = await self.assessor.assess_code_output(
                self.request, self.execution_result, self.current_code, self.history
            )
            self.ui.show_assessment(self.assessment)
            self.code_generation_count += 1

            self.history.append(
                ExecutionAssessmentHistoryItem(
                    plan=orig_plan,
                    code=self.current_code,
                    execution_result=self.execution_result,
                    assessment=self.assessment,
                )
            )

            if self.assessment.success:
                self.ui.process_final_output(self.request, self.current_code)
                self.ui.clean_code_section()
                return self.current_code

            if self.code_generation_count >= self.max_code_generation:
                return self.current_code

            if self.assessment.should_retry and self.assessment.code:
                self.current_code = self.assessment.code
                continue
