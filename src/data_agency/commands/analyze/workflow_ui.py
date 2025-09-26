from .models import (
    CodeGenerationRequest,
    ExecutionResult,
    CodeAssessmentResult,
)

from typing import Protocol, Optional


class UI(Protocol):
    def show_generated_code(
        self, code: str, explanation: Optional[str] = None, trial_number: Optional[int] = None
    ): ...
    def show_results(self, execution_result: ExecutionResult, trial_number: Optional[int] = None): ...
    def show_assessment(self, assessment: CodeAssessmentResult): ...
    def process_final_output(self, request: CodeGenerationRequest, code: str): ...
    def clean_code_section(self): ...


class ConsoleUI:
    def show_generated_code(self, code: str, explanation: Optional[str] = None, trial_number: Optional[int] = None):
        content = []
        if explanation:
            content.append(f"**Code Explanation:**\n\n{explanation}")
        title = "**Generated Code:**" if not trial_number else f"**Generated Code (Attempt {trial_number}):**"
        content.append(title)
        content.append(f"```python\n{code}\n```")
        print("\n".join(content))
        print()

    def show_results(self, execution_result: ExecutionResult, trial_number: Optional[int] = None):
        title = "**Execution Results:**" if not trial_number else f"**Execution Results (Attempt {trial_number}):**"
        content = [title]
        stdout = execution_result.stdout
        if len(stdout) > 1000:
            stdout = stdout[:1000] + "\n... (output truncated)"
        content.append(f"```\n{stdout}\n```")
        if execution_result.stderr:
            content.append(f"```stderr\n{execution_result.stderr}\n```")
        print("\n".join(content))
        print()

    def show_assessment(self, assessment: CodeAssessmentResult):
        if assessment.success:
            print("**Code Assessment:** The generated code meets the requirements.")
        else:
            print(assessment.to_markdown())
        print()

    def process_final_output(self, request: CodeGenerationRequest, code: str):
        text = request.request_text.replace("\n", " ")
        header = f"# User request: {text}\n\n"
        print(header + code)
        print()

    def clean_code_section(self):
        pass
