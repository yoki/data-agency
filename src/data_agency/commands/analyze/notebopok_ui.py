# Display for iPython environments
from typing import Optional, Dict, Any
from IPython.display import display, Markdown
from IPython.core.getipython import get_ipython

from .models import (
    CodeAssessmentResult,
    CodeGenerationRequest,
    ExecutionResult,
)


class DisplayService:
    def __init__(self):
        self.ipython_shell = get_ipython()
        self._display_handles: Dict[str, Any] = {}

    def show_generated_code(self, code: str, explanation: Optional[str] = None, trial_number: Optional[int] = None):
        content = []
        if explanation:
            content.append(f"**Code Explanation:**\n\n{explanation}")
        title = "**Generated Code:**" if not trial_number else f"**Generated Code (Attempt {trial_number}):**"
        content.append(title)
        content.append(f"```python\n{code}\n```")
        md = "\n\n".join(content)
        if "generated_code" in self._display_handles:
            self._display_handles["generated_code"].update(Markdown(md))
        else:
            handle = display(Markdown(md), display_id="generated_code")
            self._display_handles["generated_code"] = handle

    def show_assessment(self, assessment: CodeAssessmentResult):
        if assessment.success:
            display(Markdown("**Code Assessment:** The generated code meets the requirements."))
        else:
            display(Markdown(assessment.to_markdown()))

    def show_results(self, execution_result: ExecutionResult, trial_number: Optional[int] = None):
        title = "**Execution Results:**" if not trial_number else f"**Execution Results (Attempt {trial_number}):**"
        content = [title]
        stdout = execution_result.stdout
        if len(stdout) > 1000:
            stdout = stdout[:1000] + "\n... (output truncated)"
        content.append(f"```\n{stdout}\n```")
        if execution_result.stderr:
            content.append(f"```stderr\n{execution_result.stderr}\n```")
        md = "\n\n".join(content)
        if "execution_results" in self._display_handles:
            self._display_handles["execution_results"].update(Markdown(md))
        else:
            handle = display(Markdown(md), display_id="execution_results")
            self._display_handles["execution_results"] = handle

    def process_final_output(self, request: CodeGenerationRequest, code: str):
        if self.ipython_shell is not None:
            text = request.request_text.replace("\n", " ")
            header = f"# User request: {text}\n\n"
            self.ipython_shell.set_next_input(header + code, replace=False)
            display(Markdown("*Code has been saved to the next cell.*"))

    def clean_code_section(self):
        if "generated_code" in self._display_handles:
            self._display_handles["generated_code"].update(Markdown(""))
