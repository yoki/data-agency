"""
Agent for the analyze command.
Orchestrates the workflow and coordinates between services using a state machine.
"""

from typing import Dict, Any
from IPython.display import display, Markdown
from IPython.core.getipython import get_ipython

# from data_agency.commands.analyze.display_service import DisplayService
# from data_agency.commands.analyze.models import CodeGenerationRequest
import pandas as pd
from lettuce_logger import pp
from mydevtools import no_warning
# from data_agency.commands.analyze.workflow import AgentWorkflow

from ...common.styles import apply_custom_styles


class DataDescribeAgent:
    """Agent that orchestrates the analyze workflow."""

    def __init__(self):
        """Initialize the agent."""
        self.MAX_RETRIES = 3

    def run(self, line: str, cell: str = ""):
        if self._handle_utility_commands(line, cell):
            return

        self._run_describe(line, cell)

    def _handle_utility_commands(self, line: str, cell: str) -> bool:
        command = line.lower().strip().split(" ")[0]

        if command.lower().strip() in ["help", "--help", "-h", "?", ""]:
            show_help()
            return True

        return False

    def _run_describe(self, line: str, cell: str):
        request_args = line.strip().split(" ")
        user_variables = self._get_user_variables(request_args)

        g20 = ["AR", "AU", "BR", "CA", "FR", "DE", "IN", "IT", "MX", "RU", "SA", "ZA", "TR", "GB", "US"]
        asean_plus3 = ["CN", "JP", "KR", "ID", "MY", "PH", "SG", "TH", "BN", "KH", "LA", "MM", "VN"]

        target_countries = cell.strip().split("--target-countries=")[-1] if "--target-countries=" in cell else "ASEAN3"
        if target_countries.lower() == "major":
            target_countries = g20 + asean_plus3
        elif target_countries.lower() == "g20":
            target_countries = g20 + asean_plus3
        elif len(target_countries) == 2:
            target_countries = [target_countries.upper()]
        else:
            target_countries = asean_plus3

        for var_name in user_variables.keys():
            variable = user_variables[var_name]
            if not isinstance(variable, pd.DataFrame):
                display(Markdown(f"**Variable '{var_name}' not dataframe.**"))
                continue

            try:
                original_max_rows = pd.get_option("display.max_rows")
                pd.set_option("display.max_rows", 200)

                describe_dataframe(variable, var_name, target_countries=target_countries)
            finally:
                pd.set_option("display.max_rows", original_max_rows)

    def _get_user_variables(self, request_args: list[str]) -> Dict[str, Any]:
        user_ns = get_ipython().user_ns  # type: ignore
        variables_from_user_ns = {}
        for var_name in request_args:
            if var_name in user_ns:
                value = user_ns[var_name]
                if isinstance(value, dict):
                    for k, v in value.items():
                        variables_from_user_ns[f"{var_name}_{k}"] = v
                else:
                    variables_from_user_ns[var_name] = value
        return variables_from_user_ns


def describe_dataframe(df: pd.DataFrame, var_name: str, target_countries: list[str]):
    """Describes the dataframe and displays its summary."""
    display(Markdown(f"**Describing DataFrame: {var_name}**"))
    columns = df.columns.tolist()
    display(Markdown(f"**columns:** {columns}"))
    frequency = df.attrs.get("frequency", "NA")
    display(Markdown(f"**frequency:** {frequency}"))
    key_cols = ["time", "reporter", "counterpart", "reporter_gr", "cpart_gr", "ccode", "cgroup"]
    variables = [c for c in columns if c not in key_cols]

    if len(target_countries) == 1:
        df2 = df[df["ccode"].isin(target_countries)].copy()
        df2 = df2[variables]
        non_missing_counts = df2.notna().sum()

        divisor = 12 if frequency == "M" else 4 if frequency == "Q" else 1
        if frequency == "NA":
            display(Markdown(f"**Data counts:**"))
            display(non_missing_counts)
        else:
            non_missing_counts = non_missing_counts / divisor
            display(Markdown(f"**Data avaiability in years for {target_countries[0]}:**"))
            display(non_missing_counts)

    else:
        df2 = df[df["ccode"].isin(target_countries)].copy()
        all_ccodes = pd.DataFrame({"ccode": target_countries})
        # pp(all_ccodes)
        display(Markdown(f"---\n**Data avaiability for each variables**"))

        for var in variables:
            if var in df.attrs.get("column_description", {}):
                description = df.attrs["column_description"][var]
                display(Markdown(f"---\n**{var}**: {description}"))
            else:
                display(Markdown(f"---\n{var}:"))

            df_nonmissing = df2[[var, "ccode"]].dropna(subset=[var])
            obs_count = df_nonmissing["ccode"].value_counts().reset_index()
            obs_count.columns = ["ccode", "n_obs"]
            obs_counts = pd.merge(all_ccodes, obs_count, on="ccode", how="left").fillna(0)

            # Separate countries with zero observations
            zero_obs_ccodes = obs_counts[obs_counts["n_obs"] == 0]
            zero_obs_ccodes["category"] = "zero"

            # Process countries with non-zero observations
            non_zero_obs_ccodes = obs_counts[obs_counts["n_obs"] > 0].copy()
            if len(non_zero_obs_ccodes) >= 3:
                non_zero_obs_ccodes["rank"] = non_zero_obs_ccodes["n_obs"].rank(method="first")
                non_zero_obs_ccodes["category"] = pd.qcut(
                    non_zero_obs_ccodes["rank"], 3, labels=["low", "mid", "high"], duplicates="drop"
                )
            elif len(non_zero_obs_ccodes) > 0:
                non_zero_obs_ccodes["category"] = "nonzero"

            # Concatenate the two groups
            final_counts = pd.concat([zero_obs_ccodes, non_zero_obs_ccodes], ignore_index=True)

            grouped = (
                final_counts.groupby("category")
                .agg(
                    n_ccode=("ccode", "count"),
                    min_n_obs=("n_obs", "min"),
                    max_n_obs=("n_obs", "max"),
                    ccodes=("ccode", list),
                )
                .reset_index()
            )
            if frequency == "NA":
                grouped["nobs_range"] = grouped.apply(
                    lambda x: f"{int(x['min_n_obs'])} to {int(x['max_n_obs'])}", axis=1
                )
                grouped.sort_values(by=["max_n_obs", "min_n_obs"], ascending=False, inplace=True)
                display(grouped[["nobs_range", "ccodes"]].reset_index(drop=True))
            else:
                freq_divisor = 12 if frequency == "M" else 4 if frequency == "Q" else 1
                grouped["nyears"] = grouped.apply(
                    lambda x: f"{int(x['min_n_obs'] / freq_divisor)} to {int(x['max_n_obs'] / freq_divisor)} years",
                    axis=1,
                )
                grouped.sort_values(by=["max_n_obs", "min_n_obs"], ascending=False, inplace=True)
                display(grouped[["nyears", "ccodes"]].reset_index(drop=True))


def show_help():
    """
    Display help information for the analyze command.

    This method shows usage instructions and examples to help users
    understand how to use the command effectively.
    """
    apply_custom_styles()
    display(Markdown(HELP_TEXT))


HELP_TEXT = """
### `$data describe` Magic Command

```
$data describe df1 
--target-countries=<ASEAN3/Major/<2-letter country code>>
```
Target ccountries are optional. If not provided, ASEAN3 countries will be used by default. Major countries include G20 and ASEAN3.
"""
