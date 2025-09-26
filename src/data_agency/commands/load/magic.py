"""
Magic command registration for the load command.
"""

# from data_agency.shared.load_from_db import load_from_db
from IPython.display import display, Markdown
from IPython.core.getipython import get_ipython
import pandas as pd


def load(line="", cell=""):
    # Parse arguments from line
    metadata_lis = line.strip().split()
    shell = get_ipython()

    if len(metadata_lis) == 0:
        help_text = _display_usage()
        display(Markdown(help_text))
        return None

    errors = _validate_metadata_list(metadata_lis, shell.user_ns)  # type: ignore
    if len(errors) > 0:
        error_message = "\n".join(errors)
        display(Markdown(f"**Failed to parse metadata:**\n{error_message}"))
        return None

    _generate_loading_code(metadata_lis, shell)


def _display_usage():
    """
    Display usage instructions for the load command.

    Returns:
        str: Usage instruction message
    """
    usage_text = """
Usage: `$data load <metadata_dataframe_names>`

Description:
Checks metadata dataframe, and create data loading code if metadata looks ok. It does not load data. It generates code to load data. load method generated usually return a pandas DataFrame combining everything. But if metadata is from different frequency or include both bilateral data and unilateral data, it returns a dictionary of dataframes. 

Arguments:
metadata_dataframe_names   Names of the metadata dataframe in your namespace

Examples:
```
$data load my_metadata_df _all_meta
$data load gdp_metadata
```

Requirements:
The metadata dataframe must contain a 'series_code' column with the codes
to load from the database.
    """
    return usage_text.strip()


def _validate_metadata_list(metadata_lis: list[str], user_ns: dict) -> list[str]:
    errors = []
    for metadata_name in metadata_lis:
        if metadata_name not in user_ns:
            errors.append(f"\n Dataframe '{metadata_name}' not found\n")
            continue

        metadata = user_ns[metadata_name]

        if not isinstance(metadata, pd.DataFrame):
            errors.append(f"'{metadata_name}' is not a pandas DataFrame")

        if metadata.empty:
            errors.append(f"'{metadata_name}' is empty dataframe")

        if "series_code" not in metadata.columns:
            errors.append(f"Metadata must contain series_code. ")
    return errors


def _generate_loading_code(metadata_lis: list[str], shell) -> None:
    args = ", ".join(metadata_lis)
    code = f"from data_agency import load\nraw_data = load([{args}])"
    display(Markdown("Data loading code generated to cell below."))
    shell.set_next_input(code, replace=False)
