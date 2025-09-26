from IPython.display import display, Markdown

import re


def magic_command_transformer(lines):
    """
    This function intercepts user input before IPython executes it.
    It looks for '$data find' and rewrites it to the correct magic.
    This is needed because IPython does not accept cell magic without cell contents and
    forces to use line magic and cell magic separately.
    So i define $<command> as line/cell magic command.
    """
    if not lines:
        return lines

    for cmd in ["data"]:
        pattern = rf"^\s*#*\s*\$\s*{cmd}\b"
        if re.match(pattern, lines[0], re.IGNORECASE):
            first_line = lines[0]
            is_cell_magic = len(lines) > 1
            magic = f"%%{cmd}" if is_cell_magic else f"%{cmd}"
            lines[0] = re.sub(pattern, magic, first_line, count=1, flags=re.IGNORECASE)
            break  # Only transform the first matching command

    return lines


# @register_line_cell_magic
def run_command(line, cell=""):
    from .commands.find.magic import find
    from .commands.analyze.magic import analyze
    from .commands.load.magic import load
    from .commands.config.magic import config
    from .commands.describe.magic import describe

    command_word = line.split()[0].lower() if len(line.split()) > 0 else ""

    new_line = " ".join(line.split(" ")[1:]) if len(line) > 1 else ""  # remove the command word
    match command_word:
        case "find":
            return find(new_line, cell)
        case "load":
            return load(new_line, cell)
        case "analyze":
            return analyze(new_line, cell)
        case "analysis":
            return analyze(new_line, cell)
        case "code":
            return analyze(new_line, cell)
        case "config":
            return config(new_line, cell)
        case "describe":
            return describe(new_line, cell)
        case _:
            return show_help(new_line, cell)


def load_ipython_extension(ipython):
    ipython.register_magic_function(run_command, "line", "data")
    ipython.register_magic_function(run_command, "cell", "data")
    ipython.input_transformers_cleanup.append(magic_command_transformer)


def show_help(line, cell):
    help_text = """
Available commands:
- `$data find <command>`: Find data based on the provided criteria.
- `$data load <command>`: Load data into the environment.
- `$data describe <dataframes>`: Get a description of the provided dataframes.
- `$data analyze <command>`: Generate the code for analyzing the loaded data.
- `$data config <command>`: Configure data settings.

For more information on each command, use:
`$data <command> help`
"""
    display(Markdown(help_text))
