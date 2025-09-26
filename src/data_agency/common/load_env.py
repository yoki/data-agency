# Determines the path for storing state and look for API keys.

import os
from pathlib import Path
from platformdirs import PlatformDirs
from dotenv import load_dotenv


# -------------------------------------------------
# PC specific path for standalone installation
# -------------------------------------------------
def _state_path() -> tuple[Path, Path]:
    # Linux: ~/.local/state/data_agency/
    # Windows: %LOCALAPPDATA%\data_agency\State\
    # Devcontainer override: DATA_AGENCY_STATE=/workspaces/data_agency/state (or any path)

    # First, try to load dotenv from common locations to get potential DATA_AGENCY_STATE override
    potential_dotenv_paths = [
        Path.cwd() / ".env",  # Current working directory
        Path.home() / ".env",  # User home directory
        Path("/secrets") / "data_agency" / ".env",  # Common devcontainer path
    ]

    # Also check if explicit override is set
    if override_path := os.environ.get("DATA_AGENCY_DOTENV_PATH"):
        potential_dotenv_paths.insert(0, Path(override_path))

    # Load dotenv from first available location (without overriding existing env vars)
    for dotenv_path in potential_dotenv_paths:
        if dotenv_path.is_file():
            load_dotenv(dotenv_path, override=False)
            break

    # Now check for state path override after dotenv is loaded
    if state_override := os.environ.get("DATA_AGENCY_STATE"):
        found_path = Path(state_override).expanduser().resolve()
    else:
        d = PlatformDirs(appname="data_agency")
        found_path = Path(d.user_state_dir)  # single root, portable

    # try to load dotenv
    if (found_path / ".env").is_file():
        # this should load LLM key
        load_dotenv(found_path / ".env", override=False)

    # load data root path
    if data_root := os.environ.get("DATA_AGENCY_DATA_ROOT"):
        pass
    else:
        data_root = "C:\\mydata\\amro-asia.org\\AFSR24 - Dollar Financing - General\\20_Data\\new_download"

    return found_path, Path(data_root)


STATE_PATH, DATA_ROOT = _state_path()
LOG_PATH = STATE_PATH / "logs"
CACHE_PATH = STATE_PATH / "cache"
CONTAINER_IO_PATH = STATE_PATH / "generated"
METADATA_PATH = DATA_ROOT / "data_agency_data"

if not STATE_PATH.parent.exists():
    raise FileNotFoundError(
        f"Parent of state directoy does not exist. Create it and set .env file following readme of codegen-agent: {STATE_PATH}"
    )

for _p in (STATE_PATH, LOG_PATH, CACHE_PATH, CONTAINER_IO_PATH):
    _p.mkdir(exist_ok=True)
