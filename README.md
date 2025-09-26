# data-agency
Agentic data retrival and analysis


## Installation
```bash
pip install data-agency@git+https://github.com/yoki/data-agency.git
```

Other than pip, docker container and LLM (Gemini) API setup is needed.

### Docker
Docker must be installed. Tested with docker installed in WSL, not in docker desptop. 

To use in devcontainer, you should install official docker, not repository one, and use fuse-overlayfs (or overlay2). Default vfs is slow (as of Aug 2025). 
```dockerfile
RUN curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh && rm get-docker.sh
RUN mkdir -p /etc/docker && \
    echo '{\n  "storage-driver": "fuse-overlayfs"\n}' > /etc/docker/daemon.json
```

### API key and other env vars
Create `.env` file:
```bash
GEMINI_API_KEY_FOR_DATA_AGENCY=your_gemini_api_key_here
```

**File locations (priority order):**
1. `$DATA_AGENCY_DOTENV_PATH` (if set)
2. `./env` (current directory)
3. `/secrets/data_agency/.env` (Docker/devcontainer)
4. `~/.config/codegen-agent/.env` (Linux/WSL)
5. `%LOCALAPPDATA%\data_agency\data_agency\.env` (Windows)

```json
    "mounts": [
        "type=bind,source=/mnt/c/my-path-to-secret,target=/secrets/data_agency,readonly",
    ],
```

