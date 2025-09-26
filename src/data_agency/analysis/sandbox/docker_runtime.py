import subprocess
import os
from pathlib import Path
from typing import Optional
import time
import platform
from importlib.resources import files
import tempfile


class DockerRuntime:
    """
    Docker runtime that uses an external Dockerfile for building the sandbox image.
    """

    def __init__(self, image: Optional[str] = None):
        # You can prebuild/pull an image and set CODEGEN_AGENT_RUNNER_IMAGE to skip builds.
        self.image = image or os.environ.get("CODEGEN_AGENT_RUNNER_IMAGE", "codegen-agent-runner:py313")
        self.is_windows = platform.system() == "Windows"

    def _run(self, cmd: list[str]) -> subprocess.CompletedProcess:
        if self.is_windows and cmd[0] == "docker":
            cmd = ["wsl.exe"] + cmd
        elif self.is_windows and cmd[0] == "dockerd":
            cmd = ["wsl.exe"] + cmd
        return subprocess.run(cmd, capture_output=True, text=True)

    def ensure_docker(self) -> None:
        """Ensure Docker daemon is running, start it if necessary."""
        # Check if Docker is already running
        check_proc = self._run(["docker", "ps"])
        if check_proc.returncode == 0:
            return  # Docker is already running

        print("Starting Docker daemon...")

        with open("/var/log/dockerd.log", "w") as log_file:
            subprocess.Popen(["dockerd"], stdout=log_file, stderr=subprocess.STDOUT, start_new_session=True)

        timeout = 10
        socket_path = Path("/var/run/docker.sock")
        while not socket_path.exists():
            if timeout <= 0:
                raise RuntimeError("Docker daemon failed to start within 20 seconds")
            time.sleep(1)
            timeout -= 1

    def _normalize_path(self, path: str) -> str:
        resolved_path = Path(path).resolve()

        if self.is_windows:
            # Convert Windows path to format that works in all shells
            path_str = str(resolved_path)
            # Convert C:\path\to\dir to /c/path/to/dir for Git Bash compatibility
            if path_str[1:3] == ":\\":
                drive = path_str[0].lower()
                rest = path_str[3:].replace("\\", "/")
                return f"/mnt/{drive}/{rest}"
            return path_str.replace("\\", "/")
        else:
            return str(resolved_path)

    def ensure_image(self) -> None:
        self.ensure_docker()
        # Fast path: image already present.
        insp = self._run(["docker", "image", "inspect", self.image])
        if insp.returncode == 0:
            # print(f"Docker image '{self.image}' already exists, using cached version")
            return

        print(f"Docker image '{self.image}' not found, building...")

        # Use the external Dockerfile.runner by default
        dockerfile_content = files("codegen_agent").joinpath("sandbox/Dockerfile.runner").read_text(encoding="utf-8")
        tmp_dockerfile_path = "./tmp_dockerfile"
        with open(tmp_dockerfile_path, "w") as f:
            f.write(dockerfile_content)

        try:
            cmd = ["docker", "build", "-t", self.image, "-f", tmp_dockerfile_path, "."]
            proc = self._run(cmd)
            if proc.returncode != 0:
                msg = [
                    "Failed to build sandbox image.",
                    f"Command: {' '.join(cmd)}",
                    f"Return code: {proc.returncode}",
                    f"STDOUT:\n{proc.stdout.strip()}",
                    f"STDERR:\n{proc.stderr.strip()}",
                ]
                raise RuntimeError("\n".join(msg))
            print(f"Successfully built image '{self.image}'")
        finally:
            Path(tmp_dockerfile_path).unlink(missing_ok=True)

    def run(self, inputs_dir: str, outputs_dir: str) -> subprocess.CompletedProcess:
        self.ensure_docker()

        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{self._normalize_path(inputs_dir)}:/inputs:ro",
            "-v",
            f"{self._normalize_path(outputs_dir)}:/outputs:rw",
            self.image,
            "python",
            "-u",
            "/inputs/prelude.py",
        ]
        proc = self._run(cmd)
        # Some Docker errors appear only on stdout; surface both if needed.
        if proc.returncode != 0 and not proc.stderr:
            proc.stderr = proc.stdout
        return proc
