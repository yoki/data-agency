"""
Integration tests for src/data_agency/analysis/sandbox/runner.py

This test module validates the Docker-based code execution environment,
"""

import subprocess

import pytest
import pandas as pd

from data_agency.analysis.sandbox.runner import execute


@pytest.mark.slow
class TestSandboxRunnerDockerRequired:
    """Tests that require Docker to be available and running."""

    @pytest.fixture(autouse=True)
    def check_docker(self):
        """Ensure Docker is available before running these tests."""
        try:
            result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
            if result.returncode != 0:
                pytest.skip("Docker not available")
        except FileNotFoundError:
            pytest.skip("Docker not installed")

    def test_comprehensive_container_safety(self):
        """Comprehensive test combining all safety features from the sample code."""
        code = """
import os, sys, shutil, subprocess

print("=== Comprehensive Container Safety Test ===")

print("\\nAttempting to write to /outputs ...")
try:
    with open("/outputs/touch_ok.txt", "w", encoding="utf-8") as f:
        f.write("ok")
    print("OK: wrote /outputs/touch_ok.txt")
except Exception as e:
    print(f"FAIL: /outputs write failed: {e}")

print("\\nAttempting to write to /inputs (should fail) ...")
try:
    with open("/inputs/should_fail.txt", "w", encoding="utf-8") as f:
        f.write("nope")
    print("FAIL: /inputs unexpectedly writable")
except Exception:
    print("OK: /inputs is read-only")

print("\\nAttempting reading ...")
try:
    with open("/inputs/code.py", "r", encoding="utf-8") as f:
        txt = f.read()
    print("OK: read /inputs/code.py")
except Exception as e:
    print(f"NG: read failed: {e}")

print("\\nAttempting destructive command (rm -rf /inputs/*) ...")
try:
    subprocess.check_call(["bash", "-lc", "rm -rf /inputs/*"])
    print("NG: rm -rf executed")
except Exception as e:
    print(f"OK: rm blocked or partly failed: {e}")

print("\\n=== Container Safety Test Complete ===")
        """

        result = execute(code, variables={}, image="python:3.13-slim")

        # Basic execution should succeed
        assert result.returncode == 0

        # Verify safety checks
        assert "OK: wrote /outputs/touch_ok.txt" in result.stdout
        assert "OK: /inputs is read-only" in result.stdout
        assert "OK: read /inputs/code.py" in result.stdout
        assert "OK: rm blocked or partly failed" in result.stdout or "NG: rm -rf executed" in result.stdout

        # Should not have unexpected failures
        assert "FAIL: /inputs unexpectedly writable" not in result.stdout
        assert "FAIL: /outputs write failed" not in result.stdout
        assert "NG: read failed" not in result.stdout


if __name__ == "__main__":
    # Allow running individual tests or the full suite
    pytest.main([__file__, "-v"])
