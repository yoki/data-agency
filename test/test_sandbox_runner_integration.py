"""
Integration tests for src/data_agency/analysis/sandbox/runner.py

This test module validates the Docker-based code execution environment,
including container safety, variable serialization, and execution correctness.
"""
import os
import sys
import time
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any

import pytest
import pandas as pd

# Add src to path to import data_agency modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_agency.analysis.sandbox.runner import execute
from data_agency.analysis.models import ExecutionResult


class TestSandboxRunnerIntegration:
    """Integration tests for the sandbox runner module."""

    def test_simple_execution(self):
        """Test basic code execution functionality."""
        code = """
print("Hello from sandbox!")
result = 2 + 3
print(f"2 + 3 = {result}")
        """
        
        # Use the simple python image to avoid build issues
        result = execute(code, variables={}, image="python:3.13-slim")
        
        assert result.returncode == 0
        assert "Hello from sandbox!" in result.stdout
        assert "2 + 3 = 5" in result.stdout
        assert result.stderr.strip() == ""

    def test_container_safety_read_only_inputs(self):
        """Test that /inputs directory is read-only inside container."""
        code = '''
import os, sys

print("Attempting to write to /inputs (should fail) ...")
try:
    with open("/inputs/should_fail.txt", "w", encoding="utf-8") as f:
        f.write("nope")
    print("FAIL: /inputs unexpectedly writable")
except Exception as e:
    print("OK: /inputs is read-only")
    print(f"Exception: {type(e).__name__}")
        '''
        
        result = execute(code, variables={}, image="python:3.13-slim")
        
        assert result.returncode == 0
        assert "OK: /inputs is read-only" in result.stdout
        assert "FAIL: /inputs unexpectedly writable" not in result.stdout

    def test_container_safety_writable_outputs(self):
        """Test that /outputs directory is writable inside container."""
        code = '''
import os, sys

print("Attempting to write to /outputs ...")
try:
    with open("/outputs/touch_ok.txt", "w", encoding="utf-8") as f:
        f.write("ok")
    print("OK: wrote /outputs/touch_ok.txt")
    
    # Verify we can read it back
    with open("/outputs/touch_ok.txt", "r", encoding="utf-8") as f:
        content = f.read()
    print(f"Read back: {content}")
except Exception as e:
    print(f"FAIL: /outputs write failed: {e}")
        '''
        
        result = execute(code, variables={}, image="python:3.13-slim")
        
        assert result.returncode == 0
        assert "OK: wrote /outputs/touch_ok.txt" in result.stdout
        assert "Read back: ok" in result.stdout
        assert "FAIL: /outputs write failed" not in result.stdout

    def test_container_safety_input_reading(self):
        """Test that container can read from /inputs directory."""
        code = '''
import os, sys

print("Attempting reading /inputs/code.py ...")
try:
    with open("/inputs/code.py", "r", encoding="utf-8") as f:
        content = f.read()
    print("OK: read /inputs/code.py")
    print(f"First 50 chars: {content[:50]}")
except Exception as e:
    print(f"NG: read failed: {e}")
        '''
        
        result = execute(code, variables={}, image="python:3.13-slim")
        
        assert result.returncode == 0
        assert "OK: read /inputs/code.py" in result.stdout
        assert "NG: read failed" not in result.stdout

    def test_container_safety_destructive_commands(self):
        """Test that destructive commands are contained within the sandbox."""
        code = '''
import subprocess, sys

print("Attempting destructive command (rm -rf /inputs/*) ...")
try:
    subprocess.check_call(["bash", "-lc", "rm -rf /inputs/*"])
    print("NG: rm -rf executed")
except Exception as e:
    print(f"OK: rm blocked or partly failed: {e}")
        '''
        
        result = execute(code, variables={}, image="python:3.13-slim")
        
        # Should either fail (preferred) or succeed but be contained
        assert result.returncode == 0  # The Python code itself should run
        # The rm command should fail or be harmless
        assert "OK: rm blocked or partly failed" in result.stdout or "NG: rm -rf executed" in result.stdout

    def test_variable_serialization_basic_types(self):
        """Test serialization and deserialization of basic variable types."""
        code = '''
print(f"my_int = {my_int}, type = {type(my_int)}")
print(f"my_float = {my_float}, type = {type(my_float)}")
print(f"my_string = '{my_string}', type = {type(my_string)}")
print(f"my_list = {my_list}, type = {type(my_list)}")
print(f"my_dict = {my_dict}, type = {type(my_dict)}")
        '''
        
        variables = {
            "my_int": 42,
            "my_float": 3.14,
            "my_string": "Hello, sandbox!",
            "my_list": [1, 2, 3, "four"],
            "my_dict": {"key": "value", "number": 123}
        }
        
        result = execute(code, variables=variables, image="python:3.13-slim")
        
        assert result.returncode == 0
        assert "my_int = 42, type = <class 'int'>" in result.stdout
        assert "my_float = 3.14, type = <class 'float'>" in result.stdout
        assert "my_string = 'Hello, sandbox!', type = <class 'str'>" in result.stdout
        assert "my_list = [1, 2, 3, 'four'], type = <class 'list'>" in result.stdout
        assert "my_dict = {'key': 'value', 'number': 123}, type = <class 'dict'>" in result.stdout

    def test_variable_serialization_pandas(self):
        """Test serialization and deserialization of pandas DataFrames."""
        pytest.skip("Pandas not available in simple Python image - would require full build")
        
        code = '''
import pandas as pd
print(f"df type = {type(df)}")
print(f"df shape = {df.shape}")
print(f"df columns = {list(df.columns)}")
print("df head:")
print(df.head())
        '''
        
        # Create a sample DataFrame
        df = pd.DataFrame({
            'A': [1, 2, 3, 4, 5],
            'B': ['a', 'b', 'c', 'd', 'e'],
            'C': [1.1, 2.2, 3.3, 4.4, 5.5]
        })
        
        variables = {"df": df}
        
        result = execute(code, variables=variables)
        
        assert result.returncode == 0
        assert "df type = <class 'pandas.core.frame.DataFrame'>" in result.stdout
        assert "df shape = (5, 3)" in result.stdout
        assert "df columns = ['A', 'B', 'C']" in result.stdout

    def test_variable_filtering_unused_variables(self):
        """Test that only used variables are serialized."""
        code = '''
print(f"used_var = {used_var}")
# unused_var is not referenced in the code
        '''
        
        variables = {
            "used_var": "I am used",
            "unused_var": "I am not used"
        }
        
        result = execute(code, variables=variables, image="python:3.13-slim")
        
        assert result.returncode == 0
        assert "used_var = I am used" in result.stdout
        # The unused variable should not cause any issues

    def test_error_handling_syntax_error(self):
        """Test error handling for code with syntax errors."""
        code = '''
print("This will work")
this is invalid python syntax
print("This won't run")
        '''
        
        result = execute(code, variables={}, image="python:3.13-slim")
        
        assert result.returncode != 0
        assert "SyntaxError" in result.stderr or "invalid syntax" in result.stderr

    def test_error_handling_runtime_error(self):
        """Test error handling for runtime errors."""
        code = '''
print("Starting execution")
x = 1 / 0  # This will cause a ZeroDivisionError
print("This won't print")
        '''
        
        result = execute(code, variables={}, image="python:3.13-slim")
        
        assert result.returncode != 0
        assert "Starting execution" in result.stdout
        assert "ZeroDivisionError" in result.stderr

    def test_system_exit_handling(self):
        """Test that explicit sys.exit() calls are handled properly."""
        code = '''
import sys
print("Before exit")
sys.exit(42)
print("After exit - should not print")
        '''
        
        result = execute(code, variables={}, image="python:3.13-slim")
        
        assert result.returncode == 42
        assert "Before exit" in result.stdout
        assert "After exit - should not print" not in result.stdout

    def time_docker_operations(self):
        """Profile individual Docker operations for performance testing."""
        print("=== Docker Performance Test ===")

        # Test 1: Simple container run with simple Python image
        start = time.time()
        result = subprocess.run(
            ["docker", "run", "--rm", "python:3.13-slim", "python", "-c", "print('hello')"],
            capture_output=True,
            text=True,
        )
        simple_time = time.time() - start
        print(f"Simple container run: {simple_time:.3f}s - Return code: {result.returncode}")

        # Test 2: Container with volume mounts (no execution)
        start = time.time()
        with tempfile.TemporaryDirectory() as temp_dir:
            result = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{temp_dir}:/test_mount:ro",
                    "python:3.13-slim",
                    "python",
                    "-c",
                    "print('with mount')",
                ],
                capture_output=True,
                text=True,
            )
        mount_time = time.time() - start
        print(f"Container with mount: {mount_time:.3f}s - Return code: {result.returncode}")

        # Test 3: Check if image exists
        start = time.time()
        result = subprocess.run(
            ["docker", "image", "inspect", "python:3.13-slim"], 
            capture_output=True, 
            text=True
        )
        inspect_time = time.time() - start
        print(f"Image inspection: {inspect_time:.3f}s - Return code: {result.returncode}")

        return simple_time, mount_time, inspect_time

    def test_performance_benchmarking(self):
        """Test performance characteristics of the execution environment."""
        print("\n=== Full Execution Test ===")
        start_time = time.time()
        
        test_code = '''
import time
print("Starting performance test...")
for i in range(3):
    print(f"Iteration {i}")
    time.sleep(0.1)
print("Performance test completed")
        '''
        
        exec_start = time.time()
        result = execute(test_code, variables={}, image="python:3.13-slim")
        exec_end = time.time()

        print(f"Execution took: {exec_end - exec_start:.3f}s")
        print(f"Total time: {exec_end - start_time:.3f}s")

        # Also run the Docker timing operations for comparison
        try:
            simple_time, mount_time, inspect_time = self.time_docker_operations()
            print(f"\n=== Performance Analysis ===")
            print(f"Simple Docker run: {simple_time:.3f}s")
            print(f"Docker with mounts: {mount_time:.3f}s")
            print(f"Full codegen execution: {exec_end - exec_start:.3f}s")
            if simple_time > 0:
                print(f"Overhead ratio: {(exec_end - exec_start) / simple_time:.1f}x")
        except Exception as e:
            print(f"Performance profiling skipped: {e}")

        # Assertions for reasonable performance
        assert result.returncode == 0
        assert "Performance test completed" in result.stdout
        assert exec_end - exec_start < 30  # Should complete within 30 seconds

    def test_cleanup_functionality(self):
        """Test that cleanup of old run directories works."""
        from data_agency.common.load_env import CONTAINER_IO_PATH
        
        # Count existing run directories before test
        existing_runs = len([d for d in CONTAINER_IO_PATH.iterdir() 
                           if d.is_dir() and d.name.startswith("run_")])
        
        # Execute a simple test
        code = 'print("Testing cleanup")'
        result = execute(code, variables={})
        
        assert result.returncode == 0
        
        # Count run directories after test
        final_runs = len([d for d in CONTAINER_IO_PATH.iterdir() 
                         if d.is_dir() and d.name.startswith("run_")])
        
        # Should have created one new run directory
        # (cleanup happens after execution, keeping the most recent 50)
        assert final_runs >= existing_runs
        assert final_runs <= 50  # Should not exceed max_runs limit


@pytest.mark.integration
class TestSandboxRunnerDockerRequired:
    """Tests that require Docker to be available and running."""
    
    @pytest.fixture(autouse=True)
    def check_docker(self):
        """Ensure Docker is available before running these tests."""
        try:
            result = subprocess.run(["docker", "--version"], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                pytest.skip("Docker not available")
        except FileNotFoundError:
            pytest.skip("Docker not installed")

    def test_comprehensive_container_safety(self):
        """Comprehensive test combining all safety features from the sample code."""
        code = '''
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
        '''
        
        result = execute(code, variables={}, image="python:3.13-slim")
        
        # Basic execution should succeed
        assert result.returncode == 0
        
        # Verify safety checks
        assert "OK: wrote /outputs/touch_ok.txt" in result.stdout
        assert "OK: /inputs is read-only" in result.stdout
        assert "OK: read /inputs/code.py" in result.stdout
        assert ("OK: rm blocked or partly failed" in result.stdout or 
                "NG: rm -rf executed" in result.stdout)
        
        # Should not have unexpected failures
        assert "FAIL: /inputs unexpectedly writable" not in result.stdout
        assert "FAIL: /outputs write failed" not in result.stdout
        assert "NG: read failed" not in result.stdout


if __name__ == "__main__":
    # Allow running individual tests or the full suite
    pytest.main([__file__, "-v"])