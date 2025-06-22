"""
Simple validation tests to verify the testing infrastructure is properly set up.
This file doesn't import the oxen module to avoid build dependencies.
"""
import pytest
import os
import sys
import tempfile
from pathlib import Path


class TestInfrastructureValidation:
    """Validate that the testing infrastructure is properly configured."""
    
    @pytest.mark.unit
    def test_pytest_is_installed(self):
        """Verify pytest is installed and importable."""
        import pytest
        assert pytest.__version__
    
    @pytest.mark.unit
    def test_pytest_cov_is_installed(self):
        """Verify pytest-cov is installed."""
        import pytest_cov
        assert pytest_cov
    
    @pytest.mark.unit
    def test_pytest_mock_is_installed(self):
        """Verify pytest-mock is installed."""
        import pytest_mock
        assert pytest_mock
    
    @pytest.mark.unit
    def test_markers_are_defined(self, request):
        """Verify custom markers are properly defined."""
        # Get marker names from the test item
        marker_names = [marker.name for marker in request.node.iter_markers()]
        # The 'unit' marker should be present on this test
        assert "unit" in marker_names
    
    @pytest.mark.integration
    def test_integration_marker(self, request):
        """Test that integration marker is properly defined."""
        marker_names = [marker.name for marker in request.node.iter_markers()]
        assert "integration" in marker_names
    
    @pytest.mark.slow
    def test_slow_marker(self, request):
        """Test that slow marker is properly defined."""
        import time
        marker_names = [marker.name for marker in request.node.iter_markers()]
        assert "slow" in marker_names
        # Simulate a slow test
        time.sleep(0.1)


class TestDirectoryStructure:
    """Validate the testing directory structure."""
    
    @pytest.mark.unit
    def test_test_directories_exist(self):
        """Verify test directory structure is correct."""
        test_root = Path(__file__).parent
        assert test_root.exists()
        assert test_root.name == "tests"
        
        unit_dir = test_root / "unit"
        integration_dir = test_root / "integration"
        
        assert unit_dir.exists()
        assert unit_dir.is_dir()
        assert (unit_dir / "__init__.py").exists()
        
        assert integration_dir.exists()
        assert integration_dir.is_dir()
        assert (integration_dir / "__init__.py").exists()
    
    @pytest.mark.unit
    def test_conftest_exists(self):
        """Verify conftest.py exists."""
        test_root = Path(__file__).parent
        conftest_path = test_root / "conftest.py"
        assert conftest_path.exists()
    
    @pytest.mark.unit
    def test_pyproject_config(self):
        """Verify pyproject.toml has proper test configuration."""
        project_root = Path(__file__).parent.parent
        pyproject_path = project_root / "pyproject.toml"
        assert pyproject_path.exists()
        
        content = pyproject_path.read_text()
        assert "[tool.pytest.ini_options]" in content
        assert "[tool.coverage.run]" in content
        assert "[tool.poetry.scripts]" in content


class TestBasicFixtures:
    """Test basic fixtures that don't require oxen imports."""
    
    @pytest.mark.unit
    def test_temp_directory_creation(self):
        """Test that we can create temporary directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            assert os.path.exists(tmpdir)
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, 'w') as f:
                f.write("test content")
            assert os.path.exists(test_file)


if __name__ == "__main__":
    # Allow running this file directly for quick validation
    pytest.main([__file__, "-v"])