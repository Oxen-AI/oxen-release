"""
Validation tests to verify the testing infrastructure is properly set up.
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
    def test_fixtures_are_available(self, temp_dir, mock_config, sample_dataframe):
        """Verify custom fixtures are working."""
        assert os.path.exists(temp_dir)
        assert os.path.isdir(temp_dir)
        assert mock_config.get("api_key") == "test_api_key_123"
        assert len(sample_dataframe) == 5
        assert "name" in sample_dataframe.columns
    
    @pytest.mark.unit
    def test_markers_are_defined(self, request):
        """Verify custom markers are properly defined."""
        markers = [marker.name for marker in request.config.iter_markers()]
        assert "unit" in markers
        assert "integration" in markers
        assert "slow" in markers
    
    @pytest.mark.integration
    def test_sample_files_creation(self, sample_csv_file, sample_parquet_file):
        """Verify file creation fixtures work properly."""
        assert os.path.exists(sample_csv_file)
        assert sample_csv_file.endswith(".csv")
        
        assert os.path.exists(sample_parquet_file)
        assert sample_parquet_file.endswith(".parquet")
    
    @pytest.mark.unit
    def test_mock_fixtures(self, mock_http_response, mock_remote_repo):
        """Verify mock fixtures are properly configured."""
        assert mock_http_response.status_code == 200
        assert mock_http_response.json()["status"] == "success"
        
        assert mock_remote_repo.identifier == "test-user/test-repo"
        assert mock_remote_repo.exists() is True
    
    @pytest.mark.unit
    def test_coverage_source_path(self):
        """Verify coverage is configured to track the correct source."""
        # This test verifies the configuration indirectly
        from oxen import Repo
        assert Repo.__module__.startswith("oxen")
    
    @pytest.mark.slow
    def test_slow_marker(self):
        """Test that slow marker is properly defined."""
        import time
        # Simulate a slow test
        time.sleep(0.1)
        assert True


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
        """Verify conftest.py exists and is importable."""
        test_root = Path(__file__).parent
        conftest_path = test_root / "conftest.py"
        assert conftest_path.exists()
        
        # Verify we can import fixtures from conftest
        from conftest import temp_dir, mock_config
        assert temp_dir
        assert mock_config


if __name__ == "__main__":
    # Allow running this file directly for quick validation
    pytest.main([__file__, "-v"])