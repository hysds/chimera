"""
Pytest configuration and shared fixtures for Chimera unit tests.
"""
import os
import tempfile
import pytest
from pathlib import Path


@pytest.fixture
def test_fixtures_dir():
    """Return the path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_chimera_config(test_fixtures_dir):
    """Return path to sample chimera config file."""
    return test_fixtures_dir / "sample_chimera_config.yaml"


@pytest.fixture
def sample_pge_config(test_fixtures_dir):
    """Return path to sample PGE config file."""
    return test_fixtures_dir / "sample_pge_config.json"


@pytest.fixture
def sample_sf_context(test_fixtures_dir):
    """Return path to sample SciFlo context file."""
    return test_fixtures_dir / "sample_sf_context.json"


@pytest.fixture
def sample_settings(test_fixtures_dir):
    """Return path to sample settings file."""
    return test_fixtures_dir / "sample_settings.yaml"


@pytest.fixture
def invalid_config(test_fixtures_dir):
    """Return path to invalid config file for error testing."""
    return test_fixtures_dir / "invalid_config.yaml"


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_config_file(temp_dir):
    """Create a temporary config file."""
    config_content = """
test_setting: test_value
nested:
  key1: value1
  key2: value2
"""
    config_file = temp_dir / "temp_config.yaml"
    config_file.write_text(config_content)
    return config_file


@pytest.fixture
def nonexistent_file(temp_dir):
    """Return path to a non-existent file."""
    return temp_dir / "nonexistent.yaml"