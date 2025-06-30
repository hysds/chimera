"""
Unit tests for chimera.commons.conf_util module.
"""
import json
import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open

from chimera.commons.conf_util import YamlConf, YamlConfError, load_config


class TestYamlConf:
    """Test cases for YamlConf class."""

    def test_yamlconf_init_valid_file(self, sample_chimera_config):
        """Test YamlConf initialization with valid YAML file."""
        conf = YamlConf(str(sample_chimera_config))
        
        assert conf.file == str(sample_chimera_config)
        assert isinstance(conf.cfg, dict)
        assert "preprocessor" in conf.cfg
        assert "job_submitter" in conf.cfg

    def test_yamlconf_init_nonexistent_file(self, nonexistent_file):
        """Test YamlConf initialization with non-existent file."""
        with pytest.raises(FileNotFoundError):
            YamlConf(str(nonexistent_file))

    def test_yamlconf_init_invalid_yaml(self, invalid_config):
        """Test YamlConf initialization with invalid YAML."""
        with pytest.raises(Exception):  # yaml.YAMLError or similar
            YamlConf(str(invalid_config))

    def test_yamlconf_file_property(self, sample_chimera_config):
        """Test YamlConf file property."""
        conf = YamlConf(str(sample_chimera_config))
        assert conf.file == str(sample_chimera_config)

    def test_yamlconf_cfg_property(self, sample_chimera_config):
        """Test YamlConf cfg property."""
        conf = YamlConf(str(sample_chimera_config))
        cfg = conf.cfg
        
        assert isinstance(cfg, dict)
        assert "preprocessor" in cfg
        assert cfg["preprocessor"]["module_path"] == "chimera.precondition_functions"

    def test_yamlconf_get_existing_key(self, sample_chimera_config):
        """Test YamlConf get method with existing key."""
        conf = YamlConf(str(sample_chimera_config))
        
        preprocessor = conf.get("preprocessor")
        assert isinstance(preprocessor, dict)
        assert preprocessor["module_path"] == "chimera.precondition_functions"

    def test_yamlconf_get_nonexistent_key(self, sample_chimera_config):
        """Test YamlConf get method with non-existent key."""
        conf = YamlConf(str(sample_chimera_config))
        
        with pytest.raises(YamlConfError):
            conf.get("nonexistent_key")

    def test_yamlconf_get_nested_access(self, sample_chimera_config):
        """Test accessing nested configuration values."""
        conf = YamlConf(str(sample_chimera_config))
        
        nested = conf.get("nested")
        assert nested["key1"] == "value1"
        assert nested["key2"] == "value2"

    def test_yamlconf_repr(self, sample_chimera_config):
        """Test YamlConf string representation."""
        conf = YamlConf(str(sample_chimera_config))
        repr_str = repr(conf)
        
        # Should be valid JSON
        parsed = json.loads(repr_str)
        assert isinstance(parsed, dict)
        assert "preprocessor" in parsed

    @patch("builtins.open", mock_open(read_data="test_key: test_value\\n"))
    @patch("yaml.safe_load")
    def test_yamlconf_init_mocked(self, mock_yaml_load):
        """Test YamlConf initialization with mocked file operations."""
        mock_yaml_load.return_value = {"test_key": "test_value"}
        
        conf = YamlConf("mocked_file.yaml")
        
        assert conf.file == "mocked_file.yaml"
        assert conf.cfg == {"test_key": "test_value"}
        mock_yaml_load.assert_called_once()


class TestLoadConfig:
    """Test cases for load_config function."""

    def test_load_config_yaml_file(self, sample_chimera_config):
        """Test load_config with YAML file."""
        config = load_config(str(sample_chimera_config))
        
        assert isinstance(config, dict)
        assert "preprocessor" in config
        assert "job_submitter" in config

    def test_load_config_json_file(self, sample_pge_config):
        """Test load_config with JSON file."""
        config = load_config(str(sample_pge_config))
        
        assert isinstance(config, dict)
        assert "runconfig" in config
        assert "pge_name" in config
        assert config["pge_name"] == "Test_PGE"

    def test_load_config_nonexistent_file(self, nonexistent_file):
        """Test load_config with non-existent file."""
        with pytest.raises(RuntimeError, match="Could not load Config"):
            load_config(str(nonexistent_file))

    def test_load_config_invalid_extension(self, temp_dir):
        """Test load_config with unsupported file extension."""
        invalid_file = temp_dir / "config.txt"
        invalid_file.write_text("some content")
        
        with pytest.raises(RuntimeError, match="Config file must end in .yaml or .json"):
            load_config(str(invalid_file))

    def test_load_config_invalid_yaml(self, invalid_config):
        """Test load_config with invalid YAML file."""
        with pytest.raises(RuntimeError, match="Could not load Config"):
            load_config(str(invalid_config))

    def test_load_config_invalid_json(self, temp_dir):
        """Test load_config with invalid JSON file."""
        invalid_json = temp_dir / "invalid.json"
        invalid_json.write_text("{ invalid json content")
        
        with pytest.raises(RuntimeError, match="Could not load Config"):
            load_config(str(invalid_json))

    def test_load_config_json_preserves_order(self, temp_dir):
        """Test load_config preserves order for JSON files."""
        ordered_json = temp_dir / "ordered.json"
        json_content = '{"first": 1, "second": 2, "third": 3}'
        ordered_json.write_text(json_content)
        
        config = load_config(str(ordered_json))
        
        # Check that keys are in order (OrderedDict behavior)
        keys = list(config.keys())
        assert keys == ["first", "second", "third"]

    @patch("chimera.commons.conf_util.YamlConf")
    def test_load_config_yaml_uses_yamlconf(self, mock_yamlconf, temp_dir):
        """Test that load_config uses YamlConf for YAML files."""
        yaml_file = temp_dir / "test.yaml"
        yaml_file.write_text("test: value")
        
        mock_instance = mock_yamlconf.return_value
        mock_instance.cfg = {"test": "value"}
        
        config = load_config(str(yaml_file))
        
        mock_yamlconf.assert_called_once_with(str(yaml_file))
        assert config == {"test": "value"}


class TestYamlConfError:
    """Test cases for YamlConfError exception."""

    def test_yamlconf_error_inheritance(self):
        """Test that YamlConfError is properly defined."""
        error = YamlConfError()
        assert isinstance(error, Exception)

    def test_yamlconf_error_raised_by_get(self, sample_chimera_config):
        """Test that YamlConfError is raised by get method."""
        conf = YamlConf(str(sample_chimera_config))
        
        with pytest.raises(YamlConfError):
            conf.get("nonexistent_key")


class TestIntegration:
    """Integration tests for conf_util module."""

    def test_yamlconf_and_load_config_consistency(self, sample_chimera_config):
        """Test that YamlConf and load_config return consistent results."""
        # Load using YamlConf
        conf = YamlConf(str(sample_chimera_config))
        yamlconf_result = conf.cfg
        
        # Load using load_config
        load_config_result = load_config(str(sample_chimera_config))
        
        assert yamlconf_result == load_config_result

    def test_real_chimera_config_structure(self, sample_chimera_config):
        """Test loading real chimera config structure."""
        config = load_config(str(sample_chimera_config))
        
        # Verify expected structure
        assert "preprocessor" in config
        assert "job_submitter" in config
        
        # Check preprocessor structure
        preprocessor = config["preprocessor"]
        assert "module_path" in preprocessor
        assert "class_name" in preprocessor
        
        # Check job_submitter structure
        job_submitter = config["job_submitter"]
        assert "module_path" in job_submitter
        assert "class_name" in job_submitter