#!/usr/bin/env python
"""
Unit tests for chimera.run_pge_docker module.
Tests the submit_pge_job() function in isolation using mocks.
"""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest

from chimera.pge_job_submitter import PgeJobSubmitter
from chimera.run_pge_docker import submit_pge_job


class TestRunPgeDocker:
    """Test cases for run_pge_docker.submit_pge_job() function."""

    def test_submit_pge_job_success(self):
        """Test successful job submission with valid inputs."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"
        wuid = "test_wuid"
        job_num = 123
        expected_job_json = {"job_id": "test_job_123", "status": "submitted"}

        mock_config = {
            "job_submitter": {
                "module_path": "test.module",
                "class_name": "TestJobSubmitter",
            }
        }

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf, patch(
            "chimera.run_pge_docker.import_module"
        ) as mock_import_module, patch(
            "chimera.run_pge_docker.getattr"
        ) as mock_getattr, patch(
            "chimera.run_pge_docker.issubclass"
        ) as mock_issubclass:

            # Setup mocks
            mock_yaml_conf.return_value.cfg = mock_config
            mock_module = Mock()
            mock_import_module.return_value = mock_module

            mock_cls = Mock()
            mock_getattr.return_value = mock_cls
            mock_issubclass.return_value = True

            mock_cls_instance = Mock()
            mock_cls.return_value = mock_cls_instance
            mock_cls_instance.submit_job.return_value = expected_job_json

            # Act
            result = submit_pge_job(
                sf_context,
                runconfig,
                pge_config_file,
                settings_file,
                chimera_config_file,
                wuid,
                job_num,
            )

            # Assert
            assert result == expected_job_json
            mock_yaml_conf.assert_called_once_with(chimera_config_file)
            mock_import_module.assert_called_once_with("test.module")
            mock_getattr.assert_called_once_with(mock_module, "TestJobSubmitter")
            mock_issubclass.assert_called_once_with(mock_cls, PgeJobSubmitter)
            mock_cls.assert_called_once_with(
                sf_context, runconfig, pge_config_file, settings_file, wuid, job_num
            )
            mock_cls_instance.submit_job.assert_called_once()

    def test_submit_pge_job_missing_module_path(self):
        """Test error when module_path is missing from config."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"

        mock_config = {"job_submitter": {}}

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf:
            mock_yaml_conf.return_value.cfg = mock_config

            # Act & Assert
            with pytest.raises(RuntimeError, match="'module_path' must be defined"):
                submit_pge_job(
                    sf_context,
                    runconfig,
                    pge_config_file,
                    settings_file,
                    chimera_config_file,
                )

    def test_submit_pge_job_missing_class_name(self):
        """Test error when class_name is missing from config."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"

        mock_config = {"job_submitter": {"module_path": "test.module"}}

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf:
            mock_yaml_conf.return_value.cfg = mock_config

            # Act & Assert
            with pytest.raises(RuntimeError, match="'class_name' must be defined"):
                submit_pge_job(
                    sf_context,
                    runconfig,
                    pge_config_file,
                    settings_file,
                    chimera_config_file,
                )

    def test_submit_pge_job_missing_job_submitter_section(self):
        """Test error when job_submitter section is missing from config."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"

        mock_config = {}

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf:
            mock_yaml_conf.return_value.cfg = mock_config

            # Act & Assert
            with pytest.raises(RuntimeError, match="'module_path' must be defined"):
                submit_pge_job(
                    sf_context,
                    runconfig,
                    pge_config_file,
                    settings_file,
                    chimera_config_file,
                )

    def test_submit_pge_job_module_import_error(self):
        """Test error when module cannot be imported."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"

        mock_config = {
            "job_submitter": {
                "module_path": "non.existent.module",
                "class_name": "TestJobSubmitter",
            }
        }

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf, patch(
            "chimera.run_pge_docker.import_module"
        ) as mock_import_module:

            mock_yaml_conf.return_value.cfg = mock_config
            mock_import_module.side_effect = ImportError("Module not found")

            # Act & Assert
            with pytest.raises(ImportError, match="Module not found"):
                submit_pge_job(
                    sf_context,
                    runconfig,
                    pge_config_file,
                    settings_file,
                    chimera_config_file,
                )

    def test_submit_pge_job_class_not_found(self):
        """Test error when class is not found in module."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"

        mock_config = {
            "job_submitter": {
                "module_path": "test.module",
                "class_name": "NonExistentClass",
            }
        }

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf, patch(
            "chimera.run_pge_docker.import_module"
        ) as mock_import_module, patch(
            "chimera.run_pge_docker.getattr"
        ) as mock_getattr:

            mock_yaml_conf.return_value.cfg = mock_config
            mock_module = Mock()
            mock_import_module.return_value = mock_module
            mock_getattr.side_effect = AttributeError("Class not found")

            # Act & Assert
            with pytest.raises(AttributeError, match="Class not found"):
                submit_pge_job(
                    sf_context,
                    runconfig,
                    pge_config_file,
                    settings_file,
                    chimera_config_file,
                )

    def test_submit_pge_job_invalid_subclass(self):
        """Test error when class is not a subclass of PgeJobSubmitter."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"

        mock_config = {
            "job_submitter": {
                "module_path": "test.module",
                "class_name": "InvalidJobSubmitter",
            }
        }

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf, patch(
            "chimera.run_pge_docker.import_module"
        ) as mock_import_module, patch(
            "chimera.run_pge_docker.getattr"
        ) as mock_getattr, patch(
            "chimera.run_pge_docker.issubclass"
        ) as mock_issubclass:

            mock_yaml_conf.return_value.cfg = mock_config
            mock_module = Mock()
            mock_import_module.return_value = mock_module

            mock_cls = Mock()
            mock_cls.__name__ = "InvalidJobSubmitter"
            mock_getattr.return_value = mock_cls
            mock_issubclass.return_value = False

            # Act & Assert
            with pytest.raises(
                RuntimeError, match="Class must be a subclass of PgeJobSubmitter"
            ):
                submit_pge_job(
                    sf_context,
                    runconfig,
                    pge_config_file,
                    settings_file,
                    chimera_config_file,
                )

    def test_submit_pge_job_without_optional_params(self):
        """Test successful job submission without optional wuid and job_num."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"
        expected_job_json = {"job_id": "test_job_no_wuid", "status": "submitted"}

        mock_config = {
            "job_submitter": {
                "module_path": "test.module",
                "class_name": "TestJobSubmitter",
            }
        }

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf, patch(
            "chimera.run_pge_docker.import_module"
        ) as mock_import_module, patch(
            "chimera.run_pge_docker.getattr"
        ) as mock_getattr, patch(
            "chimera.run_pge_docker.issubclass"
        ) as mock_issubclass:

            # Setup mocks
            mock_yaml_conf.return_value.cfg = mock_config
            mock_module = Mock()
            mock_import_module.return_value = mock_module

            mock_cls = Mock()
            mock_getattr.return_value = mock_cls
            mock_issubclass.return_value = True

            mock_cls_instance = Mock()
            mock_cls.return_value = mock_cls_instance
            mock_cls_instance.submit_job.return_value = expected_job_json

            # Act
            result = submit_pge_job(
                sf_context,
                runconfig,
                pge_config_file,
                settings_file,
                chimera_config_file,
            )

            # Assert
            assert result == expected_job_json
            mock_cls.assert_called_once_with(
                sf_context, runconfig, pge_config_file, settings_file, None, None
            )

    @patch("chimera.run_pge_docker.logger")
    def test_submit_pge_job_logging(self, mock_logger):
        """Test that appropriate logging messages are called."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"
        expected_job_json = {"job_id": "test_job_logging", "status": "submitted"}

        mock_config = {
            "job_submitter": {
                "module_path": "test.module",
                "class_name": "TestJobSubmitter",
            }
        }

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf, patch(
            "chimera.run_pge_docker.import_module"
        ) as mock_import_module, patch(
            "chimera.run_pge_docker.getattr"
        ) as mock_getattr, patch(
            "chimera.run_pge_docker.issubclass"
        ) as mock_issubclass:

            # Setup mocks
            mock_yaml_conf.return_value.cfg = mock_config
            mock_module = Mock()
            mock_import_module.return_value = mock_module

            mock_cls = Mock()
            mock_getattr.return_value = mock_cls
            mock_issubclass.return_value = True

            mock_cls_instance = Mock()
            mock_cls.return_value = mock_cls_instance
            mock_cls_instance.submit_job.return_value = expected_job_json

            # Act
            result = submit_pge_job(
                sf_context,
                runconfig,
                pge_config_file,
                settings_file,
                chimera_config_file,
            )

            # Assert
            assert result == expected_job_json
            mock_logger.info.assert_any_call("Starting run_pge_docker step.")
            mock_logger.info.assert_any_call("Finished run_pge_docker step.")

    def test_submit_pge_job_instance_submit_job_error(self):
        """Test error when submit_job() method raises an exception."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/chimera_config.yaml"

        mock_config = {
            "job_submitter": {
                "module_path": "test.module",
                "class_name": "TestJobSubmitter",
            }
        }

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf, patch(
            "chimera.run_pge_docker.import_module"
        ) as mock_import_module, patch(
            "chimera.run_pge_docker.getattr"
        ) as mock_getattr, patch(
            "chimera.run_pge_docker.issubclass"
        ) as mock_issubclass:

            # Setup mocks
            mock_yaml_conf.return_value.cfg = mock_config
            mock_module = Mock()
            mock_import_module.return_value = mock_module

            mock_cls = Mock()
            mock_getattr.return_value = mock_cls
            mock_issubclass.return_value = True

            mock_cls_instance = Mock()
            mock_cls.return_value = mock_cls_instance
            mock_cls_instance.submit_job.side_effect = RuntimeError(
                "Job submission failed"
            )

            # Act & Assert
            with pytest.raises(RuntimeError, match="Job submission failed"):
                submit_pge_job(
                    sf_context,
                    runconfig,
                    pge_config_file,
                    settings_file,
                    chimera_config_file,
                )

    def test_submit_pge_job_yaml_config_error(self):
        """Test error when YamlConf fails to load configuration."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        runconfig = {"config": "test_config"}
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        chimera_config_file = "/path/to/invalid_config.yaml"

        with patch("chimera.run_pge_docker.YamlConf") as mock_yaml_conf:
            mock_yaml_conf.side_effect = Exception("Failed to load YAML config")

            # Act & Assert
            with pytest.raises(Exception, match="Failed to load YAML config"):
                submit_pge_job(
                    sf_context,
                    runconfig,
                    pge_config_file,
                    settings_file,
                    chimera_config_file,
                )
