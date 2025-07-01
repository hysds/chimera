#!/usr/bin/env python
"""
Unit tests for chimera.input_preprocessor module.
Tests the process() function in isolation using mocks.
"""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest

from chimera.input_preprocessor import process


class TestInputPreprocessor:
    """Test cases for input_preprocessor.process() function."""

    def test_process_success(self):
        """Test successful processing with valid inputs."""
        # Arrange
        sf_context = {"input": "test_data"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_filepath = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {"processed": "data", "status": "success"}

        with patch(
            "chimera.input_preprocessor.PreConditionEvaluator"
        ) as mock_evaluator_class:
            mock_evaluator = Mock()
            mock_evaluator.evaluate.return_value = expected_output
            mock_evaluator_class.return_value = mock_evaluator

            # Act
            result = process(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )

            # Assert
            assert result == expected_output
            mock_evaluator_class.assert_called_once_with(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )
            mock_evaluator.evaluate.assert_called_once()

    def test_process_with_empty_sf_context(self):
        """Test process function with empty sf_context."""
        # Arrange
        sf_context = {}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_filepath = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {"processed": "empty_context"}

        with patch(
            "chimera.input_preprocessor.PreConditionEvaluator"
        ) as mock_evaluator_class:
            mock_evaluator = Mock()
            mock_evaluator.evaluate.return_value = expected_output
            mock_evaluator_class.return_value = mock_evaluator

            # Act
            result = process(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )

            # Assert
            assert result == expected_output
            mock_evaluator_class.assert_called_once_with(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )

    def test_process_with_none_values(self):
        """Test process function with None values."""
        # Arrange
        sf_context = None
        chimera_config_file = None
        pge_config_filepath = None
        settings_file = None
        expected_output = {"error": "invalid_input"}

        with patch(
            "chimera.input_preprocessor.PreConditionEvaluator"
        ) as mock_evaluator_class:
            mock_evaluator = Mock()
            mock_evaluator.evaluate.return_value = expected_output
            mock_evaluator_class.return_value = mock_evaluator

            # Act
            result = process(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )

            # Assert
            assert result == expected_output
            mock_evaluator_class.assert_called_once_with(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )

    def test_process_precondition_evaluator_exception(self):
        """Test process function when PreConditionEvaluator raises an exception."""
        # Arrange
        sf_context = {"input": "test_data"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_filepath = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"

        with patch(
            "chimera.input_preprocessor.PreConditionEvaluator"
        ) as mock_evaluator_class:
            mock_evaluator_class.side_effect = ValueError("Invalid configuration")

            # Act & Assert
            with pytest.raises(ValueError, match="Invalid configuration"):
                process(
                    sf_context, chimera_config_file, pge_config_filepath, settings_file
                )

    def test_process_evaluate_method_exception(self):
        """Test process function when evaluate() method raises an exception."""
        # Arrange
        sf_context = {"input": "test_data"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_filepath = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"

        with patch(
            "chimera.input_preprocessor.PreConditionEvaluator"
        ) as mock_evaluator_class:
            mock_evaluator = Mock()
            mock_evaluator.evaluate.side_effect = RuntimeError("Evaluation failed")
            mock_evaluator_class.return_value = mock_evaluator

            # Act & Assert
            with pytest.raises(RuntimeError, match="Evaluation failed"):
                process(
                    sf_context, chimera_config_file, pge_config_filepath, settings_file
                )

    @patch("chimera.input_preprocessor.logger")
    def test_process_logging(self, mock_logger):
        """Test that appropriate logging messages are called."""
        # Arrange
        sf_context = {"input": "test_data"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_filepath = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {"processed": "data"}

        with patch(
            "chimera.input_preprocessor.PreConditionEvaluator"
        ) as mock_evaluator_class:
            mock_evaluator = Mock()
            mock_evaluator.evaluate.return_value = expected_output
            mock_evaluator_class.return_value = mock_evaluator

            # Act
            result = process(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )

            # Assert
            assert result == expected_output
            mock_logger.info.assert_any_call("Starting input_preprocessor step.")
            mock_logger.info.assert_any_call("Finished input_preprocessor step.")

    def test_process_return_type_validation(self):
        """Test that process function returns the correct type."""
        # Arrange
        sf_context = {"input": "test_data"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_filepath = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {
            "processed": "data",
            "status": "success",
            "timestamp": "2023-01-01",
        }

        with patch(
            "chimera.input_preprocessor.PreConditionEvaluator"
        ) as mock_evaluator_class:
            mock_evaluator = Mock()
            mock_evaluator.evaluate.return_value = expected_output
            mock_evaluator_class.return_value = mock_evaluator

            # Act
            result = process(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )

            # Assert
            assert isinstance(result, dict)
            assert result == expected_output
            assert "processed" in result
            assert "status" in result
            assert "timestamp" in result

    def test_process_with_complex_sf_context(self):
        """Test process function with complex sf_context structure."""
        # Arrange
        sf_context = {
            "job_specification": {"id": "test_job", "version": "1.0"},
            "context": {
                "input_files": ["file1.txt", "file2.txt"],
                "parameters": {"param1": "value1", "param2": 42},
            },
        }
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_filepath = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {"processed_complex": "data"}

        with patch(
            "chimera.input_preprocessor.PreConditionEvaluator"
        ) as mock_evaluator_class:
            mock_evaluator = Mock()
            mock_evaluator.evaluate.return_value = expected_output
            mock_evaluator_class.return_value = mock_evaluator

            # Act
            result = process(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )

            # Assert
            assert result == expected_output
            mock_evaluator_class.assert_called_once_with(
                sf_context, chimera_config_file, pge_config_filepath, settings_file
            )
