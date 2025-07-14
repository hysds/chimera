#!/usr/bin/env python
"""
Unit tests for chimera.post_processor module.
Tests the post_process() function in isolation using mocks.
"""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest

from chimera.post_processor import post_process


class TestPostProcessor:
    """Test cases for post_processor.post_process() function."""

    def test_post_process_success(self):
        """Test successful post-processing with valid inputs."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {"job_id": "test_job_123", "status": "completed"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        test_mode = False
        expected_output = {"context": "processed_data", "job_status": 2}

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
                test_mode,
            )

            # Assert
            assert result == expected_output
            mock_post_processor_class.assert_called_once_with(
                sf_context,
                chimera_config_file,
                pge_config_file,
                settings_file,
                job_result,
            )
            mock_post_processor.process.assert_called_once()

    def test_post_process_test_mode_true(self):
        """Test post-processing with test_mode=True."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {"job_id": "test_job_123", "status": "completed"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        test_mode = True
        expected_output = {"context": "test_mode_data", "job_status": 2}

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
                test_mode,
            )

            # Assert
            assert result == expected_output
            mock_post_processor_class.assert_called_once_with(
                sf_context,
                chimera_config_file,
                pge_config_file,
                settings_file,
                job_result,
            )

    def test_post_process_with_empty_job_result(self):
        """Test post-processing with empty job_result."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {"context": "empty_job_result", "job_status": 0}

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
            )

            # Assert
            assert result == expected_output
            mock_post_processor_class.assert_called_once_with(
                sf_context,
                chimera_config_file,
                pge_config_file,
                settings_file,
                job_result,
            )

    def test_post_process_with_failed_job_status(self):
        """Test post-processing with failed job status."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {"job_id": "test_job_failed", "status": "failed"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {"context": "failed_job_data", "job_status": -1}

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
            )

            # Assert
            assert result == expected_output

    def test_post_process_with_none_values(self):
        """Test post-processing with None values."""
        # Arrange
        sf_context = None
        job_result = None
        chimera_config_file = None
        pge_config_file = None
        settings_file = None
        expected_output = {"error": "invalid_input"}

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
            )

            # Assert
            assert result == expected_output
            mock_post_processor_class.assert_called_once_with(
                sf_context,
                chimera_config_file,
                pge_config_file,
                settings_file,
                job_result,
            )

    def test_post_process_postprocessor_constructor_exception(self):
        """Test post_process function when PostProcessor constructor raises an exception."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {"job_id": "test_job_123", "status": "completed"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor_class.side_effect = ValueError("Invalid configuration")

            # Act & Assert
            with pytest.raises(ValueError, match="Invalid configuration"):
                post_process(
                    sf_context,
                    job_result,
                    chimera_config_file,
                    pge_config_file,
                    settings_file,
                )

    def test_post_process_process_method_exception(self):
        """Test post_process function when process() method raises an exception."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {"job_id": "test_job_123", "status": "completed"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.side_effect = RuntimeError("Processing failed")
            mock_post_processor_class.return_value = mock_post_processor

            # Act & Assert
            with pytest.raises(RuntimeError, match="Processing failed"):
                post_process(
                    sf_context,
                    job_result,
                    chimera_config_file,
                    pge_config_file,
                    settings_file,
                )

    @patch("chimera.post_processor.logger")
    def test_post_process_logging(self, mock_logger):
        """Test that appropriate logging messages are called."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {"job_id": "test_job_123", "status": "completed"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {"context": "processed_data"}

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
            )

            # Assert
            assert result == expected_output
            mock_logger.info.assert_any_call("Starting post_preprocessor step.")
            mock_logger.info.assert_any_call("Finished post_processor step.")

    def test_post_process_return_type_validation(self):
        """Test that post_process function returns the correct type."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {"job_id": "test_job_123", "status": "completed"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {
            "context": "processed_data",
            "job_status": 2,
            "metadata": {"created": "2023-01-01"},
            "products": ["product1.txt", "product2.txt"],
        }

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
            )

            # Assert
            assert isinstance(result, dict)
            assert result == expected_output
            assert "context" in result
            assert "job_status" in result
            assert "metadata" in result
            assert "products" in result

    def test_post_process_with_complex_job_result(self):
        """Test post-processing with complex job_result structure."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {
            "job_id": "complex_job_123",
            "status": "completed",
            "metadata": {
                "start_time": "2023-01-01T00:00:00Z",
                "end_time": "2023-01-01T01:00:00Z",
                "worker_node": "worker-01",
            },
            "outputs": [
                {"type": "product", "url": "s3://bucket/product1.tif"},
                {"type": "metadata", "url": "s3://bucket/metadata1.json"},
            ],
        }
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        expected_output = {"context": "complex_processed_data", "job_status": 2}

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
            )

            # Assert
            assert result == expected_output
            mock_post_processor_class.assert_called_once_with(
                sf_context,
                chimera_config_file,
                pge_config_file,
                settings_file,
                job_result,
            )

    def test_post_process_default_test_mode_false(self):
        """Test that test_mode defaults to False when not provided."""
        # Arrange
        sf_context = {"workflow": "test_workflow"}
        job_result = {"job_id": "test_job_123", "status": "completed"}
        chimera_config_file = "/path/to/chimera_config.yaml"
        pge_config_file = "/path/to/pge_config.json"
        settings_file = "/path/to/settings.yaml"
        # Note: not passing test_mode parameter
        expected_output = {"context": "default_mode_data"}

        with patch("chimera.post_processor.PostProcessor") as mock_post_processor_class:
            mock_post_processor = Mock()
            mock_post_processor.process.return_value = expected_output
            mock_post_processor_class.return_value = mock_post_processor

            # Act
            result = post_process(
                sf_context,
                job_result,
                chimera_config_file,
                pge_config_file,
                settings_file,
            )

            # Assert
            assert result == expected_output
            mock_post_processor_class.assert_called_once_with(
                sf_context,
                chimera_config_file,
                pge_config_file,
                settings_file,
                job_result,
            )

    def test_post_process_with_job_status_codes(self):
        """Test post-processing handles different job status codes correctly."""
        # Test data for different job status codes as documented in the docstring
        test_cases = [
            (
                {"job_id": "deduped_failed", "status": "deduped_failed"},
                {"job_status": -3},
            ),
            (
                {"job_id": "deduped_completed", "status": "deduped_completed"},
                {"job_status": -2},
            ),
            ({"job_id": "failed", "status": "failed"}, {"job_status": -1}),
            ({"job_id": "never_ran", "status": "never_ran"}, {"job_status": 0}),
            ({"job_id": "running", "status": "running"}, {"job_status": 1}),
            ({"job_id": "completed", "status": "completed"}, {"job_status": 2}),
        ]

        for job_result, expected_partial_output in test_cases:
            with patch(
                "chimera.post_processor.PostProcessor"
            ) as mock_post_processor_class:
                sf_context = {"workflow": "test_workflow"}
                chimera_config_file = "/path/to/chimera_config.yaml"
                pge_config_file = "/path/to/pge_config.json"
                settings_file = "/path/to/settings.yaml"

                expected_output = {"context": "status_test_data"}
                expected_output.update(expected_partial_output)

                mock_post_processor = Mock()
                mock_post_processor.process.return_value = expected_output
                mock_post_processor_class.return_value = mock_post_processor

                # Act
                result = post_process(
                    sf_context,
                    job_result,
                    chimera_config_file,
                    pge_config_file,
                    settings_file,
                )

                # Assert
                assert "job_status" in expected_output
                assert result == expected_output
