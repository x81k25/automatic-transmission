import pytest
import yaml
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.core._03_parse import *
from src.data_models import *
from tests.fixtures.core._03_parse_fixtures import *

class TestParseMediaItems:
    """Test cases for _03_parse functions."""

    def test_parse_media_items(self, parse_media_items_cases):
        """Test all parse_media_items scenarios from fixture."""
        for case in parse_media_items_cases:
            input_media = MediaDataFrame(case["input_data"])
            result = parse_media_items(input_media)
            expected_list = case["expected_fields"]

            assert isinstance(result, pl.DataFrame)
            assert result.height == len(expected_list), f"Row count mismatch for {case['description']}"

            for i in range(result.height):
                row = result.row(i, named=True)
                expected = expected_list[i]

                # Check all expected fields
                for field, expected_value in expected.items():
                    actual_value = row.get(field)
                    assert actual_value == expected_value, (
                        f"Failed for {case['description']}: "
                        f"expected {field}={expected_value}, got {actual_value}"
                    )


    def test_validate_parsed_media(self, validate_parsed_media_cases):
        """Test all validate_parsed_media scenarios from fixture."""
        for case in validate_parsed_media_cases:
            input_media = MediaDataFrame(case["input_data"])
            result = validate_parsed_media(input_media)
            expected_list = case["expected_fields"]

            assert isinstance(result, pl.DataFrame)
            assert result.height == len(expected_list), f"Row count mismatch for {case['description']}"

            for i in range(result.height):
                row = result.row(i, named=True)
                expected = expected_list[i]

                assert row["hash"] == expected["hash"], (
                    f"Failed for {case['description']}: "
                    f"expected hash {expected['hash']}, got {row['hash']}"
                )
                assert row["error_condition"] == expected["error_condition"], (
                    f"Failed for {case['description']}: "
                    f"expected error_condition={expected['error_condition']}, got {row['error_condition']}"
                )


    def test_update_status(self, update_status_cases):
        """Test all update_status scenarios from fixture."""
        for case in update_status_cases:
            input_media = MediaDataFrame(case["input_data"])
            result = update_status(input_media)
            expected_list = case["expected_fields"]

            assert isinstance(result, MediaDataFrame)
            assert result.df.height == len(expected_list), f"Row count mismatch for {case['description']}"

            for i in range(result.df.height):
                row = result.df.row(i, named=True)
                expected = expected_list[i]

                assert row["hash"] == expected["hash"], (
                    f"Failed for {case['description']}: "
                    f"expected hash {expected['hash']}, got {row['hash']}"
                )
                assert row["pipeline_status"] == expected["pipeline_status"], (
                    f"Failed for {case['description']}: "
                    f"expected pipeline_status={expected['pipeline_status']}, got {row['pipeline_status']}"
                )


    @patch('src.core._03_parse.utils.media_db_update')
    @patch('src.core._03_parse.utils.get_media_from_db')
    def test_parse_media_workflow_integration(self, mock_get_media, mock_db_update,
                                            parse_media_workflow_cases):
        """Test parse_media workflow integration scenarios from fixture."""
        for case in parse_media_workflow_cases:
            # Reset mocks for each test case
            mock_get_media.reset_mock()
            mock_db_update.reset_mock()

            # Setup input mocks
            if case["input_media"] is None:
                mock_get_media.return_value = None
            else:
                mock_get_media.return_value = MediaDataFrame(case["input_media"])

            # Execute the function
            parse_media()

            # Verify database calls
            expected_db_calls = case.get("expected_db_update_calls", 0)
            assert mock_db_update.call_count == expected_db_calls, (
                f"Failed for {case['description']}: "
                f"expected {expected_db_calls} db update calls, got {mock_db_update.call_count}"
            )

            # Verify the content of database updates if any
            if "expected_output" in case and case["expected_output"]:
                call_args = mock_db_update.call_args
                actual_media = call_args.kwargs['media']
                
                assert isinstance(actual_media, MediaDataFrame)
                assert actual_media.df.height == len(case["expected_output"]), (
                    f"Failed for {case['description']}: "
                    f"expected {len(case['expected_output'])} items in output, "
                    f"got {actual_media.df.height}"
                )
                
                # Sort both actual and expected by hash for consistent comparison
                actual_sorted = actual_media.df.sort("hash")
                expected_sorted = sorted(case["expected_output"], 
                                       key=lambda x: x["hash"])
                
                for i, expected in enumerate(expected_sorted):
                    row = actual_sorted.row(i, named=True)
                    for field, expected_value in expected.items():
                        actual_value = row.get(field)
                        assert actual_value == expected_value, (
                            f"Failed for {case['description']} item {i}: "
                            f"expected {field}={expected_value}, got {actual_value}"
                        )