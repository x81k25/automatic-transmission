import pytest
from unittest.mock import patch
import polars as pl
from src.core._04_file_filtration import *
from src.data_models import *
from tests.fixtures.core._04_file_filtration_fixtures import *

class TestFileFilteration:
    """Test cases for _04_file_filtration functions."""

    def test_filter_by_file_metadata(self, filter_by_file_metadata_cases):
        """Test all filter_by_file_metadata scenarios from fixture."""
        for case in filter_by_file_metadata_cases:
            input_item = case["input"].copy()
            result = filter_by_file_metadata(input_item)
            expected = case["expected"]

            assert result["hash"] == expected["hash"], (
                f"Failed for {case['description']}: "
                f"expected hash {expected['hash']}, got {result['hash']}"
            )
            assert result["rejection_reason"] == expected["rejection_reason"], (
                f"Failed for {case['description']}: "
                f"expected rejection_reason={expected['rejection_reason']}, got {result['rejection_reason']}"
            )

    def test_update_status(self, update_status_cases):
        """Test all update_status scenarios from fixture."""
        for case in update_status_cases:
            input_media = pl.DataFrame(case["input_data"])
            result = update_status(input_media)
            expected_list = case["expected_fields"]

            assert result.height == len(expected_list), f"Row count mismatch for {case['description']}"

            for i in range(result.height):
                row = result.row(i, named=True)
                expected = expected_list[i]

                assert row["hash"] == expected["hash"], (
                    f"Failed for {case['description']}: "
                    f"expected hash {expected['hash']}, got {row['hash']}"
                )
                assert row["rejection_status"] == expected["rejection_status"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_status={expected['rejection_status']}, got {row['rejection_status']}"
                )
                assert row["pipeline_status"] == expected["pipeline_status"], (
                    f"Failed for {case['description']}: "
                    f"expected pipeline_status={expected['pipeline_status']}, got {row['pipeline_status']}"
                )

    @patch('src.core._04_file_filtration.utils.media_db_update')
    @patch('src.core._04_file_filtration.utils.get_media_from_db')
    def test_filter_files_workflow_integration(self, mock_get_media, mock_db_update, filter_files_workflow_cases):
        """Test filter_files workflow integration scenarios from fixture."""
        for case in filter_files_workflow_cases:
            # Reset mocks for each test case
            mock_get_media.reset_mock()
            mock_db_update.reset_mock()

            # Setup input mocks
            if case["input_media"] is None:
                mock_get_media.return_value = None
            else:
                mock_get_media.return_value = pl.DataFrame(case["input_media"])

            # Execute the function
            filter_files()

            # Verify database calls
            expected_db_calls = case.get("expected_db_update_calls", 0)
            assert mock_db_update.call_count == expected_db_calls, (
                f"Failed for {case['description']}: "
                f"expected {expected_db_calls} db update calls, got {mock_db_update.call_count}"
            )

            # Verify the content of database updates if any
            if "expected_outputs" in case and case["expected_outputs"]:
                call_args = mock_db_update.call_args
                actual_media = call_args.kwargs['media']

                assert isinstance(actual_media, pl.DataFrame)
                assert actual_media.height == len(case["expected_outputs"]), (
                    f"Failed for {case['description']}: "
                    f"expected {len(case['expected_outputs'])} items in output, "
                    f"got {actual_media.height}"
                )

                # Sort both actual and expected by hash for consistent comparison
                actual_sorted = actual_media.sort("hash")
                expected_sorted = sorted(case["expected_outputs"],
                                       key=lambda x: x["hash"])

                for i, expected in enumerate(expected_sorted):
                    row = actual_sorted.row(i, named=True)
                    for field, expected_value in expected.items():
                        actual_value = row.get(field)
                        assert actual_value == expected_value, (
                            f"Failed for {case['description']} item {i}: "
                            f"expected {field}={expected_value}, got {actual_value}"
                        )