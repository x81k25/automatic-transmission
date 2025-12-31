import pytest
import polars as pl
from unittest.mock import patch
from src.core._08_download_check import *
from src.data_models import *
from tests.fixtures.core._08_download_check_fixtures import *

class TestDownloadCheck:
    """Test cases for _08_download_check functions."""

    def test_confirm_downloading_status(self, confirm_downloading_status_cases):
        """Test all confirm_downloading_status scenarios from fixture."""
        for case in confirm_downloading_status_cases:
            input_media = pl.DataFrame(case["input_media_data"])
            current_media_items = case["current_media_items"]
            result = confirm_downloading_status(input_media, current_media_items)
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


    def test_extract_and_verify_filename(self, extract_and_verify_filename_cases):
        """Test all extract_and_verify_filename scenarios from fixture."""
        for case in extract_and_verify_filename_cases:
            input_media = pl.DataFrame(case["input_media_data"])
            downloaded_media_items = case["downloaded_media_items"]
            result = extract_and_verify_filename(input_media, downloaded_media_items)
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
                assert row["original_path"] == expected["original_path"], (
                    f"Failed for {case['description']}: "
                    f"expected original_path={expected['original_path']}, got {row['original_path']}"
                )
                assert row["error_condition"] == expected["error_condition"], (
                    f"Failed for {case['description']}: "
                    f"expected error_condition={expected['error_condition']}, got {row['error_condition']}"
                )


    def test_update_status(self, update_status_cases):
        """Test all update_status scenarios from fixture."""
        for case in update_status_cases:
            input_media = pl.DataFrame(case["input_data"])
            result = update_status(input_media)
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
                assert row["pipeline_status"] == expected["pipeline_status"], (
                    f"Failed for {case['description']}: "
                    f"expected pipeline_status={expected['pipeline_status']}, got {row['pipeline_status']}"
                )

                # Check additional fields for re-ingestion cases
                if "rejection_status" in expected:
                    assert row["rejection_status"] == expected["rejection_status"], (
                        f"Failed for {case['description']}: "
                        f"expected rejection_status={expected['rejection_status']}, got {row['rejection_status']}"
                    )
                if "rejection_reason" in expected:
                    assert row["rejection_reason"] == expected["rejection_reason"], (
                        f"Failed for {case['description']}: "
                        f"expected rejection_reason={expected['rejection_reason']}, got {row['rejection_reason']}"
                    )
                if "error_condition" in expected:
                    assert row["error_condition"] == expected["error_condition"], (
                        f"Failed for {case['description']}: "
                        f"expected error_condition={expected['error_condition']}, got {row['error_condition']}"
                    )


    @patch('src.core._08_download_check.utils.media_db_update')
    @patch('src.core._08_download_check.utils.return_current_media_items')
    @patch('src.core._08_download_check.utils.get_media_by_hash')
    @patch('src.core._08_download_check.utils.get_media_from_db')
    def test_check_downloads_workflow_integration(self, mock_get_media,
                                                  mock_get_media_by_hash,
                                                  mock_return_current,
                                                  mock_db_update,
                                                  check_downloads_workflow_scenarios):
        """Test check_downloads workflow integration scenarios from fixture."""
        for case in check_downloads_workflow_scenarios:
            # Reset mocks for each test case
            mock_get_media.reset_mock()
            mock_get_media_by_hash.reset_mock()
            mock_return_current.reset_mock()
            mock_db_update.reset_mock()

            # Setup input mocks - function now calls get_media_from_db twice
            def mock_get_media_side_effect(pipeline_status):
                if pipeline_status == 'downloading':
                    if case["input_downloading_media"] is None:
                        return None
                    else:
                        df = pl.DataFrame(case["input_downloading_media"])
                        # Add original_title if missing
                        if 'original_title' not in df.columns:
                            df = df.with_columns(
                                pl.col('media_title').alias('original_title')
                            )
                        return df
                elif pipeline_status == 'transferred':
                    # For most tests, assume no transferred items unless specified
                    return case.get("input_transferred_media", None)
                return None

            mock_get_media.side_effect = mock_get_media_side_effect

            # Mock get_media_by_hash for orphaned items scenario
            if case["input_downloading_media"] is None and case["input_transmission_items"] is not None:
                # Create fake media items for the orphaned hashes from transmission
                orphaned_items = []
                for hash_key in case["input_transmission_items"].keys():
                    orphaned_items.append({
                        "hash": hash_key,
                        "media_type": "movie",
                        "media_title": "Orphaned Item",
                        "original_title": "Orphaned Item",
                        "pipeline_status": "downloading",
                        "rejection_status": "accepted",
                        "error_status": False
                    })
                mock_get_media_by_hash.return_value = pl.DataFrame(orphaned_items)
            else:
                mock_get_media_by_hash.return_value = None

            mock_return_current.return_value = case["input_transmission_items"]

            # Execute the function
            check_downloads()

            # Verify number of database update calls
            assert mock_db_update.call_count == case["expected_db_update_calls"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected_db_update_calls']} db update calls, got {mock_db_update.call_count}"
            )

            # If we expect outputs, verify the DataFrame contents
            if "expected_outputs" in case and case["expected_outputs"]:
                expected_list = case["expected_outputs"]

                # Get the DataFrame that was passed to the database update
                call_args = mock_db_update.call_args_list[-1]  # Get the last call
                actual_media = call_args.kwargs['media']  # Always use kwargs

                assert isinstance(actual_media, pl.DataFrame)
                assert actual_media.height == len(expected_list), (
                    f"Failed for {case['description']}: "
                    f"expected {len(expected_list)} items in output, got {actual_media.height}"
                )

                for i in range(actual_media.height):
                    row = actual_media.row(i, named=True)
                    expected = expected_list[i]

                    # Check all expected fields
                    for field, expected_value in expected.items():
                        actual_value = row.get(field)
                        assert actual_value == expected_value, (
                            f"Failed for {case['description']}: "
                            f"expected {field}={expected_value}, got {actual_value}"
                        )
