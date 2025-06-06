import pytest
from src.core._08_download_check import *
from src.data_models import *
from tests.fixtures.core._08_download_check_fixtures import *

class TestDownloadCheck:
    """Test cases for _08_download_check functions."""

    def test_confirm_downloading_status(self, confirm_downloading_status_cases):
        """Test all confirm_downloading_status scenarios from fixture."""
        for case in confirm_downloading_status_cases:
            input_media = MediaDataFrame(case["input_media_data"])
            current_media_items = case["current_media_items"]
            result = confirm_downloading_status(input_media, current_media_items)
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
                assert row["error_condition"] == expected["error_condition"], (
                    f"Failed for {case['description']}: "
                    f"expected error_condition={expected['error_condition']}, got {row['error_condition']}"
                )

    def test_extract_and_verify_filename(self, extract_and_verify_filename_cases):
        """Test all extract_and_verify_filename scenarios from fixture."""
        for case in extract_and_verify_filename_cases:
            input_media = MediaDataFrame(case["input_media_data"])
            downloaded_media_items = case["downloaded_media_items"]
            result = extract_and_verify_filename(input_media, downloaded_media_items)
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
                if "error_status" in expected:
                    assert row["error_status"] == expected["error_status"], (
                        f"Failed for {case['description']}: "
                        f"expected error_status={expected['error_status']}, got {row['error_status']}"
                    )
                if "error_condition" in expected:
                    assert row["error_condition"] == expected["error_condition"], (
                        f"Failed for {case['description']}: "
                        f"expected error_condition={expected['error_condition']}, got {row['error_condition']}"
                    )