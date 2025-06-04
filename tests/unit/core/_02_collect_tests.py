import pytest
from src.core._02_collect import *
from src.data_models import *
from tests.fixtures.core._02_collect_fixtures import *

class TestCollect:
    """Test cases for _02_collect functions."""

    def test_process_new_items(self, process_new_items_cases):
        """Test all process_new_items scenarios from fixture."""
        for case in process_new_items_cases:
            result = process_new_items(case["input"])
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
                assert row["original_title"] == expected["original_title"], (
                    f"Failed for {case['description']}: "
                    f"expected original_title {expected['original_title']}, got {row['original_title']}"
                )
                assert row["media_type"] == expected["media_type"], (
                    f"Failed for {case['description']}: "
                    f"expected media_type {expected['media_type']}, got {row['media_type']}"
                )
                assert row["rejection_status"] == expected["rejection_status"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_status {expected['rejection_status']}, got {row['rejection_status']}"
                )
                assert row["error_condition"] == expected["error_condition"], (
                    f"Failed for {case['description']}: "
                    f"expected error_condition {expected['error_condition']}, got {row['error_condition']}"
                )

    def test_update_rejected_status(self, update_rejected_status_cases):
        """Test all update_rejected_status scenarios from fixture."""
        for case in update_rejected_status_cases:
            input_media = MediaDataFrame(case["input_data"])
            result = update_rejected_status(input_media)
            expected_list = case["expected_fields"]

            assert isinstance(result, MediaDataFrame)
            assert result.df.height == len(expected_list), f"Row count mismatch for {case['description']}"

            for i in range(result.df.height):
                row = result.df.row(i, named=True)
                expected = expected_list[i]  # Get the i-th expected item

                assert row["hash"] == expected["hash"], (
                    f"Failed for {case['description']}: "
                    f"expected hash {expected['hash']}, got {row['hash']}"
                )
                assert row["rejection_status"] == expected["rejection_status"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_status {expected['rejection_status']}, got {row['rejection_status']}"
                )
                assert row["rejection_reason"] == expected["rejection_reason"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_reason {expected['rejection_reason']}, got {row['rejection_reason']}"
                )