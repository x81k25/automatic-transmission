import pytest
from unittest.mock import patch, MagicMock
import polars as pl
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

            assert isinstance(result, pl.DataFrame)
            assert result.height == len(expected_list), f"Row count mismatch for {case['description']}"

            for i in range(result.height):
                row = result.row(i, named=True)
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
            input_media = pl.DataFrame(case["input_data"])
            result = update_rejected_status(input_media)
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
                assert row["rejection_status"] == expected["rejection_status"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_status {expected['rejection_status']}, got {row['rejection_status']}"
                )
                assert row["rejection_reason"] == expected["rejection_reason"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_reason {expected['rejection_reason']}, got {row['rejection_reason']}"
                )


    @patch('src.core._02_collect.utils.media_db_update')
    @patch('src.core._02_collect.utils.insert_items_to_db')
    @patch('src.core._02_collect.utils.get_media_by_hash')
    @patch('src.core._02_collect.utils.return_rejected_hashes')
    @patch('src.core._02_collect.utils.compare_hashes_to_db')
    @patch('src.core._02_collect.utils.return_current_media_items')
    def test_collect_media_workflow_integration(self, mock_return_current,
                                               mock_compare_hashes,
                                               mock_return_rejected,
                                               mock_get_media_by_hash,
                                               mock_insert_db,
                                               mock_update_db,
                                               collect_media_workflow_cases):
        """Test collect_media workflow integration scenarios from fixture."""
        for case in collect_media_workflow_cases:
            # Reset mocks for each test case
            mock_return_current.reset_mock()
            mock_compare_hashes.reset_mock()
            mock_return_rejected.reset_mock()
            mock_get_media_by_hash.reset_mock()
            mock_insert_db.reset_mock()
            mock_update_db.reset_mock()

            # Setup input mocks
            mock_return_current.return_value = case["current_transmission_items"]

            if "new_hashes" in case:
                mock_compare_hashes.return_value = case["new_hashes"]

            if "rejected_hashes" in case:
                mock_return_rejected.return_value = case["rejected_hashes"]

            if "rejected_media" in case:
                mock_get_media_by_hash.return_value = pl.DataFrame(case["rejected_media"])

            # Execute the function
            collect_media()

            # Verify insert calls
            expected_insert_calls = case.get("expected_insert_calls", 0)
            assert mock_insert_db.call_count == expected_insert_calls, (
                f"Failed for {case['description']}: "
                f"expected {expected_insert_calls} insert calls, got {mock_insert_db.call_count}"
            )

            # Verify update calls
            expected_update_calls = case.get("expected_update_calls", 0)
            assert mock_update_db.call_count == expected_update_calls, (
                f"Failed for {case['description']}: "
                f"expected {expected_update_calls} update calls, got {mock_update_db.call_count}"
            )

            # Verify inserted items if any
            if "expected_insert_items" in case and case["expected_insert_items"]:
                call_args = mock_insert_db.call_args
                actual_media = call_args.kwargs['media']

                assert isinstance(actual_media, pl.DataFrame)
                assert actual_media.height == len(case["expected_insert_items"]), (
                    f"Failed for {case['description']}: "
                    f"expected {len(case['expected_insert_items'])} items to insert, "
                    f"got {actual_media.height}"
                )

                # Sort both actual and expected by hash for consistent comparison
                actual_sorted = actual_media.sort("hash")
                expected_sorted = sorted(case["expected_insert_items"],
                                       key=lambda x: x["hash"])

                for i, expected in enumerate(expected_sorted):
                    row = actual_sorted.row(i, named=True)
                    for field, expected_value in expected.items():
                        assert row.get(field) == expected_value, (
                            f"Failed for {case['description']} item {i}: "
                            f"expected {field}={expected_value}, got {row.get(field)}"
                        )

            # Verify updated items if any
            if "expected_update_items" in case and case["expected_update_items"]:
                call_args = mock_update_db.call_args
                actual_media = call_args.kwargs['media']

                assert isinstance(actual_media, pl.DataFrame)
                assert actual_media.height == len(case["expected_update_items"]), (
                    f"Failed for {case['description']}: "
                    f"expected {len(case['expected_update_items'])} items to update, "
                    f"got {actual_media.height}"
                )

                # Sort both actual and expected by hash for consistent comparison
                actual_sorted = actual_media.sort("hash")
                expected_sorted = sorted(case["expected_update_items"],
                                       key=lambda x: x["hash"])

                for i, expected in enumerate(expected_sorted):
                    row = actual_sorted.row(i, named=True)
                    for field, expected_value in expected.items():
                        assert row.get(field) == expected_value, (
                            f"Failed for {case['description']} item {i}: "
                            f"expected {field}={expected_value}, got {row.get(field)}"
                        )
