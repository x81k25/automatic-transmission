import pytest
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
            input_media = MediaDataFrame(case["input_data"])
            result = update_status(input_media)
            expected_list = case["expected_fields"]

            assert result.df.height == len(expected_list), f"Row count mismatch for {case['description']}"

            for i in range(result.df.height):
                row = result.df.row(i, named=True)
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