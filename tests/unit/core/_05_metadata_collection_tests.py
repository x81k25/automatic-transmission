import pytest
import polars as pl
from src.core._05_metadata_collection import *
from src.data_models import *
from tests.fixtures.core._05_metadata_collection_fixtures import *

class TestMetadataCollection:
    """Test cases for _05_metadata_collection functions."""

    def test_process_media_with_existing_metadata(self, process_media_with_existing_metadata_cases):
        """Test all process_media_with_existing_metadata scenarios from fixture."""
        for case in process_media_with_existing_metadata_cases:
            input_media = pl.DataFrame(case["input_media_data"])
            existing_metadata = pl.DataFrame(case["existing_metadata_data"])
            result = process_media_with_existing_metadata(input_media, existing_metadata)
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


    def test_build_training_records(self, build_training_records_cases):
        """Test all build_training_records scenarios from fixture."""
        for case in build_training_records_cases:
            input_media = pl.DataFrame(case["input_data"]) if case["input_data"] else pl.DataFrame()
            result = build_training_records(input_media)
            expected_count = case["expected_count"]
            expected_list = case["expected_fields"]

            assert isinstance(result, pl.DataFrame), f"Result should be DataFrame for {case['description']}"
            assert result.height == expected_count, (
                f"Row count mismatch for {case['description']}: "
                f"expected {expected_count}, got {result.height}"
            )

            if expected_count > 0:
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

                # Verify training-specific defaults are set
                for row in result.iter_rows(named=True):
                    assert row.get('human_labeled') == False, f"human_labeled should be False for {case['description']}"
                    assert row.get('anomalous') == False, f"anomalous should be False for {case['description']}"
                    assert row.get('reviewed') == False, f"reviewed should be False for {case['description']}"