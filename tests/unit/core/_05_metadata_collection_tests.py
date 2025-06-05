import pytest
from src.core._05_metadata_collection import *
from src.data_models import *
from tests.fixtures.core._05_metadata_collection_fixtures import *

class TestMetadataCollection:
    """Test cases for _05_metadata_collection functions."""

    def test_process_media_with_existing_metadata(self, process_media_with_existing_metadata_cases):
        """Test all process_media_with_existing_metadata scenarios from fixture."""
        for case in process_media_with_existing_metadata_cases:
            input_media = MediaDataFrame(case["input_media_data"])
            existing_metadata = pl.DataFrame(case["existing_metadata_data"])
            result = process_media_with_existing_metadata(input_media, existing_metadata)
            expected_list = case["expected_fields"]

            assert isinstance(result, MediaDataFrame)
            assert result.df.height == len(expected_list), f"Row count mismatch for {case['description']}"

            for i in range(result.df.height):
                row = result.df.row(i, named=True)
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