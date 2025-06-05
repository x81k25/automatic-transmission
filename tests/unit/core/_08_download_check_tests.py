import pytest
import polars as pl
from src.core._08_download_check import *
from src.data_models import *
from tests.fixtures.core._08_download_check_fixtures import *

class TestDownloadCheck:
    """Test cases for _08_download_check functions."""

    def test_update_status(self, update_status_cases):
        """Test all update_status scenarios from fixture."""
        for case in update_status_cases:
            # Create MediaDataFrame with standard schema (no download_complete)
            input_media = MediaDataFrame(case["input_data"])

            # Add download_complete column with test values
            download_values = case["download_complete_values"]
            input_media_with_download = input_media.df.with_columns(
                download_complete=pl.Series(download_values)
            )

            # Call update_status with the modified DataFrame
            result = update_status(MediaDataFrame(input_media_with_download))
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