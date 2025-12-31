import pytest
import polars as pl
from src.core._09_transfer import *
from src.data_models import *
from tests.fixtures.core._09_transfer_fixtures import *

class TestTransfer:
    """Test cases for _09_transfer functions."""

    def test_generate_file_paths(self, generate_file_paths_cases, monkeypatch):
        """Test all generate_file_paths scenarios from fixture."""
        # Mock the environment variables
        monkeypatch.setenv('DOWNLOAD_DIR', '/d/media-cache/prod/complete/')
        monkeypatch.setenv('AT_MOVIE_DIR', '/k/media/video/movies/')
        monkeypatch.setenv('AT_TV_SHOW_DIR', '/k/media/video/tv/')

        for case in generate_file_paths_cases:
            # Make a copy to avoid modifying the original fixture data
            media_item = case["input"].copy()

            # Call the function - should never raise an exception
            result = generate_file_paths(media_item)
            expected = case["expected"]

            # Check that the result is the same object (modified in place)
            assert result is media_item, (
                f"Failed for {case['description']}: "
                f"generate_file_paths should modify the input dict in place"
            )

            # Check hash is preserved
            assert result["hash"] == expected["hash"], (
                f"Failed for {case['description']}: "
                f"expected hash {expected['hash']}, got {result['hash']}"
            )

            # Check error_condition
            assert result.get("error_condition") == expected["error_condition"], (
                f"Failed for {case['description']}: "
                f"expected error_condition={expected['error_condition']}, got {result.get('error_condition')}"
            )

            # If no error_condition, check that paths were set correctly
            if expected["error_condition"] is None:
                assert result.get("parent_path") == expected["parent_path"], (
                    f"Failed for {case['description']}: "
                    f"expected parent_path={expected['parent_path']}, got {result.get('parent_path')}"
                )
                assert result.get("target_path") == expected["target_path"], (
                    f"Failed for {case['description']}: "
                    f"expected target_path={expected['target_path']}, got {result.get('target_path')}"
                )
            else:
                # If there was an error, paths might not be set
                # We just ensure the function completed without exception
                pass


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
