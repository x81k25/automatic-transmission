import pytest
import os
import polars as pl
from src.core._06_media_filtration import *
from src.data_models import *
from tests.fixtures.core._06_media_filtration_fixtures import *

class TestMediaFiltration:
    """Test cases for _06_media_filtration functions."""

    def test_process_exempt_items(self, process_exempt_items_cases):
        """Test all process_exempt_items scenarios from fixture."""
        for case in process_exempt_items_cases:
            input_media = MediaDataFrame(case["input_data"])
            result = process_exempt_items(input_media)
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
                assert row["media_type"] == expected["media_type"], (
                    f"Failed for {case['description']}: "
                    f"expected media_type {expected['media_type']}, got {row['media_type']}"
                )

    def test_reject_media_without_imdb_id(self, reject_media_without_imdb_id_cases):
        """Test all reject_media_without_imdb_id scenarios from fixture."""
        for case in reject_media_without_imdb_id_cases:
            input_media = MediaDataFrame(case["input_data"])
            result = reject_media_without_imdb_id(input_media)
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
                assert row["rejection_reason"] == expected["rejection_reason"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_reason={expected['rejection_reason']}, got {row['rejection_reason']}"
                )

    def test_process_prelabeled_items(self, process_prelabeled_items_cases):
        """Test all process_prefiltered_items scenarios from fixture."""
        for case in process_prelabeled_items_cases:
            input_media = MediaDataFrame(case["input_media_data"])
            media_labels = pl.DataFrame(case["media_labels_data"])
            result = process_prelabeled_items(input_media, media_labels)
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
                assert row["rejection_reason"] == expected["rejection_reason"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_reason={expected['rejection_reason']}, got {row['rejection_reason']}"
                )

    def test_update_status(self, update_status_cases):
        """Test all update_status scenarios from fixture."""
        # Get the threshold from environment variable, fallback to 0.35 for testing
        threshold = float(os.getenv('REEL_DRIVER_THRESHOLD', '0.35'))

        for case in update_status_cases:
            # Create MediaDataFrame with standard schema (no probability)
            input_media = MediaDataFrame(case["input_data"])

            # Add probability column with test values if not None
            probability_values = case["probability_values"]
            if probability_values[0] is not None:
                input_media_with_probability = input_media.df.with_columns(
                    probability=pl.Series(probability_values)
                )
                # Call update_status with the modified DataFrame
                result = update_status(MediaDataFrame(input_media_with_probability))
            else:
                # Call update_status without probability column
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
                assert row["rejection_status"] == expected["rejection_status"], (
                    f"Failed for {case['description']}: "
                    f"expected rejection_status={expected['rejection_status']}, got {row['rejection_status']}"
                )
                assert row["pipeline_status"] == expected["pipeline_status"], (
                    f"Failed for {case['description']}: "
                    f"expected pipeline_status={expected['pipeline_status']}, got {row['pipeline_status']}"
                )

                # For cases with probability-based rejection reasons, adjust expected message with actual threshold
                if "rejection_reason" in expected:
                    expected_reason = expected["rejection_reason"]
                    if expected_reason and "below threshold" in expected_reason:
                        # Extract probability from the rejection reason and rebuild with actual threshold
                        parts = expected_reason.split()
                        if len(parts) >= 4:  # "probability X.XXX below threshold Y.YY"
                            prob_value = parts[1]
                            expected_reason = f"probability {prob_value} below threshold {threshold}"

                    assert row["rejection_reason"] == expected_reason, (
                        f"Failed for {case['description']}: "
                        f"expected rejection_reason={expected_reason}, got {row['rejection_reason']}"
                    )