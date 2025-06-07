import pytest
import os
from pathlib import PurePosixPath
from src.utils.local_file_operations import *
from tests.fixtures.utils.local_file_operations_fixtures import *

class TestLocalFileOperations:
    """Test cases for local_file_operations functions."""

    def test_generate_movie_target_path(self, generate_movie_target_path_cases):
        """Test all generate_movie_target_path scenarios from fixture."""
        for case in generate_movie_target_path_cases:
            result = generate_movie_target_path(**case["input"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_generate_movie_target_path_errors(self, generate_movie_target_path_error_cases):
        """Test all generate_movie_target_path error scenarios from fixture."""
        for case in generate_movie_target_path_error_cases:
            with pytest.raises(case["expected_error"]):
                generate_movie_target_path(**case["input"])

    def test_generate_tv_season_parent_path(self, generate_tv_season_parent_path_cases):
        """Test all generate_tv_season_parent_path scenarios from fixture."""
        for case in generate_tv_season_parent_path_cases:
            result = generate_tv_season_parent_path(**case["input"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_generate_tv_season_parent_path_errors(self, generate_tv_season_parent_path_error_cases):
        """Test all generate_tv_season_parent_path error scenarios from fixture."""
        for case in generate_tv_season_parent_path_error_cases:
            with pytest.raises(case["expected_error"]):
                generate_tv_season_parent_path(**case["input"])

    def test_generate_tv_season_target_path(self, generate_tv_season_target_path_cases):
        """Test all generate_tv_season_target_path scenarios from fixture."""
        for case in generate_tv_season_target_path_cases:
            result = generate_tv_season_target_path(**case["input"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_generate_tv_season_target_path_errors(self, generate_tv_season_target_path_error_cases):
        """Test all generate_tv_season_target_path error scenarios from fixture."""
        for case in generate_tv_season_target_path_error_cases:
            with pytest.raises(case["expected_error"]):
                generate_tv_season_target_path(**case["input"])

    def test_generate_tv_show_parent_path(self, generate_tv_show_parent_path_cases):
        """Test all generate_tv_show_parent_path scenarios from fixture."""
        for case in generate_tv_show_parent_path_cases:
            result = generate_tv_show_parent_path(**case["input"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_generate_tv_show_parent_path_errors(self, generate_tv_show_parent_path_error_cases):
        """Test all generate_tv_show_parent_path error scenarios from fixture."""
        for case in generate_tv_show_parent_path_error_cases:
            with pytest.raises(case["expected_error"]):
                generate_tv_show_parent_path(**case["input"])

    def test_generate_tv_show_target_path(self, generate_tv_show_target_path_cases):
        """Test all generate_tv_show_target_path scenarios from fixture."""
        for case in generate_tv_show_target_path_cases:
            result = generate_tv_show_target_path(**case["input"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_generate_tv_show_target_path_errors(self, generate_tv_show_target_path_error_cases):
        """Test all generate_tv_show_target_path error scenarios from fixture."""
        for case in generate_tv_show_target_path_error_cases:
            with pytest.raises(case["expected_error"]):
                generate_tv_show_target_path(**case["input"])