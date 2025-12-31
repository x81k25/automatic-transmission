import pytest
from src.utils.parse_element import *
from tests.fixtures.utils.parse_elements_fixtures import *

class TestParseElements:
    """Test cases for all parse_element functions."""

    def test_extract_hash_from_direct_download_url(self, extract_hash_from_direct_download_url_cases):
        """Test all hash extraction scenarios from fixture."""
        for case in extract_hash_from_direct_download_url_cases:
            result = extract_hash_from_direct_download_url(case["url"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_hash_from_magnet_link(self, extract_hash_from_magnet_link_cases):
        """Test all magnet hash extraction scenarios from fixture."""
        for case in extract_hash_from_magnet_link_cases:
            result = extract_hash_from_magnet_link(case["url"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_classify_media_type(self, classify_media_type_cases):
        """Test all media type classification scenarios from fixture."""
        for case in classify_media_type_cases:
            result = classify_media_type(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_title(self, extract_title_cases):
        """Test all title extraction scenarios from fixture."""
        for case in extract_title_cases:
            result = extract_title(case["raw_title"], case["media_type"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_year(self, extract_year_cases):
        """Test all year extraction scenarios from fixture."""
        for case in extract_year_cases:
            result = extract_year(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_season_from_episode(self, extract_season_from_episode_cases):
        """Test all season extraction from episode scenarios from fixture."""
        for case in extract_season_from_episode_cases:
            result = extract_season_from_episode(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_episode_from_episode(self, extract_episode_from_episode_cases):
        """Test all episode extraction scenarios from fixture."""
        for case in extract_episode_from_episode_cases:
            result = extract_episode_from_episode(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_season_from_season(self, extract_season_from_season_cases):
        """Test all season extraction from season scenarios from fixture."""
        for case in extract_season_from_season_cases:
            result = extract_season_from_season(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_season_from_episode_pack(self, extract_season_from_episode_pack_cases):
        """Test all season extraction from episode pack scenarios from fixture."""
        for case in extract_season_from_episode_pack_cases:
            result = extract_season_from_episode_pack(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_resolution(self, extract_resolution_cases):
        """Test all resolution extraction scenarios from fixture."""
        for case in extract_resolution_cases:
            result = extract_resolution(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_video_codec(self, extract_video_codec_cases):
        """Test all video codec extraction scenarios from fixture."""
        for case in extract_video_codec_cases:
            result = extract_video_codec(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_audio_codec(self, extract_audio_codec_cases):
        """Test all audio codec extraction scenarios from fixture."""
        for case in extract_audio_codec_cases:
            result = extract_audio_codec(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_upload_type(self, extract_upload_type_cases):
        """Test all upload type extraction scenarios from fixture."""
        for case in extract_upload_type_cases:
            result = extract_upload_type(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

    def test_extract_uploader(self, extract_uploader_cases):
        """Test all uploader extraction scenarios from fixture."""
        for case in extract_uploader_cases:
            result = extract_uploader(case["title"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )