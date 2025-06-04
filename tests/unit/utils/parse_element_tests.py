import pytest
from src.utils.parse_element import *
from tests.fixtures.utils.parse_elements_fixtures import *

class TestParseElements:
    """Test cases for extract_hash_from_direct_download_url function."""

    def test_extract_hash_from_direct_download_url(self, extract_hash_from_direct_download_url_cases):
        """Test all hash extraction scenarios from fixture."""
        for case in extract_hash_from_direct_download_url_cases:
            result = extract_hash_from_direct_download_url(case["url"])
            assert result == case["expected"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected']}, got {result}"
            )

