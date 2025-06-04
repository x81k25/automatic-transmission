import pytest

var = "iejoajweoifjoawejfa"

@pytest.fixture
def extract_hash_from_direct_download_url_cases():
    """10 test scenarios for extract_hash_from_direct_download_url function."""
    return [
        {
            "description": "Standard YTS torrent URL",
            "url": "https://yts.mx/torrent/download/ABC123DEF456789012345678901234567890ABCD.torrent",
            "expected": "abc123def456789012345678901234567890abcd.torrent"
        },
        {
            "description": "URL without file extension",
            "url": "https://example.com/downloads/ABC123DEF456789012345678901234567890ABCD",
            "expected": "abc123def456789012345678901234567890abcd"
        },
        {
            "description": "Hash only with no path",
            "url": "ABC123DEF456789012345678901234567890ABCD",
            "expected": "abc123def456789012345678901234567890abcd"
        },
        {
            "description": "URL with multiple path segments",
            "url": "https://torrents.example.com/files/movies/2024/DEF456789012345678901234567890ABCDEF123456.torrent",
            "expected": "def456789012345678901234567890abcdef123456.torrent"
        },
        {
            "description": "Uppercase hash gets lowercased",
            "url": "https://example.com/FEDCBA0987654321FEDCBA0987654321FEDCBA09",
            "expected": "fedcba0987654321fedcba0987654321fedcba09"
        },
        {
            "description": "Empty string returns None",
            "url": "",
            "expected": None
        },
        {
            "description": "URL ending with slash returns None",
            "url": "https://example.com/downloads/",
            "expected": None
        },
        {
            "description": "Just a slash returns None",
            "url": "/",
            "expected": None
        },
        {
            "description": "URL with query parameters",
            "url": "https://example.com/download/1234567890ABCDEF1234567890ABCDEF12345678.torrent?key=value",
            "expected": "1234567890abcdef1234567890abcdef12345678.torrent?key=value"
        },
        {
            "description": "URL with special characters in filename",
            "url": "https://example.com/ABC123_special-file.torrent",
            "expected": "abc123_special-file.torrent"
        }
    ]