import pytest
from feedparser import FeedParserDict
from src.data_models import *

@pytest.fixture
def rss_feed_ingest_cases():
    """Test scenarios for rss_feed_ingest function."""
    return [
        {
            "description": "yts.mx RSS feed with movie entries",
            "rss_url": "https://yts.mx/rss/",
            "rss_source": "yts.mx",
            "mock_feed_data": {
                "channel": {"title": "YTS.MX RSS Feed"},
                "entries": [
                    {
                        "title": "Test Movie 1 (2023)",
                        "links": [
                            {"href": "https://yts.mx/movie/test-movie-1"},
                            {"href": "https://yts.mx/torrent/download/ABC123DEF456"}
                        ],
                        "description": "Test Movie 1 Description"
                    },
                    {
                        "title": "Test Movie 2 (2024)",
                        "links": [
                            {"href": "https://yts.mx/movie/test-movie-2"},
                            {"href": "https://yts.mx/torrent/download/GHI789JKL012"}
                        ],
                        "description": "Test Movie 2 Description"
                    }
                ]
            },
            "expected_entries": [
                {
                    "title": "Test Movie 1 (2023)",
                    "links": [
                        {"href": "https://yts.mx/movie/test-movie-1"},
                        {"href": "https://yts.mx/torrent/download/ABC123DEF456"}
                    ],
                    "description": "Test Movie 1 Description",
                    "rss_source": "yts.mx"
                },
                {
                    "title": "Test Movie 2 (2024)",
                    "links": [
                        {"href": "https://yts.mx/movie/test-movie-2"},
                        {"href": "https://yts.mx/torrent/download/GHI789JKL012"}
                    ],
                    "description": "Test Movie 2 Description",
                    "rss_source": "yts.mx"
                }
            ]
        },
        {
            "description": "episodefeed.com RSS feed with TV show entries",
            "rss_url": "https://episodefeed.com/rss/148/abc123",
            "rss_source": "episodefeed.com",
            "mock_feed_data": {
                "channel": {"title": "EpisodeFeed Custom RSS"},
                "entries": [
                    {
                        "title": "Test Show S01E01 1080p HDTV",
                        "link": "magnet:?xt=urn:btih:1234567890abcdef1234567890abcdef12345678",
                        "tv_show_name": "Test Show",
                        "description": "Episode 1 Description"
                    },
                    {
                        "title": "Test Show S01E02 720p WEB-DL",
                        "link": "magnet:?xt=urn:btih:fedcba0987654321fedcba0987654321fedcba09",
                        "tv_show_name": "Test Show",
                        "description": "Episode 2 Description"
                    }
                ]
            },
            "expected_entries": [
                {
                    "title": "Test Show S01E01 1080p HDTV",
                    "link": "magnet:?xt=urn:btih:1234567890abcdef1234567890abcdef12345678",
                    "tv_show_name": "Test Show",
                    "description": "Episode 1 Description",
                    "rss_source": "episodefeed.com"
                },
                {
                    "title": "Test Show S01E02 720p WEB-DL",
                    "link": "magnet:?xt=urn:btih:fedcba0987654321fedcba0987654321fedcba09",
                    "tv_show_name": "Test Show",
                    "description": "Episode 2 Description",
                    "rss_source": "episodefeed.com"
                }
            ]
        },
        {
            "description": "Empty RSS feed",
            "rss_url": "https://example.com/empty/rss",
            "rss_source": "example.com",
            "mock_feed_data": {
                "channel": {"title": "Empty Feed"},
                "entries": []
            },
            "expected_entries": []
        }
    ]


@pytest.fixture
def format_entries_cases():
    """Test scenarios for format_entries function."""
    return [
        {
            "description": "yts.mx movie entry formatting",
            "input_entry": FeedParserDict({
                "title": "Test Movie (2023)",
                "links": [
                    {"href": "https://yts.mx/movie/test-movie"},
                    {"href": "https://yts.mx/torrent/download/ABC123DEF456GHI789JKL012MNO345PQR678STU"}
                ],
                "description": "Test Movie Description",
                "rss_source": "yts.mx"
            }),
            "expected_output": {
                "hash": "ABC123DEF456GHI789JKL012MNO345PQR678STU",
                "media_type": "movie",
                "original_title": "Test Movie (2023)",
                "original_link": "https://yts.mx/torrent/download/ABC123DEF456GHI789JKL012MNO345PQR678STU"
            }
        },
        {
            "description": "episodefeed.com TV episode entry formatting",
            "input_entry": FeedParserDict({
                "title": "Test Show S01E01 1080p HDTV",
                "link": "magnet:?xt=urn:btih:1234567890abcdef1234567890abcdef12345678&dn=Test+Show+S01E01",
                "tv_show_name": "Test Show",
                "description": "Episode Description",
                "rss_source": "episodefeed.com"
            }),
            "expected_output": {
                "hash": "1234567890abcdef1234567890abcdef12345678",
                "media_type": "tv_show",
                "media_title": "Test Show",
                "original_title": "Test Show S01E01 1080p HDTV",
                "original_link": "magnet:?xt=urn:btih:1234567890abcdef1234567890abcdef12345678&dn=Test+Show+S01E01"
            }
        },
        {
            "description": "episodefeed.com TV season pack entry formatting",
            "input_entry": FeedParserDict({
                "title": "Another Series S02 Complete 1080p WEB-DL",
                "link": "magnet:?xt=urn:btih:fedcba0987654321fedcba0987654321fedcba09",
                "tv_show_name": "Another Series",
                "description": "Season 2 Complete",
                "rss_source": "episodefeed.com"
            }),
            "expected_output": {
                "hash": "fedcba0987654321fedcba0987654321fedcba09",
                "media_type": "tv_season",
                "media_title": "Another Series",
                "original_title": "Another Series S02 Complete 1080p WEB-DL",
                "original_link": "magnet:?xt=urn:btih:fedcba0987654321fedcba0987654321fedcba09"
            }
        }
    ]


@pytest.fixture
def rss_ingest_workflow_cases():
    """Test scenarios for rss_ingest workflow integration."""
    return [
        {
            "description": "Complete workflow with both RSS sources and new entries",
            "env_vars": {
                "AT_RSS_SOURCES": "yts.mx,episodefeed.com",
                "AT_RSS_URLS": "https://yts.mx/rss/,https://episodefeed.com/rss/148/abc123"
            },
            "mock_feed_data": [
                {
                    "channel": {"title": "YTS.MX RSS Feed"},
                    "entries": [
                        {
                            "title": "New Movie (2024)",
                            "links": [
                                {"href": "https://yts.mx/movie/new-movie"},
                                {"href": "https://yts.mx/torrent/download/NEWHASH123456789012345678901234567890123"}
                            ]
                        }
                    ]
                },
                {
                    "channel": {"title": "EpisodeFeed Custom RSS"},
                    "entries": [
                        {
                            "title": "New Show S01E01 1080p",
                            "link": "magnet:?xt=urn:btih:NEWHASH234567890123456789012345678901234",
                            "tv_show_name": "New Show"
                        }
                    ]
                }
            ],
            "mock_db_hashes": [],  # All hashes are new
            "expected_db_insert_count": 1,
            "expected_insert_items": [
                {
                    "hash": "NEWHASH123456789012345678901234567890123",
                    "media_type": "movie",
                    "original_title": "New Movie (2024)",
                    "pipeline_status": "ingested"
                },
                {
                    "hash": "NEWHASH234567890123456789012345678901234",
                    "media_type": "tv_show",
                    "media_title": "New Show",
                    "original_title": "New Show S01E01 1080p",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "Workflow with duplicate entries within same ingest",
            "env_vars": {
                "AT_RSS_SOURCES": "yts.mx,yts.mx",
                "AT_RSS_URLS": "https://yts.mx/rss/1,https://yts.mx/rss/2"
            },
            "mock_feed_data": [
                {
                    "channel": {"title": "YTS.MX RSS Feed 1"},
                    "entries": [
                        {
                            "title": "Duplicate Movie (2024)",
                            "links": [
                                {"href": "https://yts.mx/movie/dup-movie"},
                                {"href": "https://yts.mx/torrent/download/DUPHASH123456789012345678901234567890123"}
                            ]
                        }
                    ]
                },
                {
                    "channel": {"title": "YTS.MX RSS Feed 2"},
                    "entries": [
                        {
                            "title": "Duplicate Movie (2024)",
                            "links": [
                                {"href": "https://yts.mx/movie/dup-movie"},
                                {"href": "https://yts.mx/torrent/download/DUPHASH123456789012345678901234567890123"}
                            ]
                        }
                    ]
                }
            ],
            "mock_db_hashes": [],
            "expected_db_insert_count": 1,
            "expected_insert_items": [
                {
                    "hash": "DUPHASH123456789012345678901234567890123",
                    "media_type": "movie",
                    "original_title": "Duplicate Movie (2024)",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "Workflow with all entries already in database",
            "env_vars": {
                "AT_RSS_SOURCES": "yts.mx",
                "AT_RSS_URLS": "https://yts.mx/rss/"
            },
            "mock_feed_data": [
                {
                    "channel": {"title": "YTS.MX RSS Feed"},
                    "entries": [
                        {
                            "title": "Existing Movie (2024)",
                            "links": [
                                {"href": "https://yts.mx/movie/existing"},
                                {"href": "https://yts.mx/torrent/download/EXISTHASH12345678901234567890123456789012"}
                            ]
                        }
                    ]
                }
            ],
            "mock_db_hashes": ["EXISTHASH12345678901234567890123456789012"],
            "expected_db_insert_count": 0,
            "expected_insert_items": []
        },
        {
            "description": "Workflow with mismatched RSS sources and URLs",
            "env_vars": {
                "AT_RSS_SOURCES": "yts.mx",
                "AT_RSS_URLS": "https://yts.mx/rss/,https://extra.com/rss/"
            },
            "mock_feed_data": [],
            "mock_db_hashes": [],
            "expected_db_insert_count": 0,
            "expected_insert_items": [],
            "expected_error_log": "env var rss_sources does not match length of rss_urls"
        },
        {
            "description": "Workflow with empty RSS feeds",
            "env_vars": {
                "AT_RSS_SOURCES": "yts.mx,episodefeed.com",
                "AT_RSS_URLS": "https://yts.mx/rss/,https://episodefeed.com/rss/"
            },
            "mock_feed_data": [
                {
                    "channel": {"title": "Empty YTS Feed"},
                    "entries": []
                },
                {
                    "channel": {"title": "Empty Episode Feed"},
                    "entries": []
                }
            ],
            "mock_db_hashes": [],
            "expected_db_insert_count": 0,
            "expected_insert_items": []
        }
    ]