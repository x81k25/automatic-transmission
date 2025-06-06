import pytest
from src.data_models import *

@pytest.fixture
def filter_by_file_metadata_cases():
    """Test scenarios for filter_by_file_metadata function."""
    return [
        {
            "description": "Movie with valid 1080p resolution",
            "input": {
                "hash": "valid1080p123456789012345678901234567890",
                "media_type": "movie",
                "media_title": "Valid Movie",
                "release_year": 2020,
                "resolution": "1080p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "unfiltered",
                "rejection_reason": None
            },
            "expected": {
                "hash": "valid1080p123456789012345678901234567890",
                "rejection_reason": None
            }
        },
        {
            "description": "Movie with invalid 720p resolution",
            "input": {
                "hash": "invalid720p12345678901234567890123456789",
                "media_type": "movie",
                "media_title": "Invalid Movie",
                "release_year": 2020,
                "resolution": "720p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "unfiltered",
                "rejection_reason": None
            },
            "expected": {
                "hash": "invalid720p12345678901234567890123456789",
                "rejection_reason": "resolution 720p is not in allowed_values"
            }
        },
        {
            "description": "Movie with null resolution (not nullable)",
            "input": {
                "hash": "nullres123456789012345678901234567890123",
                "media_type": "movie",
                "media_title": "Null Resolution Movie",
                "release_year": 2020,
                "resolution": None,
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "unfiltered",
                "rejection_reason": None
            },
            "expected": {
                "hash": "nullres123456789012345678901234567890123",
                "rejection_reason": "resolution is null"
            }
        },
        {
            "description": "Movie with invalid 2160p resolution",
            "input": {
                "hash": "invalid2160p1234567890123456789012345678",
                "media_type": "movie",
                "media_title": "4K Movie",
                "release_year": 2020,
                "resolution": "2160p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "unfiltered",
                "rejection_reason": None
            },
            "expected": {
                "hash": "invalid2160p1234567890123456789012345678",
                "rejection_reason": "resolution 2160p is not in allowed_values"
            }
        },
        {
            "description": "Movie with invalid 480p resolution",
            "input": {
                "hash": "invalid480p12345678901234567890123456789",
                "media_type": "movie",
                "media_title": "SD Movie",
                "release_year": 2020,
                "resolution": "480p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "unfiltered",
                "rejection_reason": None
            },
            "expected": {
                "hash": "invalid480p12345678901234567890123456789",
                "rejection_reason": "resolution 480p is not in allowed_values"
            }
        },
        {
            "description": "TV show with any resolution (no filters apply)",
            "input": {
                "hash": "tvshow720p123456789012345678901234567890",
                "media_type": "tv_show",
                "media_title": "TV Show",
                "season": 1,
                "episode": 1,
                "resolution": "720p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "unfiltered",
                "rejection_reason": None
            },
            "expected": {
                "hash": "tvshow720p123456789012345678901234567890",
                "rejection_reason": None
            }
        },
        {
            "description": "TV season with any resolution (no filters apply)",
            "input": {
                "hash": "tvseason480p12345678901234567890123456789",
                "media_type": "tv_season",
                "media_title": "TV Season",
                "season": 2,
                "resolution": "480p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "unfiltered",
                "rejection_reason": None
            },
            "expected": {
                "hash": "tvseason480p12345678901234567890123456789",
                "rejection_reason": None
            }
        },
        {
            "description": "Movie with existing rejection reason unchanged",
            "input": {
                "hash": "existing123456789012345678901234567890123",
                "media_type": "movie",
                "media_title": "Movie with Existing Rejection",
                "release_year": 2020,
                "resolution": "720p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "rejected",
                "rejection_reason": "existing rejection reason"
            },
            "expected": {
                "hash": "existing123456789012345678901234567890123",
                "rejection_reason": "resolution 720p is not in allowed_values"
            }
        },
        {
            "description": "Movie with override status and invalid resolution",
            "input": {
                "hash": "override123456789012345678901234567890123",
                "media_type": "movie",
                "media_title": "Override Movie",
                "release_year": 2020,
                "resolution": "720p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "override",
                "rejection_reason": None
            },
            "expected": {
                "hash": "override123456789012345678901234567890123",
                "rejection_reason": "resolution 720p is not in allowed_values"
            }
        },
        {
            "description": "Movie with valid resolution and override status",
            "input": {
                "hash": "validoverride123456789012345678901234567",
                "media_type": "movie",
                "media_title": "Valid Override Movie",
                "release_year": 2020,
                "resolution": "1080p",
                "pipeline_status": "parsed",
                "error_status": False,
                "rejection_status": "override",
                "rejection_reason": None
            },
            "expected": {
                "hash": "validoverride123456789012345678901234567",
                "rejection_reason": None
            }
        }
    ]


@pytest.fixture
def update_status_cases():
    """Test scenarios for update_status function."""
    return [
        {
            "description": "Single item with no rejection reason gets accepted",
            "input_data": [
                {
                    "hash": "accepted123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Accepted Movie",
                    "release_year": 2020,
                    "resolution": "1080p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered",
                    "rejection_reason": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "accepted123456789012345678901234567890123",
                    "rejection_status": "accepted",
                    "pipeline_status": "file_accepted"
                }
            ]
        },
        {
            "description": "Single item with rejection reason gets rejected",
            "input_data": [
                {
                    "hash": "rejected123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Rejected Movie",
                    "release_year": 2020,
                    "resolution": "720p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered",
                    "rejection_reason": "resolution 720p is not in allowed_values"
                }
            ],
            "expected_fields": [
                {
                    "hash": "rejected123456789012345678901234567890123",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "Item with override status maintains override regardless of rejection reason",
            "input_data": [
                {
                    "hash": "override123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Override Movie",
                    "release_year": 2020,
                    "resolution": "720p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "override",
                    "rejection_reason": "resolution 720p is not in allowed_values"
                }
            ],
            "expected_fields": [
                {
                    "hash": "override123456789012345678901234567890123",
                    "rejection_status": "override",
                    "pipeline_status": "file_accepted"
                }
            ]
        },
        {
            "description": "Item with override status and no rejection reason maintains override",
            "input_data": [
                {
                    "hash": "validoverride123456789012345678901234567",
                    "media_type": "movie",
                    "media_title": "Valid Override Movie",
                    "release_year": 2020,
                    "resolution": "1080p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "override",
                    "rejection_reason": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "validoverride123456789012345678901234567",
                    "rejection_status": "override",
                    "pipeline_status": "file_accepted"
                }
            ]
        },
        {
            "description": "Item already rejected maintains rejected status",
            "input_data": [
                {
                    "hash": "alreadyrejected123456789012345678901234567",
                    "media_type": "movie",
                    "media_title": "Already Rejected Movie",
                    "release_year": 2020,
                    "resolution": "720p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values"
                }
            ],
            "expected_fields": [
                {
                    "hash": "alreadyrejected123456789012345678901234567",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "Multiple items with mixed statuses",
            "input_data": [
                {
                    "hash": "mixed1234567890123456789012345678901234567",
                    "media_type": "movie",
                    "media_title": "Good Movie",
                    "release_year": 2020,
                    "resolution": "1080p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered",
                    "rejection_reason": None
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678",
                    "media_type": "movie",
                    "media_title": "Bad Movie",
                    "release_year": 2020,
                    "resolution": "720p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered",
                    "rejection_reason": "resolution 720p is not in allowed_values"
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789",
                    "media_type": "movie",
                    "media_title": "Override Movie",
                    "release_year": 2020,
                    "resolution": "480p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "override",
                    "rejection_reason": "resolution 480p is not in allowed_values"
                }
            ],
            "expected_fields": [
                {
                    "hash": "mixed1234567890123456789012345678901234567",
                    "rejection_status": "accepted",
                    "pipeline_status": "file_accepted"
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected"
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789",
                    "rejection_status": "override",
                    "pipeline_status": "file_accepted"
                }
            ]
        },
        {
            "description": "TV show and TV season items get accepted regardless of resolution",
            "input_data": [
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "media_type": "tv_show",
                    "media_title": "TV Show",
                    "season": 1,
                    "episode": 1,
                    "resolution": "720p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered",
                    "rejection_reason": None
                },
                {
                    "hash": "tvseason123456789012345678901234567890123456",
                    "media_type": "tv_season",
                    "media_title": "TV Season",
                    "season": 2,
                    "resolution": "480p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered",
                    "rejection_reason": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "rejection_status": "accepted",
                    "pipeline_status": "file_accepted"
                },
                {
                    "hash": "tvseason123456789012345678901234567890123456",
                    "rejection_status": "accepted",
                    "pipeline_status": "file_accepted"
                }
            ]
        },
        {
            "description": "All items with override status maintain file_accepted pipeline status",
            "input_data": [
                {
                    "hash": "alloverride1234567890123456789012345678901",
                    "media_type": "movie",
                    "media_title": "Override Movie 1",
                    "release_year": 2020,
                    "resolution": "1080p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "override",
                    "rejection_reason": None
                },
                {
                    "hash": "alloverride2345678901234567890123456789012",
                    "media_type": "movie",
                    "media_title": "Override Movie 2",
                    "release_year": 2020,
                    "resolution": "720p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "override",
                    "rejection_reason": "resolution 720p is not in allowed_values"
                }
            ],
            "expected_fields": [
                {
                    "hash": "alloverride1234567890123456789012345678901",
                    "rejection_status": "override",
                    "pipeline_status": "file_accepted"
                },
                {
                    "hash": "alloverride2345678901234567890123456789012",
                    "rejection_status": "override",
                    "pipeline_status": "file_accepted"
                }
            ]
        },
        {
            "description": "All items with rejected status maintain rejected pipeline status",
            "input_data": [
                {
                    "hash": "allrejected1234567890123456789012345678901",
                    "media_type": "movie",
                    "media_title": "Rejected Movie 1",
                    "release_year": 2020,
                    "resolution": "720p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values"
                },
                {
                    "hash": "allrejected2345678901234567890123456789012",
                    "media_type": "movie",
                    "media_title": "Rejected Movie 2",
                    "release_year": 2020,
                    "resolution": "480p",
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 480p is not in allowed_values"
                }
            ],
            "expected_fields": [
                {
                    "hash": "allrejected1234567890123456789012345678901",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected"
                },
                {
                    "hash": "allrejected2345678901234567890123456789012",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected"
                }
            ]
        }
    ]