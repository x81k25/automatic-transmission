import pytest
from src.data_models import *

@pytest.fixture
def update_status_cases():
    """Test scenarios for update_status function."""
    return [
        {
            "description": "Single item with accepted status and no errors gets transferred",
            "input_data": [
                {
                    "hash": "accepted123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Accepted Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "accepted123456789012345678901234567890123",
                    "pipeline_status": "transferred"
                }
            ]
        },
        {
            "description": "Single item with override status and no errors gets transferred",
            "input_data": [
                {
                    "hash": "override123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Override Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "override123456789012345678901234567890123",
                    "pipeline_status": "transferred"
                }
            ]
        },
        {
            "description": "Single item with rejected status gets rejected pipeline status",
            "input_data": [
                {
                    "hash": "rejected123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Rejected Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "rejected123456789012345678901234567890123",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "Single item with accepted status but with error keeps original status",
            "input_data": [
                {
                    "hash": "erroraccepted123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "failed to transfer media: Permission denied"
                }
            ],
            "expected_fields": [
                {
                    "hash": "erroraccepted123456789012345678901234567890",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Single item with override status but with error keeps original status",
            "input_data": [
                {
                    "hash": "erroroverride123456789012345678901234567890",
                    "media_type": "tv_show",
                    "media_title": "Error TV Show",
                    "season": 1,
                    "episode": 1,
                    "pipeline_status": "downloaded",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "failed to transfer media: Disk full"
                }
            ],
            "expected_fields": [
                {
                    "hash": "erroroverride123456789012345678901234567890",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Multiple items with mixed acceptance statuses and no errors",
            "input_data": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Good Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "Override Show",
                    "season": 2,
                    "episode": 5,
                    "pipeline_status": "downloaded",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "media_type": "tv_season",
                    "media_title": "Rejected Season",
                    "season": 3,
                    "pipeline_status": "downloaded",
                    "rejection_status": "rejected",
                    "rejection_reason": "media search failed",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "pipeline_status": "transferred"
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "pipeline_status": "transferred"
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "Multiple items with mixed error and acceptance statuses",
            "input_data": [
                {
                    "hash": "mixederror1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Success Movie",
                    "release_year": 2021,
                    "pipeline_status": "downloaded",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixederror2345678901234567890123456789012345",
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "release_year": 2021,
                    "pipeline_status": "downloaded",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "failed to transfer media: Network timeout"
                },
                {
                    "hash": "mixederror3456789012345678901234567890123456",
                    "media_type": "tv_show",
                    "media_title": "Rejected Show",
                    "season": 1,
                    "episode": 10,
                    "pipeline_status": "downloaded",
                    "rejection_status": "rejected",
                    "rejection_reason": "no imdb_id for media filtration",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "mixederror1234567890123456789012345678901234",
                    "pipeline_status": "transferred"
                },
                {
                    "hash": "mixederror2345678901234567890123456789012345",
                    "pipeline_status": "downloaded"
                },
                {
                    "hash": "mixederror3456789012345678901234567890123456",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "All items with rejected status get rejected pipeline status",
            "input_data": [
                {
                    "hash": "allrejected1234567890123456789012345678901",
                    "media_type": "movie",
                    "media_title": "Rejected Movie 1",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values",
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "allrejected2345678901234567890123456789012",
                    "media_type": "movie",
                    "media_title": "Rejected Movie 2",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "rejected",
                    "rejection_reason": "probability 0.2 below threshold 0.35",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "allrejected1234567890123456789012345678901",
                    "pipeline_status": "rejected"
                },
                {
                    "hash": "allrejected2345678901234567890123456789012",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "All items with errors keep original pipeline status",
            "input_data": [
                {
                    "hash": "allerrors1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Error Movie 1",
                    "release_year": 2022,
                    "pipeline_status": "downloaded",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "failed to transfer media: File not found"
                },
                {
                    "hash": "allerrors2345678901234567890123456789012345",
                    "media_type": "tv_season",
                    "media_title": "Error Season",
                    "season": 2,
                    "pipeline_status": "downloaded",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "failed to transfer media: Invalid path"
                }
            ],
            "expected_fields": [
                {
                    "hash": "allerrors1234567890123456789012345678901234",
                    "pipeline_status": "downloaded"
                },
                {
                    "hash": "allerrors2345678901234567890123456789012345",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "All items with accepted/override status and no errors get transferred",
            "input_data": [
                {
                    "hash": "allsuccess1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Success Movie 1",
                    "release_year": 2023,
                    "pipeline_status": "downloaded",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "allsuccess2345678901234567890123456789012345",
                    "media_type": "tv_show",
                    "media_title": "Success Show",
                    "season": 1,
                    "episode": 5,
                    "pipeline_status": "downloaded",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "allsuccess3456789012345678901234567890123456",
                    "media_type": "tv_season",
                    "media_title": "Success Season",
                    "season": 4,
                    "pipeline_status": "downloaded",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "allsuccess1234567890123456789012345678901234",
                    "pipeline_status": "transferred"
                },
                {
                    "hash": "allsuccess2345678901234567890123456789012345",
                    "pipeline_status": "transferred"
                },
                {
                    "hash": "allsuccess3456789012345678901234567890123456",
                    "pipeline_status": "transferred"
                }
            ]
        },
        {
            "description": "Item with unfiltered status keeps original pipeline status",
            "input_data": [
                {
                    "hash": "unfiltered123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Unfiltered Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "unfiltered",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "unfiltered123456789012345678901234567890123",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Item with rejected status and error gets rejected pipeline status",
            "input_data": [
                {
                    "hash": "rejectederror123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Rejected Error Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloaded",
                    "rejection_status": "rejected",
                    "rejection_reason": "media search failed",
                    "error_status": True,
                    "error_condition": "failed to transfer media: Access denied"
                }
            ],
            "expected_fields": [
                {
                    "hash": "rejectederror123456789012345678901234567890",
                    "pipeline_status": "rejected"
                }
            ]
        }
    ]