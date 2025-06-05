import pytest
from src.data_models import *

@pytest.fixture
def update_status_cases():
    """Test scenarios for update_status function."""
    return [
        {
            "description": "Single item with download_complete=true gets downloaded status",
            "input_data": [
                {
                    "hash": "completed123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Completed Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true"],
            "expected_fields": [
                {
                    "hash": "completed123456789012345678901234567890123",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Single item with download_complete=None gets re-ingested status",
            "input_data": [
                {
                    "hash": "incomplete123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Incomplete Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": [None],
            "expected_fields": [
                {
                    "hash": "incomplete123456789012345678901234567890123",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "Single item with download error gets re-ingested status",
            "input_data": [
                {
                    "hash": "errordownload123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["Hash not found in transmission"],
            "expected_fields": [
                {
                    "hash": "errordownload123456789012345678901234567890",
                    "pipeline_status": "ingested"
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
                    "pipeline_status": "downloading",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true"],
            "expected_fields": [
                {
                    "hash": "rejected123456789012345678901234567890123",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "Multiple items with mixed download completion statuses",
            "input_data": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Completed Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "Incomplete Show",
                    "season": 2,
                    "episode": 5,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "media_type": "tv_season",
                    "media_title": "Rejected Season",
                    "season": 3,
                    "pipeline_status": "downloading",
                    "rejection_status": "rejected",
                    "rejection_reason": "media search failed",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true", None, "true"],
            "expected_fields": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "pipeline_status": "downloaded"
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "pipeline_status": "ingested"
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "Multiple items with different error types during download check",
            "input_data": [
                {
                    "hash": "mixederror1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Success Movie",
                    "release_year": 2021,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixederror2345678901234567890123456789012345",
                    "media_type": "movie",
                    "media_title": "Not Found Movie",
                    "release_year": 2021,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixederror3456789012345678901234567890123456",
                    "media_type": "tv_show",
                    "media_title": "Connection Error Show",
                    "season": 1,
                    "episode": 10,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true", "Hash not found within transmission", "Connection timeout"],
            "expected_fields": [
                {
                    "hash": "mixederror1234567890123456789012345678901234",
                    "pipeline_status": "downloaded"
                },
                {
                    "hash": "mixederror2345678901234567890123456789012345",
                    "pipeline_status": "ingested"
                },
                {
                    "hash": "mixederror3456789012345678901234567890123456",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "All items with rejected status get rejected pipeline status regardless of download completion",
            "input_data": [
                {
                    "hash": "allrejected1234567890123456789012345678901",
                    "media_type": "movie",
                    "media_title": "Rejected Movie 1",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
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
                    "pipeline_status": "downloading",
                    "rejection_status": "rejected",
                    "rejection_reason": "probability 0.2 below threshold 0.35",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true", None],
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
            "description": "All items with completed downloads get downloaded status",
            "input_data": [
                {
                    "hash": "allcomplete1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Complete Movie 1",
                    "release_year": 2022,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "allcomplete2345678901234567890123456789012345",
                    "media_type": "tv_season",
                    "media_title": "Complete Season",
                    "season": 2,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true", "true"],
            "expected_fields": [
                {
                    "hash": "allcomplete1234567890123456789012345678901234",
                    "pipeline_status": "downloaded"
                },
                {
                    "hash": "allcomplete2345678901234567890123456789012345",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "All items with incomplete downloads get re-ingested status",
            "input_data": [
                {
                    "hash": "allincomplete123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Incomplete Movie 1",
                    "release_year": 2023,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "allincomplete234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "Incomplete Show",
                    "season": 1,
                    "episode": 5,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": [None, "RPC connection failed"],
            "expected_fields": [
                {
                    "hash": "allincomplete123456789012345678901234567890",
                    "pipeline_status": "ingested"
                },
                {
                    "hash": "allincomplete234567890123456789012345678901",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "Item with override status and completed download gets downloaded status",
            "input_data": [
                {
                    "hash": "override123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Override Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true"],
            "expected_fields": [
                {
                    "hash": "override123456789012345678901234567890123",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Item with unfiltered status and completed download gets downloaded status",
            "input_data": [
                {
                    "hash": "unfiltered123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Unfiltered Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "unfiltered",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true"],
            "expected_fields": [
                {
                    "hash": "unfiltered123456789012345678901234567890123",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Items with different starting pipeline statuses but completed downloads",
            "input_data": [
                {
                    "hash": "diffstatus1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Different Status Movie",
                    "release_year": 2021,
                    "pipeline_status": "media_accepted",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "diffstatus2345678901234567890123456789012345",
                    "media_type": "tv_show",
                    "media_title": "Different Status Show",
                    "season": 3,
                    "episode": 8,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "download_complete_values": ["true", "true"],
            "expected_fields": [
                {
                    "hash": "diffstatus1234567890123456789012345678901234",
                    "pipeline_status": "downloaded"
                },
                {
                    "hash": "diffstatus2345678901234567890123456789012345",
                    "pipeline_status": "downloaded"
                }
            ]
        }
    ]