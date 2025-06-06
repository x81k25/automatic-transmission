import pytest
from src.data_models import *

@pytest.fixture
def confirm_downloading_status_cases():
    """Test scenarios for confirm_downloading_status function."""
    return [
        {
            "description": "Single item found in transmission stays downloading",
            "input_media_data": [
                {
                    "hash": "downloading123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Downloading Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "current_media_items": {
                "downloading123456789012345678901234567890": {
                    "id": 133,
                    "name": "Downloading Movie (2020) [1080p] [BluRay] [YTS.MX]",
                    "progress": 45.5,
                    "status": "downloading"
                }
            },
            "expected_fields": []
        },
        {
            "description": "Single item not found in transmission gets error",
            "input_media_data": [
                {
                    "hash": "notfound123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Not Found Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "current_media_items": {
                "otherhash123456789012345678901234567890123": {
                    "id": 134,
                    "name": "Other Movie (2020) [1080p]",
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "expected_fields": [
                {
                    "hash": "notfound123456789012345678901234567890123",
                    "error_condition": "not found within transmission"
                }
            ]
        },
        {
            "description": "Multiple items with mixed presence in transmission",
            "input_media_data": [
                {
                    "hash": "found1234567890123456789012345678901234567",
                    "media_type": "movie",
                    "media_title": "Found Movie",
                    "release_year": 2021,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "missing123456789012345678901234567890123456",
                    "media_type": "tv_show",
                    "media_title": "Missing Show",
                    "season": 1,
                    "episode": 5,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "present123456789012345678901234567890123456",
                    "media_type": "tv_season",
                    "media_title": "Present Season",
                    "season": 2,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "error_status": False
                }
            ],
            "current_media_items": {
                "found1234567890123456789012345678901234567": {
                    "id": 135,
                    "name": "Found Movie (2021) [1080p]",
                    "progress": 75.2,
                    "status": "downloading"
                },
                "present123456789012345678901234567890123456": {
                    "id": 137,
                    "name": "Present Season S02 [1080p]",
                    "progress": 12.8,
                    "status": "downloading"
                }
            },
            "expected_fields": [
                {
                    "hash": "missing123456789012345678901234567890123456",
                    "error_condition": "not found within transmission"
                }
            ]
        },
        {
            "description": "All items found in transmission",
            "input_media_data": [
                {
                    "hash": "allfound1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "All Found Movie 1",
                    "release_year": 2022,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "allfound2345678901234567890123456789012345",
                    "media_type": "movie",
                    "media_title": "All Found Movie 2",
                    "release_year": 2022,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "error_status": False
                }
            ],
            "current_media_items": {
                "allfound1234567890123456789012345678901234": {
                    "id": 138,
                    "name": "All Found Movie 1 (2022) [1080p]",
                    "progress": 90.5,
                    "status": "downloading"
                },
                "allfound2345678901234567890123456789012345": {
                    "id": 139,
                    "name": "All Found Movie 2 (2022) [1080p]",
                    "progress": 33.3,
                    "status": "downloading"
                }
            },
            "expected_fields": []
        },
        {
            "description": "All items missing from transmission",
            "input_media_data": [
                {
                    "hash": "allmissing123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "All Missing Movie 1",
                    "release_year": 2023,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "allmissing234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "All Missing Show",
                    "season": 3,
                    "episode": 10,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "current_media_items": {
                "differenthash123456789012345678901234567890": {
                    "id": 140,
                    "name": "Different Content",
                    "progress": 50.0,
                    "status": "downloading"
                }
            },
            "expected_fields": [
                {
                    "hash": "allmissing123456789012345678901234567890",
                    "error_condition": "not found within transmission"
                },
                {
                    "hash": "allmissing234567890123456789012345678901",
                    "error_condition": "not found within transmission"
                }
            ]
        },
        {
            "description": "Empty current media items means all missing",
            "input_media_data": [
                {
                    "hash": "emptytransmission123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Empty Transmission Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "current_media_items": {},
            "expected_fields": [
                {
                    "hash": "emptytransmission123456789012345678901234",
                    "error_condition": "not found within transmission"
                }
            ]
        }
    ]

@pytest.fixture
def extract_and_verify_filename_cases():
    """Test scenarios for extract_and_verify_filename function."""
    return [
        {
            "description": "Single item with valid filename gets path extracted",
            "input_media_data": [
                {
                    "hash": "validfile123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Valid File Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "downloaded_media_items": {
                "validfile123456789012345678901234567890123": {
                    "id": 150,
                    "name": "Valid File Movie (2020) [1080p] [BluRay] [YTS.MX]",
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "expected_fields": [
                {
                    "hash": "validfile123456789012345678901234567890123",
                    "original_path": "Valid File Movie (2020) [1080p] [BluRay] [YTS.MX]",
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Single item with None filename gets error",
            "input_media_data": [
                {
                    "hash": "nullfile123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Null File Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "downloaded_media_items": {
                "nullfile123456789012345678901234567890123": {
                    "id": 151,
                    "name": None,
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "expected_fields": [
                {
                    "hash": "nullfile123456789012345678901234567890123",
                    "original_path": None,
                    "error_condition": "media_item.name returned None object"
                }
            ]
        },
        {
            "description": "Single item with empty string filename gets error",
            "input_media_data": [
                {
                    "hash": "emptyfile12345678901234567890123456789012",
                    "media_type": "movie",
                    "media_title": "Empty File Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "downloaded_media_items": {
                "emptyfile12345678901234567890123456789012": {
                    "id": 152,
                    "name": "",
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "expected_fields": [
                {
                    "hash": "emptyfile12345678901234567890123456789012",
                    "original_path": "",
                    "error_condition": "media_item.name returned empty string"
                }
            ]
        },
        {
            "description": "Single item with hash as filename gets error",
            "input_media_data": [
                {
                    "hash": "hashfile123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Hash File Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "downloaded_media_items": {
                "hashfile123456789012345678901234567890123": {
                    "id": 153,
                    "name": "hashfile123456789012345678901234567890123",
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "expected_fields": [
                {
                    "hash": "hashfile123456789012345678901234567890123",
                    "original_path": "hashfile123456789012345678901234567890123",
                    "error_condition": "media_item.name contains item hash not filename"
                }
            ]
        },
        {
            "description": "Multiple items with mixed filename validity",
            "input_media_data": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Mixed Valid Movie",
                    "release_year": 2021,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "Mixed Invalid Show",
                    "season": 1,
                    "episode": 5,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "media_type": "tv_season",
                    "media_title": "Mixed Hash Season",
                    "season": 2,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "error_status": False
                }
            ],
            "downloaded_media_items": {
                "mixed1234567890123456789012345678901234567890": {
                    "id": 154,
                    "name": "Mixed Valid Movie (2021) [1080p] [BluRay]",
                    "progress": 100.0,
                    "status": "seeding"
                },
                "mixed2345678901234567890123456789012345678901": {
                    "id": 155,
                    "name": None,
                    "progress": 100.0,
                    "status": "seeding"
                },
                "mixed3456789012345678901234567890123456789012": {
                    "id": 156,
                    "name": "mixed3456789012345678901234567890123456789012",
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "expected_fields": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "original_path": "Mixed Valid Movie (2021) [1080p] [BluRay]",
                    "error_condition": None
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "original_path": None,
                    "error_condition": "media_item.name returned None object"
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "original_path": "mixed3456789012345678901234567890123456789012",
                    "error_condition": "media_item.name contains item hash not filename"
                }
            ]
        },
        {
            "description": "All items with valid filenames",
            "input_media_data": [
                {
                    "hash": "allvalid1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "All Valid Movie 1",
                    "release_year": 2022,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "allvalid2345678901234567890123456789012345",
                    "media_type": "tv_show",
                    "media_title": "All Valid Show",
                    "season": 4,
                    "episode": 12,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "error_status": False
                }
            ],
            "downloaded_media_items": {
                "allvalid1234567890123456789012345678901234": {
                    "id": 158,
                    "name": "All Valid Movie 1 (2022) [1080p] [WEB-DL]",
                    "progress": 100.0,
                    "status": "seeding"
                },
                "allvalid2345678901234567890123456789012345": {
                    "id": 159,
                    "name": "All.Valid.Show.S04E12.1080p.WEB-DL.H264",
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "expected_fields": [
                {
                    "hash": "allvalid1234567890123456789012345678901234",
                    "original_path": "All Valid Movie 1 (2022) [1080p] [WEB-DL]",
                    "error_condition": None
                },
                {
                    "hash": "allvalid2345678901234567890123456789012345",
                    "original_path": "All.Valid.Show.S04E12.1080p.WEB-DL.H264",
                    "error_condition": None
                }
            ]
        },
        {
            "description": "All items with invalid filenames",
            "input_media_data": [
                {
                    "hash": "allinvalid123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "All Invalid Movie 1",
                    "release_year": 2023,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "allinvalid234567890123456789012345678901",
                    "media_type": "tv_season",
                    "media_title": "All Invalid Season",
                    "season": 5,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "downloaded_media_items": {
                "allinvalid123456789012345678901234567890": {
                    "id": 160,
                    "name": "",
                    "progress": 100.0,
                    "status": "seeding"
                },
                "allinvalid234567890123456789012345678901": {
                    "id": 161,
                    "name": None,
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "expected_fields": [
                {
                    "hash": "allinvalid123456789012345678901234567890",
                    "original_path": "",
                    "error_condition": "media_item.name returned empty string"
                },
                {
                    "hash": "allinvalid234567890123456789012345678901",
                    "original_path": None,
                    "error_condition": "media_item.name returned None object"
                }
            ]
        }
    ]

@pytest.fixture
def update_status_cases():
    """Test scenarios for update_status function."""
    return [
        {
            "description": "Item with error 'not found within transmission' gets re-ingested",
            "input_data": [
                {
                    "hash": "notfound123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Not Found Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "not found within transmission"
                }
            ],
            "expected_fields": [
                {
                    "hash": "notfound123456789012345678901234567890123",
                    "pipeline_status": "ingested",
                    "rejection_status": "unfiltered",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Item with no errors and not rejected gets downloaded status",
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
            "expected_fields": [
                {
                    "hash": "completed123456789012345678901234567890123",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Item with rejected status gets rejected pipeline status",
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
            "expected_fields": [
                {
                    "hash": "rejected123456789012345678901234567890123",
                    "pipeline_status": "downloading"
                }
            ]
        },
        {
            "description": "Item with override status and no errors gets downloaded status",
            "input_data": [
                {
                    "hash": "override123456789012345678901234567890123",
                    "media_type": "tv_show",
                    "media_title": "Override Show",
                    "season": 1,
                    "episode": 5,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "override123456789012345678901234567890123",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Item with filename error but not transmission error keeps downloading status",
            "input_data": [
                {
                    "hash": "filenameerror123456789012345678901234567",
                    "media_type": "movie",
                    "media_title": "Filename Error Movie",
                    "release_year": 2020,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "media_item.name returned None object"
                }
            ],
            "expected_fields": [
                {
                    "hash": "filenameerror123456789012345678901234567",
                    "pipeline_status": "downloading"
                }
            ]
        },
        {
            "description": "Multiple items with mixed statuses and re-ingestion logic",
            "input_data": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Mixed Success Movie",
                    "release_year": 2021,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "Mixed Missing Show",
                    "season": 2,
                    "episode": 8,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "not found within transmission"
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "media_type": "tv_season",
                    "media_title": "Mixed Override Season",
                    "season": 3,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "pipeline_status": "downloaded"
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "pipeline_status": "ingested",
                    "rejection_status": "unfiltered",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "All items with transmission errors get re-ingested",
            "input_data": [
                {
                    "hash": "allerrors1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Error Movie 1",
                    "release_year": 2022,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "not found within transmission"
                },
                {
                    "hash": "allerrors2345678901234567890123456789012345",
                    "media_type": "tv_show",
                    "media_title": "Error Show",
                    "season": 4,
                    "episode": 15,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "not found within transmission"
                }
            ],
            "expected_fields": [
                {
                    "hash": "allerrors1234567890123456789012345678901234",
                    "pipeline_status": "ingested",
                    "rejection_status": "unfiltered",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "allerrors2345678901234567890123456789012345",
                    "pipeline_status": "ingested",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ]
        },
        {
            "description": "All items with no errors get downloaded status",
            "input_data": [
                {
                    "hash": "allsuccess1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Success Movie 1",
                    "release_year": 2023,
                    "pipeline_status": "downloading",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "allsuccess2345678901234567890123456789012345",
                    "media_type": "tv_season",
                    "media_title": "Success Season",
                    "season": 5,
                    "pipeline_status": "downloading",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "allsuccess1234567890123456789012345678901234",
                    "pipeline_status": "downloaded"
                },
                {
                    "hash": "allsuccess2345678901234567890123456789012345",
                    "pipeline_status": "downloaded"
                }
            ]
        },
        {
            "description": "Items with different starting pipeline statuses but same outcome",
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