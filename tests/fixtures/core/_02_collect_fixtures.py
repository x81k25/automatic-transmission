import pytest
import polars as pl
from src.data_models import *

@pytest.fixture
def process_new_items_cases():
    """Test scenarios for process_new_items function."""
    return [
        {
            "description": "Single movie item with valid hash and title",
            "input": {
                "08105069ee5816d68259486376a58a4846e69eb4": {
                    "name": "Paranormal Activity 2 (2010) [REPACK] [1080p] [BluRay] [5.1] [YTS.MX]"
                }
            },
            "expected_fields": [
                {
                    "hash": "08105069ee5816d68259486376a58a4846e69eb4",
                    "original_title": "Paranormal Activity 2 (2010) [REPACK] [1080p] [BluRay] [5.1] [YTS.MX]",
                    "media_type": "movie",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Single TV show item with season and episode",
            "input": {
                "1e22f5f51514c2761480edf0f10e12f3ea523bb1": {
                    "name": "Abbott Elementary S04E04 Costume Contest 1080p DSNP WEB-DL DD 5 1 H 264-playWEB[EZTVx.to].mkv"
                }
            },
            "expected_fields": [
                {
                    "hash": "1e22f5f51514c2761480edf0f10e12f3ea523bb1",
                    "original_title": "Abbott Elementary S04E04 Costume Contest 1080p DSNP WEB-DL DD 5 1 H 264-playWEB[EZTVx.to].mkv",
                    "media_type": "tv_show",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Single TV season item with season pattern",
            "input": {
                "276b69e62b2bbb848e0290525f74048ecf78df0b": {
                    "name": "The.Studio.S01.1080p.ATVP.WEB-DL.ITA-ENG.DD5.1.H.264-G66"
                }
            },
            "expected_fields": [
                {
                    "hash": "276b69e62b2bbb848e0290525f74048ecf78df0b",
                    "original_title": "The.Studio.S01.1080p.ATVP.WEB-DL.ITA-ENG.DD5.1.H.264-G66",
                    "media_type": "tv_season",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Item with UIndex prefix that gets cleaned",
            "input": {
                "24b812e34cf5440c4de952245b676becfb1db2c7": {
                    "name": "www.UIndex.org    -    Star Wars Andor S02E06 What a Festive Evening 1080p DSNP WEB-DL DDP5 1 H 264-NTb"
                }
            },
            "expected_fields": [
                {
                    "hash": "24b812e34cf5440c4de952245b676becfb1db2c7",
                    "original_title": "www.UIndex.org    -    Star Wars Andor S02E06 What a Festive Evening 1080p DSNP WEB-DL DDP5 1 H 264-NTb",
                    "media_type": "tv_show",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Item with hash-only name (unknown media type)",
            "input": {
                "c944695ad9af1230dad06081fa4262e349391c92": {
                    "name": "c944695ad9af1230dad06081fa4262e349391c92"
                }
            },
            "expected_fields": {}
        },
        {
            "description": "Multiple items with mixed media types",
            "input": {
                "65ce427473cab660b59d4003e36ec58b2f2c7f1c": {
                    "name": "Paranormal Activity 2 (2010) [1080p] [BluRay] [5.1] [YTS.MX]"
                },
                "1711fd6fc62082c3430f584eed8d5579a1ce03a6": {
                    "name": "Nature.S43E13.Hummingbirds.of.Hollywood.1080p.WEB.h264-BAE[EZTVx.to].mkv"
                },
                "69dc2d2a0df4b111a4ed7150e4976611ff5b04e5": {
                    "name": "Andor S02 1080p x265-ELiTE EZTV"
                }
            },
            "expected_fields": [
                {
                    "hash": "65ce427473cab660b59d4003e36ec58b2f2c7f1c",
                    "original_title": "Paranormal Activity 2 (2010) [1080p] [BluRay] [5.1] [YTS.MX]",
                    "media_type": "movie",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                },
                {
                    "hash": "1711fd6fc62082c3430f584eed8d5579a1ce03a6",
                    "original_title": "Nature.S43E13.Hummingbirds.of.Hollywood.1080p.WEB.h264-BAE[EZTVx.to].mkv",
                    "media_type": "tv_show",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                },
                {
                    "hash": "69dc2d2a0df4b111a4ed7150e4976611ff5b04e5",
                    "original_title": "Andor S02 1080p x265-ELiTE EZTV",
                    "media_type": "tv_season",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Item with unextractable title",
            "input": {
                "badextraction123456789012345678901234567890": {
                    "name": "Random.Unclassifiable.String.Without.Patterns.123"
                }
            },
            "expected_fields": [
                {
                    "hash": "badextraction123456789012345678901234567890",
                    "original_title": "Random.Unclassifiable.String.Without.Patterns.123",
                    "media_type": "unknown",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": "media_type is unknown"
                }
            ]
        },
        {
            "description": "Item with empty name field",
            "input": {
                "emptyname12345678901234567890123456789012": {
                    "name": ""
                }
            },
            "expected_fields": [
                {
                    "hash": "emptyname12345678901234567890123456789012",
                    "original_title": "",
                    "media_type": "unknown",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": "media_type is unknown"
                }
            ]
        },
        {
            "description": "item where name == hash for 1 item, and 2 normal items",
            "input": {
                "eabf8c01ebb615f4fcb5f94ff191cb5ef13de906": {
                    "name": "eabf8c01ebb615f4fcb5f94ff191cb5ef13de906"
                },
                "1711fd6fc62082c3430f584eed8d5579a1ce03a6": {
                    "name": "Nature.S43E13.Hummingbirds.of.Hollywood.1080p.WEB.h264-BAE[EZTVx.to].mkv"
                },
                "69dc2d2a0df4b111a4ed7150e4976611ff5b04e5": {
                    "name": "Andor S02 1080p x265-ELiTE EZTV"
                }
            },
            "expected_fields": [
                {
                    "hash": "1711fd6fc62082c3430f584eed8d5579a1ce03a6",
                    "original_title": "Nature.S43E13.Hummingbirds.of.Hollywood.1080p.WEB.h264-BAE[EZTVx.to].mkv",
                    "media_type": "tv_show",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                },
                {
                    "hash": "69dc2d2a0df4b111a4ed7150e4976611ff5b04e5",
                    "original_title": "Andor S02 1080p x265-ELiTE EZTV",
                    "media_type": "tv_season",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "error_condition": None
                }
            ]
        }
    ]

@pytest.fixture
def update_rejected_status_cases():
    """Test scenarios for update_rejected_status function."""
    return [
        {
            "description": "Single item with rejected status gets set to override",
            "input_data": [
                {
                    "hash": "rejected123456789012345678901234567890",
                    "original_title": "Some Rejected Movie (2020) [1080p]",
                    "media_type": "movie",
                    "media_title": "Some Rejected Movie",
                    "rejection_status": "rejected",
                    "rejection_reason": "probability 0.2 below threshold 0.35"
                }
            ],
            "expected_fields": [
                {
                    "hash": "rejected123456789012345678901234567890",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": "probability 0.2 below threshold 0.35"
                }
            ]
        },
        {
            "description": "Single item with accepted status gets set to override",
            "input_data": [
                {
                    "hash": "accepted123456789012345678901234567890",
                    "original_title": "Good Movie (2020) [1080p]",
                    "media_type": "movie",
                    "media_title": "Good Movie",
                    "rejection_status": "accepted",
                    "rejection_reason": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "accepted123456789012345678901234567890",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Single item with override status remains override",
            "input_data": [
                {
                    "hash": "override123456789012345678901234567890",
                    "original_title": "Override Movie (2020) [1080p]",
                    "media_type": "movie",
                    "media_title": "Override Movie",
                    "rejection_status": "override",
                    "rejection_reason": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "override123456789012345678901234567890",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Multiple items with mixed rejection statuses",
            "input_data": [
                {
                    "hash": "multi1234567890123456789012345678901234",
                    "original_title": "Rejected TV Show S01E01",
                    "media_type": "tv_show",
                    "media_title": "Rejected TV Show",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values"
                },
                {
                    "hash": "multi2234567890123456789012345678901234",
                    "original_title": "Accepted Movie (2021) [1080p]",
                    "media_type": "movie",
                    "media_title": "Accepted Movie",
                    "rejection_status": "accepted",
                    "rejection_reason": None
                },
                {
                    "hash": "multi3234567890123456789012345678901234",
                    "original_title": "Override Season S02",
                    "media_type": "tv_season",
                    "media_title": "Override Season",
                    "rejection_status": "override",
                    "rejection_reason": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "multi1234567890123456789012345678901234",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": "resolution 720p is not in allowed_values"
                },
                {
                    "hash": "multi2234567890123456789012345678901234",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": None
                },
                {
                    "hash": "multi3234567890123456789012345678901234",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Item with unfiltered status gets set to override",
            "input_data": [
                {
                    "hash": "unfiltered123456789012345678901234567890",
                    "original_title": "Unfiltered Movie (2020) [1080p]",
                    "media_type": "movie",
                    "media_title": "Unfiltered Movie",
                    "rejection_status": "unfiltered",
                    "rejection_reason": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "unfiltered123456789012345678901234567890",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Item with file filtration rejection reason",
            "input_data": [
                {
                    "hash": "filefilter123456789012345678901234567890",
                    "original_title": "Bad Resolution Movie (2020) [720p]",
                    "media_type": "movie",
                    "media_title": "Bad Resolution Movie",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values"
                }
            ],
            "expected_fields": [
                {
                    "hash": "filefilter123456789012345678901234567890",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": "resolution 720p is not in allowed_values"
                }
            ]
        },
        {
            "description": "Item with metadata search failure rejection reason",
            "input_data": [
                {
                    "hash": "metafail123456789012345678901234567890",
                    "original_title": "Unknown Movie (2020) [1080p]",
                    "media_type": "movie",
                    "media_title": "Unknown Movie",
                    "rejection_status": "rejected",
                    "rejection_reason": "media search failed"
                }
            ],
            "expected_fields": [
                {
                    "hash": "metafail123456789012345678901234567890",
                    "rejection_status": RejectionStatus.OVERRIDE,
                    "rejection_reason": "media search failed"
                }
            ]
        }
    ]


@pytest.fixture
def collect_media_workflow_cases():
    """Test scenarios for collect_media workflow integration."""
    return [
        {
            "description": "No transmission items - early return",
            "current_transmission_items": None,
            "expected_insert_calls": 0,
            "expected_update_calls": 0
        },
        {
            "description": "Empty transmission items - early return",
            "current_transmission_items": {},
            "new_hashes": [],
            "rejected_hashes": [],
            "expected_insert_calls": 0,
            "expected_update_calls": 0
        },
        {
            "description": "Single new item - insert only",
            "current_transmission_items": {
                "newhash123456789012345678901234567890123": {
                    "id": 1,
                    "name": "New Movie (2024) [1080p] [BluRay]",
                    "progress": 50.0,
                    "status": "downloading"
                }
            },
            "new_hashes": ["newhash123456789012345678901234567890123"],
            "rejected_hashes": [],
            "expected_insert_calls": 1,
            "expected_update_calls": 0,
            "expected_insert_items": [
                {
                    "hash": "newhash123456789012345678901234567890123",
                    "original_title": "New Movie (2024) [1080p] [BluRay]",
                    "media_type": "movie",
                    "rejection_status": "override",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "Single rejected item - update only",
            "current_transmission_items": {
                "rejectedhash123456789012345678901234567890": {
                    "id": 2,
                    "name": "Rejected Movie (2023) [720p]",
                    "progress": 75.0,
                    "status": "downloading"
                }
            },
            "new_hashes": [],
            "rejected_hashes": ["rejectedhash123456789012345678901234567890"],
            "rejected_media": [
                {
                    "hash": "rejectedhash123456789012345678901234567890",
                    "original_title": "Rejected Movie (2023) [720p]",
                    "media_type": "movie",
                    "media_title": "Rejected Movie",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values",
                    "pipeline_status": "media_rejected"
                }
            ],
            "expected_insert_calls": 0,
            "expected_update_calls": 1,
            "expected_update_items": [
                {
                    "hash": "rejectedhash123456789012345678901234567890",
                    "rejection_status": "override"
                }
            ]
        },
        {
            "description": "Mixed new and rejected items",
            "current_transmission_items": {
                "newhash234567890123456789012345678901234": {
                    "id": 3,
                    "name": "New Show S01E01 1080p WEB-DL",
                    "progress": 25.0,
                    "status": "downloading"
                },
                "rejectedhash345678901234567890123456789012": {
                    "id": 4,
                    "name": "Previously Rejected Movie (2022) [720p]",
                    "progress": 90.0,
                    "status": "downloading"
                }
            },
            "new_hashes": ["newhash234567890123456789012345678901234"],
            "rejected_hashes": ["rejectedhash345678901234567890123456789012"],
            "rejected_media": [
                {
                    "hash": "rejectedhash345678901234567890123456789012",
                    "original_title": "Previously Rejected Movie (2022) [720p]",
                    "media_type": "movie",
                    "media_title": "Previously Rejected Movie",
                    "rejection_status": "rejected",
                    "rejection_reason": "probability 0.1 below threshold 0.35",
                    "pipeline_status": "media_rejected"
                }
            ],
            "expected_insert_calls": 1,
            "expected_update_calls": 1,
            "expected_insert_items": [
                {
                    "hash": "newhash234567890123456789012345678901234",
                    "original_title": "New Show S01E01 1080p WEB-DL",
                    "media_type": "tv_show",
                    "rejection_status": "override",
                    "pipeline_status": "ingested"
                }
            ],
            "expected_update_items": [
                {
                    "hash": "rejectedhash345678901234567890123456789012",
                    "rejection_status": "override"
                }
            ]
        },
        {
            "description": "Multiple new items with hash-only name filtered out",
            "current_transmission_items": {
                "goodhash123456789012345678901234567890123": {
                    "id": 5,
                    "name": "Good Movie (2024) [1080p]",
                    "progress": 10.0,
                    "status": "downloading"
                },
                "badhash234567890123456789012345678901234": {
                    "id": 6,
                    "name": "badhash234567890123456789012345678901234",
                    "progress": 20.0,
                    "status": "downloading"
                },
                "tvhash345678901234567890123456789012345": {
                    "id": 7,
                    "name": "TV Season S02 Complete 1080p",
                    "progress": 30.0,
                    "status": "downloading"
                }
            },
            "new_hashes": [
                "goodhash123456789012345678901234567890123",
                "badhash234567890123456789012345678901234",
                "tvhash345678901234567890123456789012345"
            ],
            "rejected_hashes": [],
            "expected_insert_calls": 1,
            "expected_update_calls": 0,
            "expected_insert_items": [
                {
                    "hash": "goodhash123456789012345678901234567890123",
                    "original_title": "Good Movie (2024) [1080p]",
                    "media_type": "movie",
                    "rejection_status": "override",
                    "pipeline_status": "ingested"
                },
                {
                    "hash": "tvhash345678901234567890123456789012345",
                    "original_title": "TV Season S02 Complete 1080p",
                    "media_type": "tv_season",
                    "rejection_status": "override",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "Item with unknown media type gets error condition",
            "current_transmission_items": {
                "unknownhash123456789012345678901234567890": {
                    "id": 8,
                    "name": "Strange.File.Without.Clear.Type.123",
                    "progress": 60.0,
                    "status": "downloading"
                }
            },
            "new_hashes": ["unknownhash123456789012345678901234567890"],
            "rejected_hashes": [],
            "expected_insert_calls": 1,
            "expected_update_calls": 0,
            "expected_insert_items": [
                {
                    "hash": "unknownhash123456789012345678901234567890",
                    "original_title": "Strange.File.Without.Clear.Type.123",
                    "media_type": "unknown",
                    "rejection_status": "override",
                    "error_condition": "media_type is unknown",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "All items already in database - no new or rejected",
            "current_transmission_items": {
                "existinghash123456789012345678901234567890": {
                    "id": 9,
                    "name": "Existing Movie (2024) [1080p]",
                    "progress": 100.0,
                    "status": "seeding"
                }
            },
            "new_hashes": [],
            "rejected_hashes": [],
            "expected_insert_calls": 0,
            "expected_update_calls": 0
        },
        {
            "description": "Multiple rejected items with different rejection reasons",
            "current_transmission_items": {
                "reject1hash123456789012345678901234567890": {
                    "id": 10,
                    "name": "Low Probability Movie (2023) [1080p]",
                    "progress": 85.0,
                    "status": "downloading"
                },
                "reject2hash234567890123456789012345678901": {
                    "id": 11,
                    "name": "Bad Resolution Show S01E01 720p",
                    "progress": 95.0,
                    "status": "downloading"
                }
            },
            "new_hashes": [],
            "rejected_hashes": [
                "reject1hash123456789012345678901234567890",
                "reject2hash234567890123456789012345678901"
            ],
            "rejected_media": [
                {
                    "hash": "reject1hash123456789012345678901234567890",
                    "original_title": "Low Probability Movie (2023) [1080p]",
                    "media_type": "movie",
                    "media_title": "Low Probability Movie",
                    "rejection_status": "rejected",
                    "rejection_reason": "probability 0.15 below threshold 0.35",
                    "pipeline_status": "media_rejected"
                },
                {
                    "hash": "reject2hash234567890123456789012345678901",
                    "original_title": "Bad Resolution Show S01E01 720p",
                    "media_type": "tv_show",
                    "media_title": "Bad Resolution Show",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values",
                    "pipeline_status": "file_rejected"
                }
            ],
            "expected_insert_calls": 0,
            "expected_update_calls": 1,
            "expected_update_items": [
                {
                    "hash": "reject1hash123456789012345678901234567890",
                    "rejection_status": "override"
                },
                {
                    "hash": "reject2hash234567890123456789012345678901",
                    "rejection_status": "override"
                }
            ]
        }
    ]