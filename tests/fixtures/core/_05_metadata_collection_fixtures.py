from decimal import Decimal
import pytest
from src.data_models import *

import pytest
import polars as pl
from decimal import Decimal

@pytest.fixture
def process_media_with_existing_metadata_cases():
    """Test scenarios for process_media_with_existing_metadata function."""
    return [
        {
            "description": "Single item with matching tmdb_id gets metadata attached",
            "input_media_data": [
                {
                    "hash": "0a397fcb9f8774e03f861b9c33521b6367410f1b",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 241554
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 241554,
                    "imdb_id": "tt30444310",
                    "media_title": "Murderbot",
                    "release_year": 2025,
                    "budget": None,
                    "revenue": None,
                    "genre": ["Comedy", "Drama", "Sci-Fi & Fantasy"],
                    "tmdb_rating": 7.3,
                    "tmdb_votes": 67,
                    "overview": "In a high-tech future, a rogue security robot secretly gains free will."
                }
            ],
            "expected_fields": [
                {
                    "hash": "0a397fcb9f8774e03f861b9c33521b6367410f1b",
                    "tmdb_id": 241554,
                    "imdb_id": "tt30444310",
                    "media_title": "Murderbot",
                    "release_year": 2025,
                    "tmdb_rating": 7.3
                }
            ]
        },
        {
            "description": "Single item with non-matching tmdb_id gets filtered out",
            "input_media_data": [
                {
                    "hash": "nomatch123456789012345678901234567890123456",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 999999
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 241554,
                    "imdb_id": "tt30444310",
                    "media_title": "Murderbot",
                    "release_year": 2025,
                    "budget": None,
                    "revenue": None,
                    "genre": ["Comedy", "Drama", "Sci-Fi & Fantasy"]
                }
            ],
            "expected_fields": []
        },
        {
            "description": "Multiple items with mixed matching tmdb_ids",
            "input_media_data": [
                {
                    "hash": "match1234567890123456789012345678901234567890",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 125935
                },
                {
                    "hash": "nomatch123456789012345678901234567890123456",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 888888
                },
                {
                    "hash": "match2345678901234567890123456789012345678901",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 1244944
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 125935,
                    "imdb_id": "tt14218830",
                    "media_title": "Abbott Elementary",
                    "release_year": 2021,
                    "tmdb_rating": Decimal("7.450"),
                    "genre": ["Comedy"]
                },
                {
                    "tmdb_id": 1244944,
                    "imdb_id": "tt31314296",
                    "media_title": "The Woman in the Yard",
                    "release_year": 2025,
                    "budget": 12000000,
                    "revenue": 23307302,
                    "genre": ["Horror", "Mystery"]
                }
            ],
            "expected_fields": [
                {
                    "hash": "match1234567890123456789012345678901234567890",
                    "tmdb_id": 125935,
                    "imdb_id": "tt14218830",
                    "media_title": "Abbott Elementary"
                },
                {
                    "hash": "match2345678901234567890123456789012345678901",
                    "tmdb_id": 1244944,
                    "imdb_id": "tt31314296",
                    "media_title": "The Woman in the Yard"
                }
            ]
        },
        {
            "description": "Multiple items with same tmdb_id get same metadata",
            "input_media_data": [
                {
                    "hash": "same1234567890123456789012345678901234567890",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 83867
                },
                {
                    "hash": "same2345678901234567890123456789012345678901",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "override",
                    "error_status": False,
                    "tmdb_id": 83867
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 83867,
                    "imdb_id": "tt9253284",
                    "media_title": "Andor",
                    "release_year": 2022,
                    "tmdb_rating": Decimal("8.248"),
                    "tmdb_votes": 1518,
                    "genre": ["Sci-Fi & Fantasy", "Action & Adventure", "Drama"]
                }
            ],
            "expected_fields": [
                {
                    "hash": "same1234567890123456789012345678901234567890",
                    "tmdb_id": 83867,
                    "imdb_id": "tt9253284",
                    "media_title": "Andor"
                },
                {
                    "hash": "same2345678901234567890123456789012345678901",
                    "tmdb_id": 83867,
                    "imdb_id": "tt9253284",
                    "media_title": "Andor"
                }
            ]
        },
        {
            "description": "All items match existing metadata",
            "input_media_data": [
                {
                    "hash": "allmatch1234567890123456789012345678901234",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 60694
                },
                {
                    "hash": "allmatch2345678901234567890123456789012345",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 100088
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 60694,
                    "imdb_id": "tt3530232",
                    "media_title": "Last Week Tonight with John Oliver",
                    "release_year": 2014,
                    "genre": ["Talk", "Comedy", "News"]
                },
                {
                    "tmdb_id": 100088,
                    "imdb_id": "tt3581920",
                    "media_title": "The Last of Us",
                    "release_year": 2023,
                    "tmdb_rating": 8.554,
                    "genre": ["Drama"]
                }
            ],
            "expected_fields": [
                {
                    "hash": "allmatch1234567890123456789012345678901234",
                    "tmdb_id": 60694,
                    "imdb_id": "tt3530232",
                    "media_title": "Last Week Tonight with John Oliver"
                },
                {
                    "hash": "allmatch2345678901234567890123456789012345",
                    "tmdb_id": 100088,
                    "imdb_id": "tt3581920",
                    "media_title": "The Last of Us"
                }
            ]
        },
        {
            "description": "No items match existing metadata",
            "input_media_data": [
                {
                    "hash": "nomatch1234567890123456789012345678901234",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 777777
                },
                {
                    "hash": "nomatch2345678901234567890123456789012345",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 888888
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 60694,
                    "imdb_id": "tt3530232",
                    "media_title": "Last Week Tonight with John Oliver",
                    "release_year": 2014
                },
                {
                    "tmdb_id": 100088,
                    "imdb_id": "tt3581920",
                    "media_title": "The Last of Us",
                    "release_year": 2023
                }
            ],
            "expected_fields": []
        },
        {
            "description": "Movie and TV show with different metadata fields",
            "input_media_data": [
                {
                    "hash": "movie1234567890123456789012345678901234567890",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 814776
                },
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 1667
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 814776,
                    "imdb_id": "tt17527468",
                    "media_title": "Bottoms",
                    "release_year": 2023,
                    "budget": 11300000,
                    "revenue": 12976079,
                    "runtime": 91,
                    "genre": ["Comedy"]
                },
                {
                    "tmdb_id": 1667,
                    "imdb_id": "tt0072562",
                    "media_title": "Saturday Night Live",
                    "release_year": 1975,
                    "budget": None,
                    "revenue": None,
                    "runtime": None,
                    "genre": ["Comedy", "News"]
                }
            ],
            "expected_fields": [
                {
                    "hash": "movie1234567890123456789012345678901234567890",
                    "tmdb_id": 814776,
                    "imdb_id": "tt17527468",
                    "media_title": "Bottoms",
                    "budget": 11300000,
                    "revenue": 12976079,
                    "runtime": 91
                },
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "tmdb_id": 1667,
                    "imdb_id": "tt0072562",
                    "media_title": "Saturday Night Live",
                    "budget": None,
                    "revenue": None,
                    "runtime": None
                }
            ]
        },
        {
            "description": "Items with partial metadata fields populated",
            "input_media_data": [
                {
                    "hash": "partial123456789012345678901234567890123456",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 38705,
                    "media_title": "Old Title"  # This should get overwritten
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 38705,
                    "imdb_id": "tt0011071",
                    "media_title": "Convict 13",
                    "release_year": 1920,
                    "budget": 0,
                    "revenue": 0,
                    "runtime": 22
                }
            ],
            "expected_fields": [
                {
                    "hash": "partial123456789012345678901234567890123456",
                    "tmdb_id": 38705,
                    "imdb_id": "tt0011071",
                    "media_title": "Convict 13",  # Should be updated from metadata
                    "release_year": 1920,
                    "budget": 0,
                    "revenue": 0,
                    "runtime": 22
                }
            ]
        }
    ]


@pytest.fixture
def update_status_cases():
    """Test scenarios for update_status function."""
    return [
        {
            "description": "Single item with accepted status and no errors gets metadata_collected",
            "input_data": [
                {
                    "hash": "0a397fcb9f8774e03f861b9c33521b6367410f1b",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "0a397fcb9f8774e03f861b9c33521b6367410f1b",
                    "pipeline_status": "metadata_collected"
                }
            ]
        },
        {
            "description": "Single item with override status and no errors gets metadata_collected",
            "input_data": [
                {
                    "hash": "1711fd6fc62082c3430f584eed8d5579a1ce03a6",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "1711fd6fc62082c3430f584eed8d5579a1ce03a6",
                    "pipeline_status": "metadata_collected"
                }
            ]
        },
        {
            "description": "Single item with rejected status gets rejected pipeline status",
            "input_data": [
                {
                    "hash": "073af66920011582fec201cb6843ef46a51daa02",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "073af66920011582fec201cb6843ef46a51daa02",
                    "pipeline_status": "rejected"
                }
            ]
        },
        {
            "description": "Single item with error status keeps original pipeline status",
            "input_data": [
                {
                    "hash": "0c5ba8200b9f15d01002b10bcfe43a5b85bd2902",
                    "pipeline_status": "ingested",
                    "rejection_status": "unfiltered",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "media_title is null; season is null; episode is null"
                }
            ],
            "expected_fields": [
                {
                    "hash": "0c5ba8200b9f15d01002b10bcfe43a5b85bd2902",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "Item with accepted status but with error keeps original status",
            "input_data": [
                {
                    "hash": "erroraccepted123456789012345678901234567890",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "API connection failed"
                }
            ],
            "expected_fields": [
                {
                    "hash": "erroraccepted123456789012345678901234567890",
                    "pipeline_status": "file_accepted"
                }
            ]
        },
        {
            "description": "Item with override status but with error keeps original status",
            "input_data": [
                {
                    "hash": "erroroverride123456789012345678901234567890",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "metadata collection timeout"
                }
            ],
            "expected_fields": [
                {
                    "hash": "erroroverride123456789012345678901234567890",
                    "pipeline_status": "file_accepted"
                }
            ]
        },
        {
            "description": "Multiple items with mixed acceptance statuses and no errors",
            "input_data": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "rejected",
                    "rejection_reason": "media search failed",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "pipeline_status": "metadata_collected"
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "pipeline_status": "metadata_collected"
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
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "mixederror2345678901234567890123456789012345",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "TMDB API rate limit exceeded"
                },
                {
                    "hash": "mixederror3456789012345678901234567890123456",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 2160p is not in allowed_values",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_fields": [
                {
                    "hash": "mixederror1234567890123456789012345678901234",
                    "pipeline_status": "metadata_collected"
                },
                {
                    "hash": "mixederror2345678901234567890123456789012345",
                    "pipeline_status": "file_accepted"
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
                    "pipeline_status": "file_accepted",
                    "rejection_status": "rejected",
                    "rejection_reason": "resolution 720p is not in allowed_values",
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "allrejected2345678901234567890123456789012",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "rejected",
                    "rejection_reason": "media search failed",
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
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "OMDb API connection failed"
                },
                {
                    "hash": "allerrors2345678901234567890123456789012345",
                    "pipeline_status": "parsed",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": True,
                    "error_condition": "TMDB search timeout"
                }
            ],
            "expected_fields": [
                {
                    "hash": "allerrors1234567890123456789012345678901234",
                    "pipeline_status": "file_accepted"
                },
                {
                    "hash": "allerrors2345678901234567890123456789012345",
                    "pipeline_status": "parsed"
                }
            ]
        }
    ]