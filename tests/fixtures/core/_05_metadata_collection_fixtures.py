from decimal import Decimal
import pytest
from src.data_models import *
import polars as pl

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
                    "tmdb_id": 241554,
                    "media_type": "tv_show",
                    "media_title": "Murderbot"
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 241554,
                    "imdb_id": "tt30444310",
                    "media_title": "Murderbot",
                    "media_type": "tv_show",
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
                    "media_type": "tv_show",
                    "release_year": 2025
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
                    "tmdb_id": 999999,
                    "media_type": "movie",
                    "media_title": "Unknown Movie"
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 241554,
                    "imdb_id": "tt30444310",
                    "media_title": "Murderbot",
                    "media_type": "tv_show",
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
                    "tmdb_id": 125935,
                    "media_type": "tv_show",
                    "media_title": "Abbott Elementary"
                },
                {
                    "hash": "nomatch123456789012345678901234567890123456",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 888888,
                    "media_type": "movie",
                    "media_title": "Unknown Movie 2"
                },
                {
                    "hash": "match2345678901234567890123456789012345678901",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 1244944,
                    "media_type": "movie",
                    "media_title": "The Woman in the Yard"
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 125935,
                    "imdb_id": "tt14218830",
                    "media_title": "Abbott Elementary",
                    "media_type": "tv_show",
                    "release_year": 2021,
                    "tmdb_rating": Decimal("7.450"),
                    "genre": ["Comedy"]
                },
                {
                    "tmdb_id": 1244944,
                    "imdb_id": "tt31314296",
                    "media_title": "The Woman in the Yard",
                    "media_type": "movie",
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
                    "media_title": "Abbott Elementary",
                    "media_type": "tv_show"
                },
                {
                    "hash": "match2345678901234567890123456789012345678901",
                    "tmdb_id": 1244944,
                    "imdb_id": "tt31314296",
                    "media_title": "The Woman in the Yard",
                    "media_type": "movie"
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
                    "tmdb_id": 83867,
                    "media_type": "tv_show",
                    "media_title": "Andor"
                },
                {
                    "hash": "same2345678901234567890123456789012345678901",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "override",
                    "error_status": False,
                    "tmdb_id": 83867,
                    "media_type": "tv_show",
                    "media_title": "Andor"
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 83867,
                    "imdb_id": "tt9253284",
                    "media_title": "Andor",
                    "media_type": "tv_show",
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
                    "media_title": "Andor",
                    "media_type": "tv_show"
                },
                {
                    "hash": "same2345678901234567890123456789012345678901",
                    "tmdb_id": 83867,
                    "imdb_id": "tt9253284",
                    "media_title": "Andor",
                    "media_type": "tv_show"
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
                    "tmdb_id": 60694,
                    "media_type": "tv_show",
                    "media_title": "Last Week Tonight with John Oliver"
                },
                {
                    "hash": "allmatch2345678901234567890123456789012345",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 100088,
                    "media_type": "tv_show",
                    "media_title": "The Last of Us"
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 60694,
                    "imdb_id": "tt3530232",
                    "media_title": "Last Week Tonight with John Oliver",
                    "media_type": "tv_show",
                    "release_year": 2014,
                    "genre": ["Talk", "Comedy", "News"]
                },
                {
                    "tmdb_id": 100088,
                    "imdb_id": "tt3581920",
                    "media_title": "The Last of Us",
                    "media_type": "tv_show",
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
                    "media_title": "Last Week Tonight with John Oliver",
                    "media_type": "tv_show"
                },
                {
                    "hash": "allmatch2345678901234567890123456789012345",
                    "tmdb_id": 100088,
                    "imdb_id": "tt3581920",
                    "media_title": "The Last of Us",
                    "media_type": "tv_show"
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
                    "tmdb_id": 777777,
                    "media_type": "movie",
                    "media_title": "Unknown Movie 1"
                },
                {
                    "hash": "nomatch2345678901234567890123456789012345",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 888888,
                    "media_type": "movie",
                    "media_title": "Unknown Movie 2"
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 60694,
                    "imdb_id": "tt3530232",
                    "media_title": "Last Week Tonight with John Oliver",
                    "media_type": "tv_show",
                    "release_year": 2014
                },
                {
                    "tmdb_id": 100088,
                    "imdb_id": "tt3581920",
                    "media_title": "The Last of Us",
                    "media_type": "tv_show",
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
                    "tmdb_id": 814776,
                    "media_type": "movie",
                    "media_title": "Bottoms"
                },
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "tmdb_id": 1667,
                    "media_type": "tv_show",
                    "media_title": "Saturday Night Live"
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 814776,
                    "imdb_id": "tt17527468",
                    "media_title": "Bottoms",
                    "media_type": "movie",
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
                    "media_type": "tv_show",
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
                    "media_type": "movie"
                },
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "tmdb_id": 1667,
                    "imdb_id": "tt0072562",
                    "media_title": "Saturday Night Live",
                    "media_type": "tv_show"
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
                    "media_type": "movie",
                    "media_title": "Old Title"  # This should be retained
                }
            ],
            "existing_metadata_data": [
                {
                    "tmdb_id": 38705,
                    "imdb_id": "tt0011071",
                    "media_title": "Convict 13",
                    "media_type": "movie",
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
                    "media_title": "Old Title",  # Should be retained from input
                    "media_type": "movie",
                    "release_year": 1920
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


@pytest.fixture
def build_training_records_cases():
    """Test scenarios for build_training_records function."""
    return [
        {
            "description": "Single item with valid imdb_id produces training record",
            "input_data": [
                {
                    "hash": "training1234567890123456789012345678901234",
                    "imdb_id": "tt1234567",
                    "tmdb_id": 12345,
                    "media_type": "movie",
                    "media_title": "Test Movie",
                    "release_year": 2023,
                    "budget": 1000000,
                    "revenue": 5000000,
                    "runtime": 120,
                    "genre": ["Action", "Drama"],
                    "overview": "A test movie overview",
                    "tmdb_rating": 7.5,
                    "tmdb_votes": 1000,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_count": 1,
            "expected_fields": [
                {
                    "imdb_id": "tt1234567",
                    "tmdb_id": 12345,
                    "media_type": "movie",
                    "media_title": "Test Movie",
                    "release_year": 2023,
                    "label": None,
                    "human_labeled": False,
                    "anomalous": False,
                    "reviewed": False
                }
            ]
        },
        {
            "description": "Item without imdb_id is filtered out",
            "input_data": [
                {
                    "hash": "noimdb12345678901234567890123456789012345",
                    "imdb_id": None,
                    "tmdb_id": 99999,
                    "media_type": "movie",
                    "media_title": "No IMDB Movie",
                    "release_year": 2023,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_count": 0,
            "expected_fields": []
        },
        {
            "description": "Rejected item is filtered out",
            "input_data": [
                {
                    "hash": "rejected123456789012345678901234567890123",
                    "imdb_id": "tt7654321",
                    "tmdb_id": 54321,
                    "media_type": "movie",
                    "media_title": "Rejected Movie",
                    "release_year": 2023,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "rejected",
                    "rejection_reason": "low quality",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_count": 0,
            "expected_fields": []
        },
        {
            "description": "Item with error is filtered out",
            "input_data": [
                {
                    "hash": "error12345678901234567890123456789012345678",
                    "imdb_id": "tt9999999",
                    "tmdb_id": 11111,
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "release_year": 2023,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": True,
                    "error_condition": "API timeout"
                }
            ],
            "expected_count": 0,
            "expected_fields": []
        },
        {
            "description": "Multiple items with same imdb_id are deduplicated",
            "input_data": [
                {
                    "hash": "dupe11234567890123456789012345678901234567",
                    "imdb_id": "tt1111111",
                    "tmdb_id": 11111,
                    "media_type": "movie",
                    "media_title": "Duplicate Movie 1",
                    "release_year": 2023,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "dupe21234567890123456789012345678901234567",
                    "imdb_id": "tt1111111",
                    "tmdb_id": 11111,
                    "media_type": "movie",
                    "media_title": "Duplicate Movie 2",
                    "release_year": 2023,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "override",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_count": 1,
            "expected_fields": [
                {
                    "imdb_id": "tt1111111",
                    "media_title": "Duplicate Movie 1"
                }
            ]
        },
        {
            "description": "Mixed valid and invalid items - only valid produce training records",
            "input_data": [
                {
                    "hash": "valid1234567890123456789012345678901234567",
                    "imdb_id": "tt2222222",
                    "tmdb_id": 22222,
                    "media_type": "tv_show",
                    "media_title": "Valid TV Show",
                    "release_year": 2022,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "noimdb2345678901234567890123456789012345",
                    "imdb_id": None,
                    "tmdb_id": 33333,
                    "media_type": "movie",
                    "media_title": "No IMDB",
                    "release_year": 2023,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "rejected2345678901234567890123456789012345",
                    "imdb_id": "tt4444444",
                    "tmdb_id": 44444,
                    "media_type": "movie",
                    "media_title": "Rejected",
                    "release_year": 2023,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "rejected",
                    "rejection_reason": "bad quality",
                    "error_status": False,
                    "error_condition": None
                },
                {
                    "hash": "valid2345678901234567890123456789012345678",
                    "imdb_id": "tt5555555",
                    "tmdb_id": 55555,
                    "media_type": "movie",
                    "media_title": "Valid Movie",
                    "release_year": 2021,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "override",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_count": 2,
            "expected_fields": [
                {
                    "imdb_id": "tt2222222",
                    "media_title": "Valid TV Show",
                    "media_type": "tv_show"
                },
                {
                    "imdb_id": "tt5555555",
                    "media_title": "Valid Movie",
                    "media_type": "movie"
                }
            ]
        },
        {
            "description": "Empty input returns empty DataFrame",
            "input_data": [],
            "expected_count": 0,
            "expected_fields": []
        },
        {
            "description": "TV show with season and episode produces training record",
            "input_data": [
                {
                    "hash": "tvshow12345678901234567890123456789012345",
                    "imdb_id": "tt6666666",
                    "tmdb_id": 66666,
                    "media_type": "tv_season",
                    "media_title": "Test TV Show",
                    "season": 2,
                    "episode": None,
                    "release_year": 2020,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "error_status": False,
                    "error_condition": None
                }
            ],
            "expected_count": 1,
            "expected_fields": [
                {
                    "imdb_id": "tt6666666",
                    "media_type": "tv_season",
                    "season": 2,
                    "label": None
                }
            ]
        }
    ]