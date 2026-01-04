import pytest
import polars as pl
from src.data_models import *

@pytest.fixture
def process_exempt_items_cases():
    """Test scenarios for process_exempt_items function."""
    return [
        {
            "description": "Single TV show gets exempted",
            "input_data": [
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "media_type": "tv_show",
                    "media_title": "Test TV Show",
                    "season": 1,
                    "episode": 5,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_fields": [
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "media_type": "tv_show"
                }
            ]
        },
        {
            "description": "Single TV season gets exempted",
            "input_data": [
                {
                    "hash": "tvseason123456789012345678901234567890123456",
                    "media_type": "tv_season",
                    "media_title": "Test TV Season",
                    "season": 2,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_fields": [
                {
                    "hash": "tvseason123456789012345678901234567890123456",
                    "media_type": "tv_season"
                }
            ]
        },
        {
            "description": "Single movie does not get exempted",
            "input_data": [
                {
                    "hash": "movie1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Test Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_fields": []
        },
        {
            "description": "Mixed media types - only TV content gets exempted",
            "input_data": [
                {
                    "hash": "movie1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Test Movie",
                    "release_year": 2021,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "media_type": "tv_show",
                    "media_title": "Test Show",
                    "season": 3,
                    "episode": 8,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "tvseason123456789012345678901234567890123456",
                    "media_type": "tv_season",
                    "media_title": "Test Season",
                    "season": 1,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_fields": [
                {
                    "hash": "tvshow123456789012345678901234567890123456",
                    "media_type": "tv_show"
                },
                {
                    "hash": "tvseason123456789012345678901234567890123456",
                    "media_type": "tv_season"
                }
            ]
        },
        {
            "description": "Empty input returns empty result",
            "input_data": [],
            "expected_fields": []
        }
    ]


@pytest.fixture
def reject_media_without_imdb_id_cases():
    """Test scenarios for reject_media_without_imdb_id function."""
    return [
        {
            "description": "Single item with null imdb_id gets rejected",
            "input_data": [
                {
                    "hash": "nullimdb123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Movie Without IMDB",
                    "release_year": 2020,
                    "imdb_id": None,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_fields": [
                {
                    "hash": "nullimdb123456789012345678901234567890123",
                    "rejection_reason": "no imdb_id for media filtration"
                }
            ]
        },
        {
            "description": "Single item with valid imdb_id does not get rejected",
            "input_data": [
                {
                    "hash": "validimdb123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Movie With IMDB",
                    "release_year": 2020,
                    "imdb_id": "tt1234567",
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_fields": []
        },
        {
            "description": "Mixed items - only null imdb_id gets rejected",
            "input_data": [
                {
                    "hash": "validimdb123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Valid Movie",
                    "release_year": 2021,
                    "imdb_id": "tt9876543",
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "nullimdb123456789012345678901234567890123",
                    "media_type": "tv_show",
                    "media_title": "Invalid Show",
                    "season": 1,
                    "episode": 1,
                    "imdb_id": None,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_fields": [
                {
                    "hash": "nullimdb123456789012345678901234567890123",
                    "rejection_reason": "no imdb_id for media filtration"
                }
            ]
        },
        {
            "description": "All items with null imdb_id get rejected",
            "input_data": [
                {
                    "hash": "nullimdb1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Movie 1",
                    "release_year": 2022,
                    "imdb_id": None,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "nullimdb2345678901234567890123456789012345",
                    "media_type": "movie",
                    "media_title": "Movie 2",
                    "release_year": 2023,
                    "imdb_id": None,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_fields": [
                {
                    "hash": "nullimdb1234567890123456789012345678901234",
                    "rejection_reason": "no imdb_id for media filtration"
                },
                {
                    "hash": "nullimdb2345678901234567890123456789012345",
                    "rejection_reason": "no imdb_id for media filtration"
                }
            ]
        },
        {
            "description": "Empty input returns empty result",
            "input_data": [],
            "expected_fields": []
        }
    ]


@pytest.fixture
def process_prelabeled_items_cases():
    """Test scenarios for process_prelabeled_items function.

    Note: Only items with anomalous=True are processed (bypass reel-driver).
    Items with anomalous=False go through reel-driver prediction.
    """
    return [
        {
            "description": "Anomalous item with would_not_watch label gets rejected",
            "input_media_data": [
                {
                    "hash": "prefiltered123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Pre-filtered Movie",
                    "release_year": 2020,
                    "imdb_id": "tt34493867",
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "media_labels_data": [
                {"imdb_id": "tt34493867", "label": "would_not_watch", "anomalous": True}
            ],
            "expected_fields": [
                {
                    "hash": "prefiltered123456789012345678901234567890123",
                    "rejection_reason": "anomalous - previously failed reel-driver"
                }
            ]
        },
        {
            "description": "Anomalous item with would_watch label does not get rejected",
            "input_media_data": [
                {
                    "hash": "goodlabel123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Good Movie",
                    "release_year": 2020,
                    "imdb_id": "tt2861424",
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "media_labels_data": [
                {"imdb_id": "tt2861424", "label": "would_watch", "anomalous": True}
            ],
            "expected_fields": [
                {
                    "hash": "goodlabel123456789012345678901234567890123",
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Non-anomalous item returns empty (goes through reel-driver)",
            "input_media_data": [
                {
                    "hash": "nomatch123456789012345678901234567890123456",
                    "media_type": "movie",
                    "media_title": "No Match Movie",
                    "release_year": 2020,
                    "imdb_id": "tt9999999",
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "media_labels_data": [
                {"imdb_id": "tt9999999", "label": "would_watch", "anomalous": False}
            ],
            "expected_fields": []
        },
        {
            "description": "Mixed anomalous items with different labels",
            "input_media_data": [
                {
                    "hash": "wouldwatch123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Would Watch Movie",
                    "release_year": 2021,
                    "imdb_id": "tt30444310",
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "wouldnotwatch12345678901234567890123456789",
                    "media_type": "movie",
                    "media_title": "Would Not Watch Movie",
                    "release_year": 2021,
                    "imdb_id": "tt31314296",
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "media_labels_data": [
                {"imdb_id": "tt30444310", "label": "would_watch", "anomalous": True},
                {"imdb_id": "tt31314296", "label": "would_not_watch", "anomalous": True}
            ],
            "expected_fields": [
                {
                    "hash": "wouldwatch123456789012345678901234567890123",
                    "rejection_reason": None
                },
                {
                    "hash": "wouldnotwatch12345678901234567890123456789",
                    "rejection_reason": "anomalous - previously failed reel-driver"
                }
            ]
        },
        {
            "description": "Empty media labels returns empty result (no anomalous items)",
            "input_media_data": [
                {
                    "hash": "emptylabels123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "No Labels Movie",
                    "release_year": 2020,
                    "imdb_id": "tt1111111",
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "media_labels_data": [
                {"imdb_id": "tt0000000", "label": "would_watch", "anomalous": False}  # Non-matching & non-anomalous
            ],
            "expected_fields": []
        }
    ]


@pytest.fixture
def update_status_cases():
    """Test scenarios for update_status function."""
    return [
        {
            "description": "Single item with probability above threshold gets accepted",
            "input_data": [
                {
                    "hash": "abovethreshold123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "High Probability Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [0.85],
            "expected_fields": [
                {
                    "hash": "abovethreshold123456789012345678901234567890",
                    "rejection_status": "accepted",
                    "pipeline_status": "media_accepted",
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Single item with probability below threshold gets rejected",
            "input_data": [
                {
                    "hash": "belowthreshold123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Low Probability Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [0.25],
            "expected_fields": [
                {
                    "hash": "belowthreshold123456789012345678901234567890",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected",
                    "rejection_reason": "probability 0.25 below threshold 0.35"
                }
            ]
        },
        {
            "description": "Single item with probability exactly at threshold gets accepted",
            "input_data": [
                {
                    "hash": "atthreshold123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Threshold Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [0.35],
            "expected_fields": [
                {
                    "hash": "atthreshold123456789012345678901234567890123",
                    "rejection_status": "accepted",
                    "pipeline_status": "media_accepted",
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Item with override status maintains override regardless of probability",
            "input_data": [
                {
                    "hash": "override123456789012345678901234567890123456",
                    "media_type": "movie",
                    "media_title": "Override Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [0.15],
            "expected_fields": [
                {
                    "hash": "override123456789012345678901234567890123456",
                    "rejection_status": "override",
                    "pipeline_status": "media_accepted",
                    "rejection_reason": "probability 0.15 below threshold 0.35"
                }
            ]
        },
        {
            "description": "Item with existing rejection reason gets rejected",
            "input_data": [
                {
                    "hash": "existingreason123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Already Rejected Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": "no imdb_id for media filtration",
                    "error_status": False
                }
            ],
            "probability_values": [0.75],
            "expected_fields": [
                {
                    "hash": "existingreason123456789012345678901234567890",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected",
                    "rejection_reason": "no imdb_id for media filtration"
                }
            ]
        },
        {
            "description": "Item with error status maintains original pipeline status",
            "input_data": [
                {
                    "hash": "errorstatus123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True,
					"error_condition": "error_condition"
                }
            ],
            "probability_values": [0.85],
            "expected_fields": [
                {
                    "hash": "errorstatus123456789012345678901234567890123",
                    "rejection_status": "accepted",
                    "pipeline_status": "metadata_collected",
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Item without probability column gets accepted if no rejection reason",
            "input_data": [
                {
                    "hash": "noprobability123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "No Probability Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [None],
            "expected_fields": [
                {
                    "hash": "noprobability123456789012345678901234567890",
                    "rejection_status": "accepted",
                    "pipeline_status": "media_accepted",
                    "rejection_reason": None
                }
            ]
        },
        {
            "description": "Multiple items with mixed probabilities and statuses",
            "input_data": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "High Prob Movie",
                    "release_year": 2021,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "media_type": "movie",
                    "media_title": "Low Prob Movie",
                    "release_year": 2021,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "media_type": "movie",
                    "media_title": "Override Movie",
                    "release_year": 2021,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "override",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [0.90, 0.10, 0.05],
            "expected_fields": [
                {
                    "hash": "mixed1234567890123456789012345678901234567890",
                    "rejection_status": "accepted",
                    "pipeline_status": "media_accepted"
                },
                {
                    "hash": "mixed2345678901234567890123456789012345678901",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected",
                    "rejection_reason": "probability 0.1 below threshold 0.35"
                },
                {
                    "hash": "mixed3456789012345678901234567890123456789012",
                    "rejection_status": "override",
                    "pipeline_status": "media_accepted"
                }
            ]
        },
        {
            "description": "Items with probability values at boundary conditions",
            "input_data": [
                {
                    "hash": "boundary1234567890123456789012345678901234567",
                    "media_type": "movie",
                    "media_title": "Just Above Threshold",
                    "release_year": 2022,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                },
                {
                    "hash": "boundary2345678901234567890123456789012345678",
                    "media_type": "movie",
                    "media_title": "Just Below Threshold",
                    "release_year": 2022,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [0.351, 0.349],
            "expected_fields": [
                {
                    "hash": "boundary1234567890123456789012345678901234567",
                    "rejection_status": "accepted",
                    "pipeline_status": "media_accepted",
                    "rejection_reason": None
                },
                {
                    "hash": "boundary2345678901234567890123456789012345678",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected",
                    "rejection_reason": "probability 0.349 below threshold 0.35"
                }
            ]
        },
        {
            "description": "Items with different starting pipeline statuses but same outcome",
            "input_data": [
                {
                    "hash": "diffstatus1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Different Status Movie 1",
                    "release_year": 2023,
                    "pipeline_status": "file_accepted",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                },
                {
                    "hash": "diffstatus2345678901234567890123456789012345",
                    "media_type": "tv_show",
                    "media_title": "Different Status Show",
                    "season": 2,
                    "episode": 8,
                    "pipeline_status": "parsed",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [0.75, 0.20],
            "expected_fields": [
                {
                    "hash": "diffstatus1234567890123456789012345678901234",
                    "rejection_status": "accepted",
                    "pipeline_status": "media_accepted"
                },
                {
                    "hash": "diffstatus2345678901234567890123456789012345",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected",
                    "rejection_reason": "probability 0.2 below threshold 0.35"
                }
            ]
        }
    ]


@pytest.fixture
def probability_type_handling_cases():
    """Test scenarios for probability column type handling."""
    return [
        {
            "description": "String probability values should be cast to Float64",
            "input_data": [
                {
                    "hash": "stringprob123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "String Probability Movie",
                    "release_year": 2024,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": ["0.756"],  # String value as might come from API
            "expected_fields": [
                {
                    "hash": "stringprob123456789012345678901234567890123",
                    "rejection_status": "accepted",
                    "pipeline_status": "media_accepted",
                    "rejection_reason": None
                }
            ],
            "expected_probability_type": pl.Float64
        },
        {
            "description": "Mixed probability types should all be cast to Float64",
            "input_data": [
                {
                    "hash": "mixedprob1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Float Probability Movie",
                    "release_year": 2024,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                },
                {
                    "hash": "mixedprob2345678901234567890123456789012345",
                    "media_type": "movie",
                    "media_title": "String Probability Movie",
                    "release_year": 2024,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": [0.823, "0.234"],  # Mixed types
            "expected_fields": [
                {
                    "hash": "mixedprob1234567890123456789012345678901234",
                    "rejection_status": "accepted",
                    "pipeline_status": "media_accepted"
                },
                {
                    "hash": "mixedprob2345678901234567890123456789012345",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected",
                    "rejection_reason": "probability 0.234 below threshold 0.35"
                }
            ],
            "expected_probability_type": pl.Float64
        },
        {
            "description": "String probabilities with rounding should work correctly",
            "input_data": [
                {
                    "hash": "roundprob1234567890123456789012345678901234",
                    "media_type": "movie",
                    "media_title": "Rounding Test Movie 1",
                    "release_year": 2024,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                },
                {
                    "hash": "roundprob2345678901234567890123456789012345",
                    "media_type": "movie",
                    "media_title": "Rounding Test Movie 2",
                    "release_year": 2024,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": False
                }
            ],
            "probability_values": ["0.3456789", "0.1234567"],  # String values that need rounding
            "expected_fields": [
                {
                    "hash": "roundprob1234567890123456789012345678901234",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected",
                    "rejection_reason": "probability 0.346 below threshold 0.35"  # Rounded to 3 decimals
                },
                {
                    "hash": "roundprob2345678901234567890123456789012345",
                    "rejection_status": "rejected",
                    "pipeline_status": "rejected",
                    "rejection_reason": "probability 0.123 below threshold 0.35"  # Rounded to 3 decimals
                }
            ],
            "expected_probability_type": pl.Float64
        }
    ]


@pytest.fixture
def update_training_labels_cases():
    """Test scenarios for update_training_labels function."""
    return [
        {
            "description": "Accepted item with valid imdb_id updates to would_watch",
            "input_data": [
                {
                    "hash": "accepted1234567890123456789012345678901234",
                    "imdb_id": "tt1234567",
                    "media_type": "movie",
                    "media_title": "Accepted Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_would_watch_ids": ["tt1234567"],
            "expected_would_not_watch_ids": []
        },
        {
            "description": "Override item with valid imdb_id updates to would_watch",
            "input_data": [
                {
                    "hash": "override1234567890123456789012345678901234",
                    "imdb_id": "tt2345678",
                    "media_type": "movie",
                    "media_title": "Override Movie",
                    "rejection_status": "override",
                    "error_status": False
                }
            ],
            "expected_would_watch_ids": ["tt2345678"],
            "expected_would_not_watch_ids": []
        },
        {
            "description": "Rejected item with valid imdb_id updates to would_not_watch",
            "input_data": [
                {
                    "hash": "rejected1234567890123456789012345678901234",
                    "imdb_id": "tt3456789",
                    "media_type": "movie",
                    "media_title": "Rejected Movie",
                    "rejection_status": "rejected",
                    "error_status": False
                }
            ],
            "expected_would_watch_ids": [],
            "expected_would_not_watch_ids": ["tt3456789"]
        },
        {
            "description": "Item without imdb_id is skipped",
            "input_data": [
                {
                    "hash": "noimdb12345678901234567890123456789012345",
                    "imdb_id": None,
                    "media_type": "movie",
                    "media_title": "No IMDB Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_would_watch_ids": [],
            "expected_would_not_watch_ids": []
        },
        {
            "description": "Item with error_status True is skipped",
            "input_data": [
                {
                    "hash": "error12345678901234567890123456789012345678",
                    "imdb_id": "tt4567890",
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "rejection_status": "accepted",
                    "error_status": True
                }
            ],
            "expected_would_watch_ids": [],
            "expected_would_not_watch_ids": []
        },
        {
            "description": "Unfiltered item is skipped",
            "input_data": [
                {
                    "hash": "unfiltered12345678901234567890123456789012",
                    "imdb_id": "tt5678901",
                    "media_type": "movie",
                    "media_title": "Unfiltered Movie",
                    "rejection_status": "unfiltered",
                    "error_status": False
                }
            ],
            "expected_would_watch_ids": [],
            "expected_would_not_watch_ids": []
        },
        {
            "description": "Mixed items - accepted, rejected, and invalid",
            "input_data": [
                {
                    "hash": "accepted1234567890123456789012345678901234",
                    "imdb_id": "tt1111111",
                    "media_type": "movie",
                    "media_title": "Accepted 1",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "override1234567890123456789012345678901234",
                    "imdb_id": "tt2222222",
                    "media_type": "movie",
                    "media_title": "Override 1",
                    "rejection_status": "override",
                    "error_status": False
                },
                {
                    "hash": "rejected1234567890123456789012345678901234",
                    "imdb_id": "tt3333333",
                    "media_type": "movie",
                    "media_title": "Rejected 1",
                    "rejection_status": "rejected",
                    "error_status": False
                },
                {
                    "hash": "noimdb12345678901234567890123456789012345",
                    "imdb_id": None,
                    "media_type": "movie",
                    "media_title": "No IMDB",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "error12345678901234567890123456789012345678",
                    "imdb_id": "tt4444444",
                    "media_type": "movie",
                    "media_title": "Error",
                    "rejection_status": "accepted",
                    "error_status": True
                }
            ],
            "expected_would_watch_ids": ["tt1111111", "tt2222222"],
            "expected_would_not_watch_ids": ["tt3333333"]
        },
        {
            "description": "Empty DataFrame returns without calling db",
            "input_data": [],
            "expected_would_watch_ids": [],
            "expected_would_not_watch_ids": []
        },
        {
            "description": "Duplicate imdb_ids are deduplicated",
            "input_data": [
                {
                    "hash": "dupe11234567890123456789012345678901234567",
                    "imdb_id": "tt5555555",
                    "media_type": "movie",
                    "media_title": "Dupe 1",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "dupe21234567890123456789012345678901234567",
                    "imdb_id": "tt5555555",
                    "media_type": "movie",
                    "media_title": "Dupe 2",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "expected_would_watch_ids": ["tt5555555"],
            "expected_would_not_watch_ids": []
        }
    ]