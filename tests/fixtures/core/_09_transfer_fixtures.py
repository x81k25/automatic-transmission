import pytest
from src.data_models import *

@pytest.fixture
def generate_file_paths_cases():
    """Test scenarios for generate_file_paths function."""
    return [
        {
            "description": "Movie with all fields populated including codec",
            "input": {
                "hash": "movie123456789012345678901234567890123456",
                "media_type": "movie",
                "media_title": "The Dark Knight",
                "release_year": 2008,
                "resolution": "1080p",
                "video_codec": "x265"
            },
            "expected": {
                "hash": "movie123456789012345678901234567890123456",
                "parent_path": "/k/media/video/movies",
                "target_path": "the-dark-knight-2008-1080p-x265",
                "error_condition": None
            }
        },
        {
            "description": "Movie without video codec",
            "input": {
                "hash": "movieno123456789012345678901234567890123456",
                "media_type": "movie",
                "media_title": "Pulp Fiction",
                "release_year": 1994,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected": {
                "hash": "movieno123456789012345678901234567890123456",
                "parent_path": "/k/media/video/movies",
                "target_path": "pulp-fiction-1994-1080p",
                "error_condition": None
            }
        },
        {
            "description": "Movie with H.265 codec conversion",
            "input": {
                "hash": "movieh265123456789012345678901234567890123",
                "media_type": "movie",
                "media_title": "Inception",
                "release_year": 2010,
                "resolution": "2160p",
                "video_codec": "H.265"
            },
            "expected": {
                "hash": "movieh265123456789012345678901234567890123",
                "parent_path": "/k/media/video/movies",
                "target_path": "inception-2010-2160p-h265",
                "error_condition": None
            }
        },
        {
            "description": "Movie with special characters in title",
            "input": {
                "hash": "moviespec123456789012345678901234567890123",
                "media_type": "movie",
                "media_title": "Spider-Man: No Way Home",
                "release_year": 2021,
                "resolution": "1080p",
                "video_codec": "x265"
            },
            "expected": {
                "hash": "moviespec123456789012345678901234567890123",
                "parent_path": "/k/media/video/movies",
                "target_path": "spider-man-no-way-home-2021-1080p-x265",
                "error_condition": None
            }
        },
        {
            "description": "Movie with malformed title (multiple spaces and special chars)",
            "input": {
                "hash": "moviebad123456789012345678901234567890123",
                "media_type": "movie",
                "media_title": "The   Lord!!!   of the  Rings:   Fellowship",
                "release_year": 2001,
                "resolution": "720p",
                "video_codec": None
            },
            "expected": {
                "hash": "moviebad123456789012345678901234567890123",
                "parent_path": "/k/media/video/movies",
                "target_path": "the-lord-of-the-rings-fellowship-2001-720p",
                "error_condition": None
            }
        },
        {
            "description": "TV season with standard data",
            "input": {
                "hash": "tvseason123456789012345678901234567890123",
                "media_type": "tv_season",
                "media_title": "Breaking Bad",
                "release_year": 2008,
                "season": 1
            },
            "expected": {
                "hash": "tvseason123456789012345678901234567890123",
                "parent_path": "/k/media/video/tv/breaking-bad-2008",
                "target_path": "s01",
                "error_condition": None
            }
        },
        {
            "description": "TV season with special characters in title",
            "input": {
                "hash": "tvseasonsp123456789012345678901234567890123",
                "media_type": "tv_season",
                "media_title": "It's Always Sunny in Philadelphia",
                "release_year": 2005,
                "season": 15
            },
            "expected": {
                "hash": "tvseasonsp123456789012345678901234567890123",
                "parent_path": "/k/media/video/tv/it-s-always-sunny-in-philadelphia-2005",
                "target_path": "s15",
                "error_condition": None
            }
        },
        {
            "description": "TV season with triple digit season",
            "input": {
                "hash": "tvseason3d123456789012345678901234567890123",
                "media_type": "tv_season",
                "media_title": "Doctor Who",
                "release_year": 1963,
                "season": 123
            },
            "expected": {
                "hash": "tvseason3d123456789012345678901234567890123",
                "parent_path": "/k/media/video/tv/doctor-who-1963",
                "target_path": "s123",
                "error_condition": None
            }
        },
        {
            "description": "TV show with standard data",
            "input": {
                "hash": "tvshow123456789012345678901234567890123456",
                "media_type": "tv_show",
                "media_title": "The Office",
                "release_year": 2005,
                "season": 2,
                "episode": 8
            },
            "expected": {
                "hash": "tvshow123456789012345678901234567890123456",
                "parent_path": "/k/media/video/tv/the-office-2005/s02",
                "target_path": "s02e08",
                "error_condition": None
            }
        },
        {
            "description": "TV show with double digit season and episode",
            "input": {
                "hash": "tvshowdd123456789012345678901234567890123456",
                "media_type": "tv_show",
                "media_title": "Friends",
                "release_year": 1994,
                "season": 10,
                "episode": 24
            },
            "expected": {
                "hash": "tvshowdd123456789012345678901234567890123456",
                "parent_path": "/k/media/video/tv/friends-1994/s10",
                "target_path": "s10e24",
                "error_condition": None
            }
        },
        {
            "description": "TV show with malformed title",
            "input": {
                "hash": "tvshowbad123456789012345678901234567890123456",
                "media_type": "tv_show",
                "media_title": "Marvel's   Agents!!!  of   S.H.I.E.L.D.",
                "release_year": 2013,
                "season": 5,
                "episode": 12
            },
            "expected": {
                "hash": "tvshowbad123456789012345678901234567890123456",
                "parent_path": "/k/media/video/tv/marvel-s-agents-of-s-h-i-e-l-d-2013/s05",
                "target_path": "s05e12",
                "error_condition": None
            }
        },
        {
            "description": "TV show with mixed digit counts",
            "input": {
                "hash": "tvshowmix123456789012345678901234567890123456",
                "media_type": "tv_show",
                "media_title": "Game of Thrones",
                "release_year": 2011,
                "season": 8,
                "episode": 123
            },
            "expected": {
                "hash": "tvshowmix123456789012345678901234567890123456",
                "parent_path": "/k/media/video/tv/game-of-thrones-2011/s08",
                "target_path": "s08e123",
                "error_condition": None
            }
        },
        {
            "description": "Movie with None media_title sets error_condition",
            "input": {
                "hash": "errorhash123456789012345678901234567890",
                "media_type": "movie",
                "media_title": None,
                "release_year": 2020,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected": {
                "hash": "errorhash123456789012345678901234567890",
                "error_condition": "error writing target_path - movie_title must be a non-empty string"
            }
        },
        {
            "description": "Movie with empty media_title sets error_condition",
            "input": {
                "hash": "errorempty12345678901234567890123456789",
                "media_type": "movie",
                "media_title": "",
                "release_year": 2020,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected": {
                "hash": "errorempty12345678901234567890123456789",
                "error_condition": "error writing target_path - movie_title must be a non-empty string"
            }
        },
        {
            "description": "Movie with None resolution succeeds without resolution in path",
            "input": {
                "hash": "errornores123456789012345678901234567890",
                "media_type": "movie",
                "media_title": "Test Movie",
                "release_year": 2020,
                "resolution": None,
                "video_codec": None
            },
            "expected": {
                "hash": "errornores123456789012345678901234567890",
                "parent_path": "/k/media/video/movies",
                "target_path": "test-movie-2020",
                "error_condition": None
            }
        },
        {
            "description": "TV season with None season sets error_condition",
            "input": {
                "hash": "errortvs123456789012345678901234567890123",
                "media_type": "tv_season",
                "media_title": "Test Show",
                "release_year": 2020,
                "season": None
            },
            "expected": {
                "hash": "errortvs123456789012345678901234567890123",
                "error_condition": "error writing target_path - season must be a valid integer"
            }
        },
        {
            "description": "TV season with negative season sets error_condition",
            "input": {
                "hash": "errortvs0123456789012345678901234567890123",
                "media_type": "tv_season",
                "media_title": "Test Show",
                "release_year": 2020,
                "season": -1
            },
            "expected": {
                "hash": "errortvs0123456789012345678901234567890123",
                "error_condition": "error writing target_path - season cannot be negative"
            }
        },
        {
            "description": "TV show with None episode sets error_condition",
            "input": {
                "hash": "errortv1234567890123456789012345678901234",
                "media_type": "tv_show",
                "media_title": "Test Show",
                "release_year": 2020,
                "season": 1,
                "episode": None
            },
            "expected": {
                "hash": "errortv1234567890123456789012345678901234",
                "error_condition": "error writing target_path - episode must be a valid integer"
            }
        },
        {
            "description": "TV show with invalid episode (0) sets error_condition",
            "input": {
                "hash": "errortv0ep123456789012345678901234567890",
                "media_type": "tv_show",
                "media_title": "Test Show",
                "release_year": 2020,
                "season": 1,
                "episode": 0
            },
            "expected": {
                "hash": "errortv0ep123456789012345678901234567890",
                "error_condition": "error writing target_path - episode must be greater than 0"
            }
        }
    ]


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