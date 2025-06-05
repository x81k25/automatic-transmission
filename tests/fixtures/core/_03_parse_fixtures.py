import pytest
import polars as pl
from src.data_models import *

@pytest.fixture
def parse_media_items_cases():
    """Test scenarios for parse_media_items function."""
    return [
        {
            "description": "Movie with year in parentheses and YTS uploader",
            "input_data": [
                {
                    "hash": "67a079157676a9b40dc1d1ba7fe46f19ae0e1254",
                    "media_type": "movie",
                    "original_title": "Live Free or Die (2006) [1080p] [WEBRip] [5.1] [YTS.MX]",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "67a079157676a9b40dc1d1ba7fe46f19ae0e1254",
                    "media_title": "Live Free or Die",
                    "release_year": 2006,
                    "resolution": "1080p",
                    "video_codec": None,
                    "audio_codec": None,
                    "upload_type": "WEBRip",
                    "uploader": "YTS.MX"
                }
            ]
        },
        {
            "description": "TV show with UIndex prefix cleaned",
            "input_data": [
                {
                    "hash": "24b812e34cf5440c4de952245b676becfb1db2c7",
                    "media_type": "tv_show",
                    "original_title": "www.UIndex.org    -    Star Wars Andor S02E06 What a Festive Evening 1080p DSNP WEB-DL DDP5 1 H 264-NTb",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "24b812e34cf5440c4de952245b676becfb1db2c7",
                    "media_title": "Star Wars Andor",
                    "season": 2,
                    "episode": 6,
                    "resolution": "1080p",
                    "video_codec": "H 264",
                    "upload_type": "WEB-DL",
                    "uploader": "NTb"
                }
            ]
        },
        {
            "description": "TV season with season pattern",
            "input_data": [
                {
                    "hash": "276b69e62b2bbb848e0290525f74048ecf78df0b",
                    "media_type": "tv_season",
                    "original_title": "The.Studio.S01.1080p.ATVP.WEB-DL.ITA-ENG.DD5.1.H.264-YTS.MX",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "276b69e62b2bbb848e0290525f74048ecf78df0b",
                    "media_title": "The Studio",
                    "season": 1,
                    "resolution": "1080p",
                    "video_codec": "H.264",
                    "upload_type": "WEB-DL",
                    "uploader": "YTS.MX"
                }
            ]
        },
        {
            "description": "Movie with periods in title",
            "input_data": [
                {
                    "hash": "08105069ee5816d68259486376a58a4846e69eb4",
                    "media_type": "movie",
                    "original_title": "Paranormal.Activity.2.(2010).[1080p].[BluRay].[5.1].[YTS.MX]",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "08105069ee5816d68259486376a58a4846e69eb4",
                    "media_title": "Paranormal Activity 2",
                    "release_year": 2010,
                    "resolution": "1080p",
                    "upload_type": "BluRay",
                    "uploader": "YTS.MX"
                }
            ]
        },
        {
            "description": "TV show with audio codec",
            "input_data": [
                {
                    "hash": "1e22f5f51514c2761480edf0f10e12f3ea523bb1",
                    "media_type": "tv_show",
                    "original_title": "Abbott Elementary S04E04 Costume Contest 1080p DSNP WEB-DL DD 5 1 H 264-playWEB EZTV",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "1e22f5f51514c2761480edf0f10e12f3ea523bb1",
                    "media_title": "Abbott Elementary",
                    "season": 4,
                    "episode": 4,
                    "resolution": "1080p",
                    "video_codec": "H 264",
                    "upload_type": "WEB-DL",
                    "uploader": "playWEB"
                }
            ]
        },
        {
            "description": "TV show with HEVC codec",
            "input_data": [
                {
                    "hash": "8dcacf01d18ce230fd75bdfac632b05835b28fd2",
                    "media_type": "tv_show",
                    "original_title": "Your.Friends.and.Neighbors.S01E09.1080p.HEVC.x265-MeGusta[EZTVx.to].mkv",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "8dcacf01d18ce230fd75bdfac632b05835b28fd2",
                    "media_title": "Your Friends and Neighbors",
                    "season": 1,
                    "episode": 9,
                    "resolution": "1080p",
                    "video_codec": "HEVC",
                    "uploader": "MeGusta"
                }
            ]
        },
        {
            "description": "Movie with x265 codec",
            "input_data": [
                {
                    "hash": "ddd5003fa891a880f8e0b3676b7af9a15754fdee",
                    "media_type": "movie",
                    "original_title": "Juliet & Romeo (2025) [1080p] [WEBRip] [x265] [10bit] [5.1] [YTS.MX]",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "ddd5003fa891a880f8e0b3676b7af9a15754fdee",
                    "media_title": "Juliet & Romeo",
                    "release_year": 2025,
                    "resolution": "1080p",
                    "video_codec": "x265",
                    "upload_type": "WEBRip",
                    "uploader": "YTS.MX"
                }
            ]
        },
        {
            "description": "TV show with complex title and multi-word season",
            "input_data": [
                {
                    "hash": "22e27ff4e69b064ad522addbda26412d0f94b6eb",
                    "media_type": "tv_show",
                    "original_title": "the.farmer.wants.a.wife.au.s15e08.1080p.hdtv.h264-MeGusta[EZTVx.to].mkv",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "22e27ff4e69b064ad522addbda26412d0f94b6eb",
                    "media_title": "the farmer wants a wife au",
                    "season": 15,
                    "episode": 8,
                    "resolution": "1080p",
                    "video_codec": "h264",
                    "uploader": "MeGusta"
                }
            ]
        },
        {
            "description": "Movie with 720p resolution (should be parsed)",
            "input_data": [
                {
                    "hash": "d9e97572a5ded49c418fa5374efdf0823020a449",
                    "media_type": "movie",
                    "original_title": "Bottoms (2023) [720p] [BluRay] [YTS.MX]",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "d9e97572a5ded49c418fa5374efdf0823020a449",
                    "media_title": "Bottoms",
                    "release_year": 2023,
                    "resolution": "720p",
                    "upload_type": "BluRay",
                    "uploader": "YTS.MX"
                }
            ]
        },
        {
            "description": "Complex TV show with long title",
            "input_data": [
                {
                    "hash": "2e4620bc31203245f03b39f92036d613278a72f2",
                    "media_type": "tv_show",
                    "original_title": "60.Minutes.S57E38.1080p.WEB.h264-MeGusta[EZTVx.to].mkv",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "2e4620bc31203245f03b39f92036d613278a72f2",
                    "media_title": "60 Minutes",
                    "season": 57,
                    "episode": 38,
                    "resolution": "1080p",
                    "video_codec": "h264",
                    "upload_type": "WEB",
                    "uploader": "MeGusta"
                }
            ]
        },
        {
            "description": "season with no special characters",
            "input_data": [
                {
                    "hash": "0c5ba8200b9f15d01002b10bcfe43a5b85bd2902",
                    "media_type": "tv_season",
                    "original_title": "The Last of Us S02 1080p x265-ELiTE EZTV",
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "0c5ba8200b9f15d01002b10bcfe43a5b85bd2902",
                    "media_title": "The Last of Us",
                    "season": 2,
                    "episode": None,
                    "resolution": "1080p",
                    "video_codec": "x265",
                    "upload_type": None,
                    "uploader": "ELiTE"
                }
            ]
        }
    ]


@pytest.fixture
def validate_parsed_media_cases():
    """Test scenarios for validate_parsed_media function."""
    return [
        {
            "description": "Valid movie with all required fields",
            "input_data": [
                {
                    "hash": "validmovie123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Valid Movie",
                    "release_year": 2020,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "validmovie123456789012345678901234567890",
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Valid TV show with all required fields",
            "input_data": [
                {
                    "hash": "validtvshow123456789012345678901234567890",
                    "media_type": "tv_show",
                    "media_title": "Valid TV Show",
                    "season": 1,
                    "episode": 5,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "validtvshow123456789012345678901234567890",
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Valid TV season with all required fields",
            "input_data": [
                {
                    "hash": "validtvseason12345678901234567890123456789",
                    "media_type": "tv_season",
                    "media_title": "Valid TV Season",
                    "season": 3,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "validtvseason12345678901234567890123456789",
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Movie missing media_title",
            "input_data": [
                {
                    "hash": "missingtitle123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": None,
                    "release_year": 2020,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "missingtitle123456789012345678901234567890",
                    "error_condition": "media_title is null"
                }
            ]
        },
        {
            "description": "Movie missing release_year",
            "input_data": [
                {
                    "hash": "missingyear123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Movie Without Year",
                    "release_year": None,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "missingyear123456789012345678901234567890",
                    "error_condition": "release_year is null"
                }
            ]
        },
        {
            "description": "Movie missing both media_title and release_year",
            "input_data": [
                {
                    "hash": "missingboth123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": None,
                    "release_year": None,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "missingboth123456789012345678901234567890",
                    "error_condition": "media_title is null; release_year is null"
                }
            ]
        },
        {
            "description": "TV show missing season",
            "input_data": [
                {
                    "hash": "missingseason12345678901234567890123456789",
                    "media_type": "tv_show",
                    "media_title": "TV Show Without Season",
                    "season": None,
                    "episode": 5,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "missingseason12345678901234567890123456789",
                    "error_condition": "season is null"
                }
            ]
        },
        {
            "description": "TV show missing episode",
            "input_data": [
                {
                    "hash": "missingepisode1234567890123456789012345678",
                    "media_type": "tv_show",
                    "media_title": "TV Show Without Episode",
                    "season": 2,
                    "episode": None,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "missingepisode1234567890123456789012345678",
                    "error_condition": "episode is null"
                }
            ]
        },
        {
            "description": "TV show missing media_title, season, and episode",
            "input_data": [
                {
                    "hash": "missingall123456789012345678901234567890",
                    "media_type": "tv_show",
                    "media_title": None,
                    "season": None,
                    "episode": None,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "missingall123456789012345678901234567890",
                    "error_condition": "media_title is null; season is null; episode is null"
                }
            ]
        },
        {
            "description": "TV season missing season",
            "input_data": [
                {
                    "hash": "seasonmissing12345678901234567890123456789",
                    "media_type": "tv_season",
                    "media_title": "TV Season Without Season Number",
                    "season": None,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "seasonmissing12345678901234567890123456789",
                    "error_condition": "season is null"
                }
            ]
        },
        {
            "description": "Mixed media types with various validation errors",
            "input_data": [
                {
                    "hash": "valid1234567890123456789012345678901234567",
                    "media_type": "movie",
                    "media_title": "Valid Movie",
                    "release_year": 2021,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                },
                {
                    "hash": "invalid123456789012345678901234567890123456",
                    "media_type": "tv_show",
                    "media_title": None,
                    "season": 1,
                    "episode": None,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                },
                {
                    "hash": "partial123456789012345678901234567890123456",
                    "media_type": "tv_season",
                    "media_title": "Partial Season",
                    "season": 2,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "valid1234567890123456789012345678901234567",
                    "error_condition": None
                },
                {
                    "hash": "invalid123456789012345678901234567890123456",
                    "error_condition": "media_title is null; episode is null"
                },
                {
                    "hash": "partial123456789012345678901234567890123456",
                    "error_condition": None
                }
            ]
        },
        {
            "description": "Item with existing error_condition gets additional validation errors",
            "input_data": [
                {
                    "hash": "existing123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": None,
                    "release_year": 2020,
                    "pipeline_status": "parsed",
                    "error_status": True,
                    "error_condition": "parsing failed",
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "existing123456789012345678901234567890123",
                    "error_condition": "parsing failed; media_title is null"
                }
            ]
        }
    ]


@pytest.fixture
def update_status_cases():
    """Test scenarios for update_status function."""
    return [
        {
            "description": "Single item with no errors gets status updated to parsed",
            "input_data": [
                {
                    "hash": "noerrors123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Good Movie",
                    "release_year": 2020,
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "error_condition": None,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "noerrors123456789012345678901234567890123",
                    "pipeline_status": "parsed"
                }
            ]
        },
        {
            "description": "Single item with errors keeps original status",
            "input_data": [
                {
                    "hash": "witherrors12345678901234567890123456789012",
                    "media_type": "movie",
                    "media_title": None,
                    "release_year": 2020,
                    "pipeline_status": "ingested",
                    "error_status": True,
                    "error_condition": "media_title is null",
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "witherrors12345678901234567890123456789012",
                    "pipeline_status": "ingested"
                }
            ]
        },
        {
            "description": "Multiple items with mixed error statuses",
            "input_data": [
                {
                    "hash": "good1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Good Movie",
                    "release_year": 2021,
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "error_condition": None,
                    "rejection_status": "unfiltered"
                },
                {
                    "hash": "bad12345678901234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "Bad TV Show",
                    "season": None,
                    "episode": 1,
                    "pipeline_status": "ingested",
                    "error_status": True,
                    "error_condition": "season is null",
                    "rejection_status": "unfiltered"
                },
                {
                    "hash": "good2345678901234567890123456789012345678901",
                    "media_type": "tv_season",
                    "media_title": "Good Season",
                    "season": 3,
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "error_condition": None,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "good1234567890123456789012345678901234567890",
                    "pipeline_status": "parsed"
                },
                {
                    "hash": "bad12345678901234567890123456789012345678901",
                    "pipeline_status": "ingested"
                },
                {
                    "hash": "good2345678901234567890123456789012345678901",
                    "pipeline_status": "parsed"
                }
            ]
        },
        {
            "description": "Item already in parsed status with no errors",
            "input_data": [
                {
                    "hash": "already123456789012345678901234567890123456",
                    "media_type": "movie",
                    "media_title": "Already Parsed Movie",
                    "release_year": 2022,
                    "pipeline_status": "parsed",
                    "error_status": False,
                    "error_condition": None,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "already123456789012345678901234567890123456",
                    "pipeline_status": "parsed"
                }
            ]
        },
        {
            "description": "All items have errors and keep original statuses",
            "input_data": [
                {
                    "hash": "error1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": None,
                    "release_year": None,
                    "pipeline_status": "ingested",
                    "error_status": True,
                    "error_condition": "media_title is null; release_year is null",
                    "rejection_status": "unfiltered"
                },
                {
                    "hash": "error2345678901234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "Error Show",
                    "season": None,
                    "episode": None,
                    "pipeline_status": "paused",
                    "error_status": True,
                    "error_condition": "season is null; episode is null",
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "error1234567890123456789012345678901234567890",
                    "pipeline_status": "ingested"
                },
                {
                    "hash": "error2345678901234567890123456789012345678901",
                    "pipeline_status": "paused"
                }
            ]
        },
        {
            "description": "All items have no errors and get updated to parsed",
            "input_data": [
                {
                    "hash": "clean1234567890123456789012345678901234567890",
                    "media_type": "movie",
                    "media_title": "Clean Movie 1",
                    "release_year": 2023,
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "error_condition": None,
                    "rejection_status": "unfiltered"
                },
                {
                    "hash": "clean2345678901234567890123456789012345678901",
                    "media_type": "tv_show",
                    "media_title": "Clean Show",
                    "season": 2,
                    "episode": 8,
                    "pipeline_status": "ingested",
                    "error_status": False,
                    "error_condition": None,
                    "rejection_status": "unfiltered"
                }
            ],
            "expected_fields": [
                {
                    "hash": "clean1234567890123456789012345678901234567890",
                    "pipeline_status": "parsed"
                },
                {
                    "hash": "clean2345678901234567890123456789012345678901",
                    "pipeline_status": "parsed"
                }
            ]
        }
    ]
