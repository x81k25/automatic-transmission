import pytest
from pathlib import PurePosixPath

@pytest.fixture
def generate_movie_target_path_cases():
    """Test scenarios for generate_movie_target_path function."""
    return [
        {
            "description": "Standard movie with x265 codec",
            "input": {
                "movie_title": "The Dark Knight",
                "release_year": 2008,
                "resolution": "1080p",
                "video_codec": "x265"
            },
            "expected": "the-dark-knight-2008-1080p-x265"
        },
        {
            "description": "Movie with H.265 codec gets converted to h265",
            "input": {
                "movie_title": "Inception",
                "release_year": 2010,
                "resolution": "1080p",
                "video_codec": "H.265"
            },
            "expected": "inception-2010-1080p-h265"
        },
        {
            "description": "Movie with H 265 codec gets converted to h265",
            "input": {
                "movie_title": "Interstellar",
                "release_year": 2014,
                "resolution": "2160p",
                "video_codec": "H 265"
            },
            "expected": "interstellar-2014-2160p-h265"
        },
        {
            "description": "Movie without video codec",
            "input": {
                "movie_title": "Pulp Fiction",
                "release_year": 1994,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected": "pulp-fiction-1994-1080p"
        },
        {
            "description": "Movie with unsupported codec (h264) - codec not included",
            "input": {
                "movie_title": "The Matrix",
                "release_year": 1999,
                "resolution": "720p",
                "video_codec": "h264"
            },
            "expected": "the-matrix-1999-720p"
        },
        {
            "description": "Movie with special characters in title",
            "input": {
                "movie_title": "Spider-Man: No Way Home",
                "release_year": 2021,
                "resolution": "1080p",
                "video_codec": "x265"
            },
            "expected": "spider-man-no-way-home-2021-1080p-x265"
        },
        {
            "description": "Movie with multiple spaces and special characters",
            "input": {
                "movie_title": "The Lord of the Rings: The Fellowship of the Ring",
                "release_year": 2001,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected": "the-lord-of-the-rings-the-fellowship-of-the-ring-2001-1080p"
        },
        {
            "description": "Movie with numbers in title",
            "input": {
                "movie_title": "2001: A Space Odyssey",
                "release_year": 1968,
                "resolution": "1080p",
                "video_codec": "x265"
            },
            "expected": "2001-a-space-odyssey-1968-1080p-x265"
        },
        {
            "description": "Movie with empty string codec",
            "input": {
                "movie_title": "Blade Runner",
                "release_year": 1982,
                "resolution": "1080p",
                "video_codec": ""
            },
            "expected": "blade-runner-1982-1080p"
        },
        {
            "description": "Movie with codec in quotes",
            "input": {
                "movie_title": "Alien",
                "release_year": 1979,
                "resolution": "1080p",
                "video_codec": '"x265"'
            },
            "expected": "alien-1979-1080p-x265"
        },
        {
            "description": "Movie with None resolution and no codec",
            "input": {
                "movie_title": "Zootopia 2",
                "release_year": 2025,
                "resolution": None,
                "video_codec": None
            },
            "expected": "zootopia-2-2025"
        },
        {
            "description": "Movie with None resolution but with codec",
            "input": {
                "movie_title": "Jackie Brown",
                "release_year": 1997,
                "resolution": None,
                "video_codec": "x265"
            },
            "expected": "jackie-brown-1997-x265"
        },
        {
            "description": "Movie with empty string resolution (treated as None)",
            "input": {
                "movie_title": "Test Movie",
                "release_year": 2020,
                "resolution": "",
                "video_codec": None
            },
            "expected": "test-movie-2020"
        }
    ]

@pytest.fixture
def generate_movie_target_path_error_cases():
    """Error test scenarios for generate_movie_target_path function."""
    return [
        {
            "description": "Empty movie title raises ValueError",
            "input": {
                "movie_title": "",
                "release_year": 2020,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected_error": ValueError
        },
        {
            "description": "None movie title raises ValueError",
            "input": {
                "movie_title": None,
                "release_year": 2020,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected_error": ValueError
        },
        {
            "description": "Invalid release year raises ValueError",
            "input": {
                "movie_title": "Test Movie",
                "release_year": None,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected_error": ValueError
        },
        {
            "description": "Non-string movie title raises ValueError",
            "input": {
                "movie_title": 123,
                "release_year": 2020,
                "resolution": "1080p",
                "video_codec": None
            },
            "expected_error": ValueError
        }
    ]

@pytest.fixture
def generate_tv_season_parent_path_cases():
    """Test scenarios for generate_tv_season_parent_path function."""
    return [
        {
            "description": "Standard TV season parent path",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "Breaking Bad",
                "release_year": 2008
            },
            "expected": PurePosixPath("/k/media/video/tv/breaking-bad-2008")
        },
        {
            "description": "TV show with special characters",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "It's Always Sunny in Philadelphia",
                "release_year": 2005
            },
            "expected": PurePosixPath("/k/media/video/tv/it-s-always-sunny-in-philadelphia-2005")
        },
        {
            "description": "TV show with numbers",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "24",
                "release_year": 2001
            },
            "expected": PurePosixPath("/k/media/video/tv/24-2001")
        },
        {
            "description": "TV show with multiple spaces",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "The   Big    Bang Theory",
                "release_year": 2007
            },
            "expected": PurePosixPath("/k/media/video/tv/the-big-bang-theory-2007")
        },
        {
            "description": "Root dir without trailing slash",
            "input": {
                "root_dir": "/k/media/video/tv",
                "tv_show_name": "Game of Thrones",
                "release_year": 2011
            },
            "expected": PurePosixPath("/k/media/video/tv/game-of-thrones-2011")
        }
    ]

@pytest.fixture
def generate_tv_season_parent_path_error_cases():
    """Error test scenarios for generate_tv_season_parent_path function."""
    return [
        {
            "description": "Empty root_dir raises ValueError",
            "input": {
                "root_dir": "",
                "tv_show_name": "Test Show",
                "release_year": 2020
            },
            "expected_error": ValueError
        },
        {
            "description": "None tv_show_name raises ValueError",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": None,
                "release_year": 2020
            },
            "expected_error": ValueError
        },
        {
            "description": "Invalid release_year raises ValueError",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "Test Show",
                "release_year": "not_a_number"
            },
            "expected_error": ValueError
        }
    ]

@pytest.fixture
def generate_tv_season_target_path_cases():
    """Test scenarios for generate_tv_season_target_path function."""
    return [
        {
            "description": "Single digit season with padding",
            "input": {"season": 1},
            "expected": "s01"
        },
        {
            "description": "Double digit season",
            "input": {"season": 15},
            "expected": "s15"
        },
        {
            "description": "Triple digit season",
            "input": {"season": 123},
            "expected": "s123"
        },
        {
            "description": "Four digit season",
            "input": {"season": 1234},
            "expected": "s1234"
        },
        {
            "description": "Season 99 (boundary case)",
            "input": {"season": 99},
            "expected": "s99"
        },
        {
            "description": "Season 100 (boundary case)",
            "input": {"season": 100},
            "expected": "s100"
        },
        {
            "description": "Season 999 (boundary case)",
            "input": {"season": 999},
            "expected": "s999"
        },
        {
            "description": "Season 1000 (boundary case)",
            "input": {"season": 1000},
            "expected": "s1000"
        }
    ]

@pytest.fixture
def generate_tv_season_target_path_error_cases():
    """Error test scenarios for generate_tv_season_target_path function."""
    return [
        {
            "description": "None season raises ValueError",
            "input": {"season": None},
            "expected_error": ValueError
        },
        {
            "description": "Zero season raises ValueError",
            "input": {"season": 0},
            "expected_error": ValueError
        },
        {
            "description": "Negative season raises ValueError",
            "input": {"season": -1},
            "expected_error": ValueError
        },
        {
            "description": "Season too large raises ValueError",
            "input": {"season": 10000},
            "expected_error": ValueError
        },
        {
            "description": "Non-integer season raises ValueError",
            "input": {"season": "not_a_number"},
            "expected_error": ValueError
        }
    ]

@pytest.fixture
def generate_tv_show_parent_path_cases():
    """Test scenarios for generate_tv_show_parent_path function."""
    return [
        {
            "description": "Standard TV show parent path",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "The Office",
                "release_year": 2005,
                "season": 1
            },
            "expected": PurePosixPath("/k/media/video/tv/the-office-2005/s01")
        },
        {
            "description": "TV show with double digit season",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "Friends",
                "release_year": 1994,
                "season": 10
            },
            "expected": PurePosixPath("/k/media/video/tv/friends-1994/s10")
        },
        {
            "description": "TV show with triple digit season",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "Doctor Who",
                "release_year": 1963,
                "season": 123
            },
            "expected": PurePosixPath("/k/media/video/tv/doctor-who-1963/s123")
        },
        {
            "description": "TV show with special characters",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "Marvel's Agents of S.H.I.E.L.D.",
                "release_year": 2013,
                "season": 5
            },
            "expected": PurePosixPath("/k/media/video/tv/marvel-s-agents-of-s-h-i-e-l-d-2013/s05")
        }
    ]

@pytest.fixture
def generate_tv_show_parent_path_error_cases():
    """Error test scenarios for generate_tv_show_parent_path function."""
    return [
        {
            "description": "Invalid season raises ValueError",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "Test Show",
                "release_year": 2020,
                "season": 0
            },
            "expected_error": ValueError
        },
        {
            "description": "Season too large raises ValueError",
            "input": {
                "root_dir": "/k/media/video/tv/",
                "tv_show_name": "Test Show",
                "release_year": 2020,
                "season": 10000
            },
            "expected_error": ValueError
        }
    ]

@pytest.fixture
def generate_tv_show_target_path_cases():
    """Test scenarios for generate_tv_show_target_path function."""
    return [
        {
            "description": "Single digit season and episode",
            "input": {"season": 1, "episode": 5},
            "expected": "s01e05"
        },
        {
            "description": "Double digit season and episode",
            "input": {"season": 15, "episode": 23},
            "expected": "s15e23"
        },
        {
            "description": "Triple digit season and episode",
            "input": {"season": 123, "episode": 456},
            "expected": "s123e456"
        },
        {
            "description": "Four digit season and episode",
            "input": {"season": 1234, "episode": 5678},
            "expected": "s1234e5678"
        },
        {
            "description": "Boundary cases - season 99, episode 99",
            "input": {"season": 99, "episode": 99},
            "expected": "s99e99"
        },
        {
            "description": "Boundary cases - season 100, episode 100",
            "input": {"season": 100, "episode": 100},
            "expected": "s100e100"
        },
        {
            "description": "Mixed digit counts",
            "input": {"season": 5, "episode": 123},
            "expected": "s05e123"
        }
    ]

@pytest.fixture
def generate_tv_show_target_path_error_cases():
    """Error test scenarios for generate_tv_show_target_path function."""
    return [
        {
            "description": "None season raises ValueError",
            "input": {"season": None, "episode": 1},
            "expected_error": ValueError
        },
        {
            "description": "None episode raises ValueError",
            "input": {"season": 1, "episode": None},
            "expected_error": ValueError
        },
        {
            "description": "Zero season raises ValueError",
            "input": {"season": 0, "episode": 1},
            "expected_error": ValueError
        },
        {
            "description": "Zero episode raises ValueError",
            "input": {"season": 1, "episode": 0},
            "expected_error": ValueError
        },
        {
            "description": "Season too large raises ValueError",
            "input": {"season": 10000, "episode": 1},
            "expected_error": ValueError
        },
        {
            "description": "Episode too large raises ValueError",
            "input": {"season": 1, "episode": 10000},
            "expected_error": ValueError
        }
    ]