import pytest

@pytest.fixture
def extract_hash_from_direct_download_url_cases():
    """10 test scenarios for extract_hash_from_direct_download_url function."""
    return [
        {
            "description": "Standard YTS torrent URL",
            "url": "https://yts.mx/torrent/download/ABC123DEF456789012345678901234567890ABCD.torrent",
            "expected": "abc123def456789012345678901234567890abcd.torrent"
        },
        {
            "description": "URL without file extension",
            "url": "https://example.com/downloads/ABC123DEF456789012345678901234567890ABCD",
            "expected": "abc123def456789012345678901234567890abcd"
        },
        {
            "description": "URL with multiple path segments",
            "url": "https://torrents.example.com/files/movies/2024/DEF456789012345678901234567890ABCDEF123456.torrent",
            "expected": "def456789012345678901234567890abcdef123456.torrent"
        },
        {
            "description": "Uppercase hash gets lowercased",
            "url": "https://example.com/FEDCBA0987654321FEDCBA0987654321FEDCBA09",
            "expected": "fedcba0987654321fedcba0987654321fedcba09"
        },
        {
            "description": "Empty string returns None",
            "url": "",
            "expected": None
        },
        {
            "description": "URL ending with slash returns None",
            "url": "https://example.com/downloads/",
            "expected": None
        },
        {
            "description": "Just a slash returns None",
            "url": "/",
            "expected": None
        },
        {
            "description": "URL with query parameters",
            "url": "https://example.com/download/1234567890ABCDEF1234567890ABCDEF12345678.torrent?key=value",
            "expected": "1234567890abcdef1234567890abcdef12345678.torrent?key=value"
        },
        {
            "description": "URL with special characters in filename",
            "url": "https://example.com/ABC123_special-file.torrent",
            "expected": "abc123_special-file.torrent"
        }
    ]

@pytest.fixture
def extract_hash_from_magnet_link_cases():
    """10 test scenarios for extract_hash_from_magnet_link function."""
    return [
        {
            "description": "Standard magnet link",
            "url": "magnet:?xt=urn:btih:1a79b3c1987ae9a456bab4617a9b47c8231e59c7&dn=Get%20Hard%20%282015%29%20%5B1080p%5D",
            "expected": "1a79b3c1987ae9a456bab4617a9b47c8231e59c7"
        },
        {
            "description": "Magnet link with multiple trackers",
            "url": "magnet:?xt=urn:btih:252e48e9b5c9c001549f9ae788a93a9a615ba6b5&dn=South Park&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce",
            "expected": "252e48e9b5c9c001549f9ae788a93a9a615ba6b5"
        },
        {
            "description": "Uppercase hash in magnet link",
            "url": "magnet:?xt=urn:btih:25A72EBFACD520A42F0AB706E34B236586C37C23&dn=Movie",
            "expected": "25a72ebfacd520a42f0ab706e34b236586c37c23"
        },
        {
            "description": "Magnet link with no display name",
            "url": "magnet:?xt=urn:btih:954581bcb144356a4ec6df449b13d3b9fef3e014&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce",
            "expected": "954581bcb144356a4ec6df449b13d3b9fef3e014"
        },
        {
            "description": "Simple magnet with minimal parameters",
            "url": "magnet:?xt=urn:btih:a72a636c31f57146cb8413e2c0ca666ba4d48809",
            "expected": "a72a636c31f57146cb8413e2c0ca666ba4d48809"
        },
        {
            "description": "Empty string returns None",
            "url": "",
            "expected": None
        },
        {
            "description": "Invalid magnet format returns None",
            "url": "not_a_magnet_link",
            "expected": None
        },
        {
            "description": "Magnet without hash returns None",
            "url": "magnet:?dn=Movie%20Name",
            "expected": None
        },
        {
            "description": "Magnet with extra parameters after hash",
            "url": "magnet:?xt=urn:btih:592960ec795bde6b5df18acc76ea33e35aab6306&dn=Imaginationland&tr=multiple&ws=webseed",
            "expected": "592960ec795bde6b5df18acc76ea33e35aab6306"
        },
        {
            "description": "Hash with mixed case letters",
            "url": "magnet:?xt=urn:btih:505F7F593065F73CF580C860CD9B108280570241&dn=Band%20in%20China",
            "expected": "505f7f593065f73cf580c860cd9b108280570241"
        }
    ]

@pytest.fixture
def classify_media_type_cases():
    """10 test scenarios for classify_media_type function."""
    return [
        {
            "description": "TV show with standard SxxExx pattern",
            "title": "South Park S19E07 Naughty Ninjas 1080p HMAX WEB-DL DD5 1 H 264-CtrlHD",
            "expected": "tv_show"
        },
        {
            "description": "Movie with year in parentheses",
            "title": "Get Hard (2015) [1080p]",
            "expected": "movie"
        },
        {
            "description": "TV season with Season keyword",
            "title": "Abbott Elementary (2021) Season 2 S02 (1080p AMZN WEB-DL x265 HEVC 10bit EAC3 5.1 Silence)",
            "expected": "tv_season"
        },
        {
            "description": "TV season with just Sxx pattern",
            "title": "NOVA S51 1080p x265-AMBER EZTV",
            "expected": "tv_season"
        },
        {
            "description": "Movie with year without parentheses",
            "title": "The Dark Knight 2008 [2160p] [4K] [BluRay] [5.1] [YTS.MX]",
            "expected": "movie"
        },
        {
            "description": "TV show with lowercase episode pattern",
            "title": "Rick and Morty s04e01 Edge of Tomorty Rick Die Rickpeat",
            "expected": "tv_show"
        },
        {
            "description": "TV season with complete season pattern",
            "title": "The.Sopranos.S04.1080p.BluRay.x265-RARBG",
            "expected": "tv_season"
        },
        {
            "description": "Unclassifiable content returns None",
            "title": "Random Document File Name Without Patterns",
            "expected": "unknown"
        },
        {
            "description": "Movie with 1900s year",
            "title": "Casablanca (1942) Classic Film Restoration",
            "expected": "movie"
        },
        {
            "description": "TV show with multi-digit season and episode",
            "title": "Doctor Who S12E10 The Timeless Children 1080p",
            "expected": "tv_show"
        },
        {
            "description": "empty string",
            "title": "",
            "expected": "unknown"
        },
        {
            "description": "random text",
            "title": "a34r98[jae5g9;8jzegrt;98ja35g4",
            "expected": "unknown"
        },
        {
            "description": "season with now special characters",
            "title": "The Last of Us S02 1080p x265-ELiTE EZTV",
            "expected": "tv_season"
        }
    ]

@pytest.fixture
def extract_title_cases():
    """10 test scenarios for extract_title function."""
    return [
        {
            "description": "Movie title with year in parentheses",
            "raw_title": "The Dark Knight (2008) [2160p] [4K] [BluRay] [5.1] [YTS.MX]",
            "media_type": "movie",
            "expected": "The Dark Knight"
        },
        {
            "description": "TV show title before episode pattern",
            "raw_title": "South Park S19E07 Naughty Ninjas 1080p HMAX WEB-DL",
            "media_type": "tv_show",
            "expected": "South Park"
        },
        {
            "description": "TV season title before season pattern",
            "raw_title": "Abbott Elementary Season 2 S02 (1080p AMZN WEB-DL)",
            "media_type": "tv_season",
            "expected": "Abbott Elementary"
        },
        {
            "description": "Movie with periods instead of spaces",
            "raw_title": "The.Departed.(2006).[2160p].[4K].[BluRay].[5.1].[YTS.MX]",
            "media_type": "movie",
            "expected": "The Departed"
        },
        {
            "description": "TV show with underscores and dashes",
            "raw_title": "Breaking_Bad-S01E01-Pilot.1080p.BluRay.x265",
            "media_type": "tv_show",
            "expected": "Breaking Bad"
        },
        {
            "description": "Movie with special characters cleaned",
            "raw_title": "Lord[of]the_Rings-(2001)[Extended].mkv",
            "media_type": "movie",
            "expected": "Lord of the Rings"
        },
        {
            "description": "TV season with lowercase pattern",
            "raw_title": "the.office.s01.complete.720p.web.dl",
            "media_type": "tv_season",
            "expected": "the office"
        },
        {
            "description": "Invalid pattern returns None",
            "raw_title": "No Year Or Episode Pattern Here",
            "media_type": "movie",
            "expected": None
        },
        {
            "description": "Complex TV show title with multiple words",
            "raw_title": "It's Always Sunny in Philadelphia S14E01 The Gang Gets Romantic",
            "media_type": "tv_show",
            "expected": "It's Always Sunny in Philadelphia"
        },
        {
            "description": "movie with year in the start of the title",
            "raw_title": "2001 A Space Odyssey (1968) [2160p] [4K] [BluRay] [5.1] [YTS.MX]",
            "media_type": "movie",
            "expected": "2001 A Space Odyssey"
        },
        {
            "description": "movie with year as the title",
            "raw_title": "1917 (2019) [2160p] [4K] [BluRay] [7.1] [YTS.MX]",
            "media_type": "movie",
            "expected": "1917"
        },
        {
            "description": "movie with no brackets around the resolution",
            "raw_title": "Dances with Wolves 1080p AMZN WEB-DL DDP 5 1 H 264-PiRaTeS",
            "media_type": "movie",
            "expected": "Dances with Wolves"
        }
    ]

@pytest.fixture
def extract_year_cases():
    """10 test scenarios for extract_year function."""
    return [
        {
            "description": "Year in parentheses",
            "title": "The Dark Knight (2008) [BluRay]",
            "expected": 2008
        },
        {
            "description": "Year in square brackets",
            "title": "Inception [2010] Director's Cut",
            "expected": 2010
        },
        {
            "description": "Year with dots as delimiters",
            "title": "Pulp.Fiction.1994.Director.Cut",
            "expected": 1994
        },
        {
            "description": "Year with dashes",
            "title": "The-Matrix-1999-Reloaded",
            "expected": 1999
        },
        {
            "description": "Year with underscores",
            "title": "Avatar_2009_Extended_Edition",
            "expected": 2009
        },
        {
            "description": "1900s year",
            "title": "Casablanca (1942) Classic",
            "expected": 1942
        },
        {
            "description": "2000s year",
            "title": "The Avengers (2012) [1080p]",
            "expected": 2012
        },
        {
            "description": "No year pattern returns None",
            "title": "Movie Without Year Information",
            "expected": None
        },
        {
            "description": "Multiple years, first one extracted",
            "title": "Remake (2015) of Classic (1975)",
            "expected": 2015
        }
    ]

@pytest.fixture
def extract_season_from_episode_cases():
    """10 test scenarios for extract_season_from_episode function."""
    return [
        {
            "description": "Standard uppercase SxxExx pattern",
            "title": "Breaking Bad S05E14 Ozymandias 1080p",
            "expected": 5
        },
        {
            "description": "Lowercase sxxexx pattern",
            "title": "the office s02e01 the dundies 720p",
            "expected": 2
        },
        {
            "description": "Season with leading zero",
            "title": "Game of Thrones S01E01 Winter Is Coming",
            "expected": 1
        },
        {
            "description": "Double digit season",
            "title": "Grey's Anatomy S15E23 What I Did for Love",
            "expected": 15
        },
        {
            "description": "Season with spaces around pattern",
            "title": "The Simpsons S32E05 The 7 Beer Itch",
            "expected": 32
        },
        {
            "description": "Mixed case pattern",
            "title": "South Park s19E07 Naughty Ninjas",
            "expected": 19
        },
        {
            "description": "Three digit season",
            "title": "Doctor Who S123E01 Future Episode",
            "expected": 123
        },
        {
            "description": "No episode pattern returns None",
            "title": "Movie Title Without Episode Pattern",
            "expected": None
        },
        {
            "description": "Pattern not properly delimited returns None",
            "title": "NotS01E01Pattern",
            "expected": None
        }
    ]

@pytest.fixture
def extract_episode_from_episode_cases():
    """10 test scenarios for extract_episode_from_episode function."""
    return [
        {
            "description": "Standard uppercase SxxExx pattern",
            "title": "Breaking Bad S05E14 Ozymandias 1080p",
            "expected": 14
        },
        {
            "description": "Lowercase sxxexx pattern",
            "title": "the office s02e08 the dundies extended",
            "expected": 8
        },
        {
            "description": "Episode with leading zero",
            "title": "Game of Thrones S01E01 Winter Is Coming",
            "expected": 1
        },
        {
            "description": "Double digit episode",
            "title": "The Simpsons S15E23 The Last Episode",
            "expected": 23
        },
        {
            "description": "Episode with spaces around pattern",
            "title": "South Park S19E07 Naughty Ninjas HD",
            "expected": 7
        },
        {
            "description": "Mixed case pattern",
            "title": "Doctor Who s12E10 The Timeless Children",
            "expected": 10
        },
        {
            "description": "Three digit episode",
            "title": "Anime Series S01E123 Final Battle",
            "expected": 123
        },
        {
            "description": "No episode pattern returns None",
            "title": "Movie Title Without Episode Pattern",
            "expected": None
        },
        {
            "description": "Pattern not properly delimited returns None",
            "title": "NotS01E01Pattern",
            "expected": None
        }
    ]

@pytest.fixture
def extract_season_from_season_cases():
    """10 test scenarios for extract_season_from_season function."""
    return [
        {
            "description": "Season with word 'Season'",
            "title": "The Office Season 5 Complete 1080p",
            "expected": 5
        },
        {
            "description": "Uppercase S pattern",
            "title": "Breaking Bad S04 Complete BluRay",
            "expected": 4
        },
        {
            "description": "Season with space",
            "title": "Game of Thrones Season 8 Final Season",
            "expected": 8
        },
        {
            "description": "Lowercase s pattern",
            "title": "stranger things s03 complete",
            "expected": 3
        },
        {
            "description": "Double digit season",
            "title": "Grey's Anatomy Season 15 Medical Drama",
            "expected": 15
        },
        {
            "description": "Season without space",
            "title": "Friends Season9 Complete Series",
            "expected": 9
        },
        {
            "description": "S pattern with leading zero",
            "title": "The Sopranos S01 Crime Drama",
            "expected": 1
        },
        {
            "description": "Case insensitive season word",
            "title": "LOST season 6 mystery series",
            "expected": 6
        },
        {
            "description": "No season pattern returns None",
            "title": "Random Movie Title 2020",
            "expected": None
        },
        {
            "description": "Season at beginning",
            "title": "Season 2 of Better Call Saul",
            "expected": 2
        }
    ]

@pytest.fixture
def extract_resolution_cases():
    """10 test scenarios for extract_resolution function."""
    return [
        {
            "description": "Standard 1080p resolution",
            "title": "Movie Title 1080p BluRay x264",
            "expected": "1080p"
        },
        {
            "description": "4K resolution",
            "title": "The Dark Knight 2160p 4K UHD",
            "expected": "2160p"
        },
        {
            "description": "HD 720p resolution",
            "title": "TV Show S01E01 720p WEB-DL",
            "expected": "720p"
        },
        {
            "description": "SD 480p resolution",
            "title": "Classic Movie 480p DVD Rip",
            "expected": "480p"
        },
        {
            "description": "8K resolution",
            "title": "Nature Documentary 4320p 8K HDR",
            "expected": "4320p"
        },
        {
            "description": "Case insensitive matching",
            "title": "Action Movie 1080P WEB DL",
            "expected": "1080P"
        },
        {
            "description": "Resolution at beginning",
            "title": "720p.Movie.Title.2020.BluRay",
            "expected": "720p"
        },
        {
            "description": "Multiple resolutions, first one returned",
            "title": "Movie 1080p upscaled from 720p",
            "expected": "1080p"
        },
        {
            "description": "No resolution returns None",
            "title": "Movie Title Without Resolution Info",
            "expected": None
        },
        {
            "description": "Invalid resolution format returns None",
            "title": "Movie Title 1080 without p",
            "expected": None
        }
    ]

@pytest.fixture
def extract_video_codec_cases():
    """10 test scenarios for extract_video_codec function."""
    return [
        {
            "description": "Standard x264 codec",
            "title": "Movie Title 1080p BluRay x264-GROUP",
            "expected": "x264"
        },
        {
            "description": "HEVC/x265 codec",
            "title": "TV Show S01E01 1080p WEB-DL x265 HEVC",
            "expected": "x265"
        },
        {
            "description": "H.264 with dot",
            "title": "Movie Title 720p WEB-DL H.264",
            "expected": "H.264"
        },
        {
            "description": "H264 without dot",
            "title": "Documentary 1080p HDTV H264",
            "expected": "H264"
        },
        {
            "description": "HEVC codec",
            "title": "4K Movie 2160p UHD BluRay HEVC",
            "expected": "HEVC"
        },
        {
            "description": "XviD codec",
            "title": "Old Movie DVDRip XviD-GROUP",
            "expected": "XviD"
        },
        {
            "description": "AV1 modern codec",
            "title": "New Movie 1080p WEB-DL AV1",
            "expected": "AV1"
        },
        {
            "description": "Case insensitive matching",
            "title": "Movie h.265 encoding test",
            "expected": "h.265"
        },
        {
            "description": "VP9 codec",
            "title": "Web Series 1080p VP9 WebM",
            "expected": "VP9"
        },
        {
            "description": "No codec returns None",
            "title": "Movie Title 1080p BluRay",
            "expected": None
        }
    ]

@pytest.fixture
def extract_audio_codec_cases():
    """10 test scenarios for extract_audio_codec function."""
    return [
        {
            "description": "DDP 5.1 audio",
            "title": "Movie Title 1080p WEB-DL DDP5.1 H264",
            "expected": "DDP5.1"
        },
        {
            "description": "AAC 2.0 audio",
            "title": "TV Show S01E01 720p HDTV AAC2.0",
            "expected": "AAC2.0"
        },
        {
            "description": "AC-3 surround",
            "title": "Movie 1080p BluRay AC-3 x264",
            "expected": "AC-3"
        },
        {
            "description": "DTS-HD audio",
            "title": "Movie 1080p BluRay DTS-HD MA 7.1",
            "expected": "DTS-HD"
        },
        {
            "description": "Atmos audio",
            "title": "Action Movie 2160p UHD Atmos TrueHD",
            "expected": "Atmos"
        },
        {
            "description": "FLAC lossless",
            "title": "Concert Recording 1080p FLAC 2.0",
            "expected": "FLAC"
        },
        {
            "description": "EAC-3 enhanced",
            "title": "Movie 1080p WEB-DL DD+ EAC-3",
            "expected": "EAC-3"
        },
        {
            "description": "Simple AAC",
            "title": "TV Episode 720p WEB AAC",
            "expected": "AAC"
        },
        {
            "description": "DTS without enhancement",
            "title": "Movie 1080p BluRay DTS 5.1",
            "expected": "DTS 5.1"
        },
        {
            "description": "No audio codec returns None",
            "title": "Movie Title 1080p x264",
            "expected": None
        }
    ]

@pytest.fixture
def extract_upload_type_cases():
    """10 test scenarios for extract_upload_type function."""
    return [
        {
            "description": "WEB-DL type",
            "title": "Movie Title 1080p WEB-DL DD5.1 H264",
            "expected": "WEB-DL"
        },
        {
            "description": "BluRay source",
            "title": "Movie 1080p BluRay x264-GROUP",
            "expected": "BluRay"
        },
        {
            "description": "WEBRip type",
            "title": "TV Show S01E01 720p WEBRip x265",
            "expected": "WEBRip"
        },
        {
            "description": "AMZN source",
            "title": "Series 1080p AMZN WEB-DL DDP5.1",
            "expected": "WEB-DL"
        },
        {
            "description": "HMAX source",
            "title": "Movie 1080p HMAX WEB-DL H264",
            "expected": "WEB-DL"
        },
        {
            "description": "PROPER release",
            "title": "Movie 1080p BluRay PROPER x264",
            "expected": "BluRay"
        },
        {
            "description": "REPACK version",
            "title": "TV Show S01E01 REPACK 1080p",
            "expected": None
        },
        {
            "description": "ATVP Apple TV source",
            "title": "Series 1080p ATVP WEB-DL",
            "expected": "WEB-DL"
        },
        {
            "description": "iNTERNAL release",
            "title": "Movie 1080p BluRay iNTERNAL",
            "expected": "BluRay"
        },
        {
            "description": "No upload type returns None",
            "title": "Movie Title 1080p x264",
            "expected": None
        }
    ]

@pytest.fixture
def extract_uploader_cases():
    """10 test scenarios for extract_uploader function."""
    return [
        {
            "description": "CtrlHD uploader",
            "title": "Movie 1080p WEB-DL H264-CtrlHD",
            "expected": "CtrlHD"
        },
        {
            "description": "YTS.MX uploader",
            "title": "Movie (2020) [1080p] [YTS.MX]",
            "expected": "YTS.MX"
        },
        {
            "description": "SuccessfulCrab uploader",
            "title": "TV Show S01E01 1080p WEB H264-SuccessfulCrab",
            "expected": "SuccessfulCrab"
        },
        {
            "description": "EDITH uploader",
            "title": "Show S01E01 1080p WEB h264-EDITH",
            "expected": "EDITH"
        },
        {
            "description": "BAE uploader",
            "title": "Documentary 1080p WEB h264-BAE",
            "expected": "BAE"
        },
        {
            "description": "NTb uploader",
            "title": "Movie 1080p WEB-DL DDP5.1 H264-NTb",
            "expected": "NTb"
        },
        {
            "description": "playWEB uploader",
            "title": "Series S01E01 1080p WEB DD 5.1 H264-playWEB",
            "expected": "playWEB"
        },
        {
            "description": "DiRT uploader",
            "title": "Show 1080p WEB h264 DiRT",
            "expected": "DiRT"
        },
        {
            "description": "FLUX uploader",
            "title": "Movie 1080p WEB-DL H264-FLUX",
            "expected": "FLUX"
        },
        {
            "description": "No uploader returns None",
            "title": "Movie Title 1080p x264",
            "expected": None
        }
    ]