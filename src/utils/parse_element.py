# standard library imports
import re

################################################################################
# hash value operations
################################################################################

def extract_hash_from_direct_download_url(href: str) -> str | None:
    """
    Extract filename/hash from direct download URL using regex.

    :param href: URL string to extract hash from
    :return: Lowercase filename/hash string or None if not found
    """
    if not href:
        return None

    # Pattern to match the last segment after the final slash
    # Captures everything that's not a slash, at the end of the string
    pattern = r'/([^/]+)$'
    match = re.search(pattern, href)

    if match:
        result = match.group(1).lower()
        return result if result else None

    # If no slash found, return None
    return None


def extract_hash_from_magnet_link(href: str) -> str | None:
    """
    Extract hash from magnet link using regex for robust parsing.

    :param href: Magnet link string to extract hash from
    :return: Lowercase hash string or None if not found
    """
    if not href:
        return None

    # Pattern to match urn:btih: followed by 40 hex characters (SHA-1 hash)
    pattern = r'urn:btih:([a-fA-F0-9]{40})'
    match = re.search(pattern, href)

    return match.group(1).lower() if match else None


################################################################################
# title operations
################################################################################

def classify_media_type(raw_title:str) -> str | None:
    """
    Classify media as either movie, tv_show, or tv_season based on the title
    :param raw_title: raw title string of media item
    :return: string indicating media type
    """
    # Core patterns
    # TV show must have SxxExx pattern (case insensitive)
    tv_pattern = r'[Ss]\d{1,4}[Ee]\d{1,4}'

    # TV season can have either "season X" or "sXX" pattern (case insensitive)
    # Looks for season/s followed by 1-2 digits, not followed by episode pattern
    season_pattern = r'(?:[Ss]eason\s+\d{1,4}|[Ss]\d{1,4})'

    # Movie pattern now accounts for years with or without parentheses
    # Matches 19xx or 20xx with similar delimiters on both sides
    movie_pattern = r'(?:^|\W)((?:19|20)\d{2})(?:$|\W)'

    if re.search(tv_pattern, raw_title):
        return "tv_show"
    elif re.search(season_pattern, raw_title):
        return "tv_season"
    elif re.search(movie_pattern, raw_title):
        return "movie"
    else:
        return "unknown"


def extract_title(raw_title: str, media_type: str) -> str | None:
    """
    extract and format the title of a media object given a more complicated
        media string; this function should return None if any issue occurred
        with makes the extract title invalid

    :param raw_title: raw_title string of media item
    :param media_type: type of collection, either "movie", "tv_show", or "tv_season"
    :return: string of the cleaned media title
    """
    cleaned_title = None

    if media_type == 'movie':
        # Try to match everything before year pattern first
        match = re.search(r'(.+?)[\s._-]*\((?:19|20)\d{2}\)', raw_title)
        if match:
            cleaned_title = match.group(1).strip()
        else:
            # If no year found, take everything before resolution pattern
            match = re.search(r'(.+?)[\s._-]*\d{3,4}p', raw_title, re.IGNORECASE)
            if match:
                cleaned_title = match.group(1).strip()
    elif media_type == 'tv_show':
        # get everything before the SxxExx pattern
        match = re.search(r'(.+?)s\d{1,4}e\d{1,4}', raw_title, re.IGNORECASE)
        if match:
            cleaned_title = match.group(1).strip()
    elif media_type == 'tv_season':
        # get everything before the season pattern
        match = re.search(r'(.+?)(?:season.{1,4}\d{1,4}|s\d{1,4})', raw_title, re.IGNORECASE)
        if match:
            cleaned_title = match.group(1).strip()

    # determine if initial title extraction was successful, and if not return none
    if cleaned_title is None or cleaned_title.strip() == "":
        return None
    else:
        # Replace special characters with spaces
        cleaned_title = re.sub('[._\\-+()\\[\\]]', ' ', cleaned_title)
        # remove trailing or leading white spice
        cleaned_title = cleaned_title.strip()

    return cleaned_title


################################################################################
# parse time
################################################################################

def extract_year(raw_title: str) -> str | None:
    # Pattern matches:
    # - Opening delimiter: (, [, ., -, or _
    # - 19 or 20 followed by two digits
    # - Closing delimiter: ), ], ., -, or _
    pattern = re.compile(r'[\(\[\.\-_]((?:19|20)\d{2})[\)\]\.\-_]')
    match = pattern.search(raw_title)
    return int(match.group(1)) if match else None


################################################################################
# parse season and episode
################################################################################

def extract_season_from_episode(raw_title: str) -> str | None:
    pattern = re.compile(r'[. ]s(\d{1,4})e\d{1,4}[. ]', re.IGNORECASE)
    match = pattern.search(raw_title)
    return int(match.group(1)) if match else None


def extract_episode_from_episode(raw_title: str) -> str | None:
    pattern = re.compile(r'[. ]s\d{1,4}e(\d{1,4})[. ]', re.IGNORECASE)
    match = pattern.search(raw_title)
    return int(match.group(1)) if match else None


def extract_season_from_season(raw_title: str) -> str | None:
    pattern = re.compile(r'(?:S|Season\s?)(\d{1,2})', re.IGNORECASE)
    match = pattern.search(raw_title)
    return int(match.group(1)) if match else None


################################################################################
# parse video, audio, and torrent metadata
################################################################################

def extract_resolution(raw_title: str)-> str | None:
    pattern = re.compile(r'(\d{3,4}p)', re.IGNORECASE)
    match = pattern.search(raw_title)
    return match.group(0) if match else None


def extract_video_codec(raw_title: str) -> str | None:
    # List of video codecs to search for
    video_codecs = [
        'h[. ]?264',
        'x264',
        'x265',
        'h[. ]?265',
        'hevc',
        'xvid',
        'divx',
        'vp8',
        'vp9',
        'av1',
        'mpeg[- ]?[24]',
        'wmv',
        'avc'
    ]

    # Create pattern with word boundaries and case insensitive matching
    codec_pattern = r'\b(' + '|'.join(video_codecs) + r')\b'
    pattern = re.compile(codec_pattern, re.IGNORECASE)

    match = pattern.search(raw_title)
    return match.group(0) if match else None


def extract_audio_codec(raw_title: str) -> str | None:
    # List of audio codecs to search for
    audio_codecs = [
        'DDP?[. ]?[257]\.1',
        'AAC[. ]?[257]\.1',
        'DDP',
        'AAC',
        'AAC2.0',
        'AC-?3',
        'E-?AC-?3',
        'TrueHD',
        'DTS(?:-?HD)?(?:[. ]?[257]\.1)?',
        'FLAC',
        'MP3',
        'WMA',
        'PCM',
        'LPCM',
        'Atmos',
        'OGG',
        'Vorbis',
        'ALAC',
        'EAC-?3'
    ]

    # Create pattern with word boundaries and case insensitive matching
    codec_pattern = r'\b(' + '|'.join(audio_codecs) + r')\b'
    pattern = re.compile(codec_pattern, re.IGNORECASE)

    match = pattern.search(raw_title)
    return match.group(0) if match else None


def extract_upload_type(raw_title: str) -> str | None:
    # List of upload types/sources to search for
    upload_types = [
        'WEB[-. ]?DL',
        'WEB(?:Rip)?',
        'BluRay',
        'WEBRip',
    ]

    # Create pattern with word boundaries and case insensitive matching
    type_pattern = r'\b(' + '|'.join(upload_types) + r')\b'
    pattern = re.compile(type_pattern, re.IGNORECASE)

    match = pattern.search(raw_title)
    return match.group(0) if match else None


def extract_uploader(raw_title: str) -> str | None:
    # List of uploader groups to search for (at end of string with prefix)
    uploaders = [
        'DiRT',
        'SuccessfulCrab',
        'EDITH',
        'FLUX',
        'CtrlHD',
        'BAE',
        'NTb',
        'LAZYCUNTS',
        'HiggsBoson',
        'RUBiK',
        'PSA',
        'YTS\.MX',
        'GGEZ',
        'playWEB',
        'MeGusta',
        'ELiTE'
    ]

    # Create pattern for uploaders with optional separators
    uploader_pattern = r'(?:[-.\[\s]*)(' + '|'.join(uploaders) + r')(?:[-.\]\s]*)'
    pattern = re.compile(uploader_pattern, re.IGNORECASE)

    match = pattern.search(raw_title)
    return match.group(1) if match else None


################################################################################
# end of parse_element.py
################################################################################