# standard library imports
import re
from typing import List, Optional, Any

# third-party imports
import yaml

################################################################################
# initial set-up and parameters
################################################################################

# read in config/string-special-conditions.yaml and store ad dict
with open('config/string-special-conditions.yaml', 'r') as file:
	special_conditions = yaml.safe_load(file)

################################################################################
# validation operations
################################################################################

def validate_dict(entry_dict:dict):
	"""
	Validate that all entries are not None, empty strings, empty lists, etc.
	:param entry_dict: any input dict
	:raises: ValueError if any entry is None, empty string, empty list, etc.
	"""
	if not all(
		entry_dict.values()):  # Checks for None, empty strings, empty lists, etc.
		raise ValueError(f"Invalid entry data: {entry_dict}")


################################################################################
# hash value operations
################################################################################

def extract_hash_from_direct_download_url(href:str) -> str:
	hash = href.split('/')[-1].lower()
	return hash

def extract_hash_from_magnet_link(href:str) -> str:
	hash = href.split('urn:btih:')[1].split('&')[0].lower()
	return hash

################################################################################
# title operations
################################################################################

def classify_media_type(raw_title:str) -> Optional[str]:
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
		return None

def extract_title(raw_title: str, media_type: str) -> str:
	"""
	Extract and format the movie name for OMDb API queries
	:param raw_title: raw_title string of media item
	:param media_type: type of collection, either "movie" or "tv_show"
	:return: string of movie name formatted for OMDb API
    """
	clean_title = raw_title

	if media_type == 'movie':
		# remove suffixes to the movie title
		# Remove quality info and encoding info that comes after the year
		# Match year pattern (with or without parentheses) and everything after
		clean_title = re.split(r'(?:^|\s)\(?(?:19|20)\d{2}\)?[^\w]*.*$', clean_title)[0]
	
	elif media_type == 'tv_show':
		# remove suffixes to tv_show_name
		## remove quality info and encoding info
		clean_title = re.sub(r'\d{3,4}p.*$', '', clean_title)

		# get everything before the SxxExx pattern
		clean_title = re.split(r'[Ss]\d{1,4}[Ee]\d{1,4}', clean_title)[0]

	elif media_type == 'tv_season':
		# Remove quality info and encoding info that comes after season pattern
		# Matches either "Season XX" or "SXX" pattern (case insensitive)
		clean_title = re.split(r'(?:[Ss]eason\s+\d{1,2}|[Ss]\d{1,2})[^\w]*.*$',
					 clean_title)[0]

		# Remove any year pattern that follows the shows name
		clean_title = re.sub(r'\s*\([12][90]\d{2}\)\s*', ' ', clean_title)

		# Remove any year pattern that's adjacent to show name
		clean_title = re.sub(r'[.\s_-]*(?:19|20)\d{2}[.\s_-]*', '.', clean_title)

	# process the title once prefixes and suffixes are removed
	## Replace periods with spaces
	clean_title = re.sub('[._\\-+]', ' ', clean_title)

	## remove trailing or leading white spice
	clean_title = clean_title.strip()

	# handle special cases
	## Special case: Add apostrophe for "Its" -> "It's"
	# clean_title = clean_title.replace('Its ', "It's ")

	return clean_title

################################################################################
# parse time
################################################################################

def extract_year(raw_title: str) -> Optional[str]:
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

def extract_season_from_episode(raw_title: str) -> Optional[str]:
	pattern = re.compile(r'[. ]s(\d{1,4})e\d{1,4}[. ]', re.IGNORECASE)
	match = pattern.search(raw_title)
	return int(match.group(1)) if match else None

def extract_episode_from_episode(raw_title: str) -> Optional[str]:
	pattern = re.compile(r'[. ]s\d{1,4}e(\d{1,4})[. ]', re.IGNORECASE)
	match = pattern.search(raw_title)
	return int(match.group(1)) if match else None

def extract_season_from_season(raw_title: str) -> Optional[str]:
	pattern = re.compile(r'(?:S|Season\s?)(\d{1,2})', re.IGNORECASE)
	match = pattern.search(raw_title)
	return int(match.group(1)) if match else None


################################################################################
# parse video, audio, and torrent metadata
################################################################################

def extract_resolution(raw_title: str)-> Optional[str]:
	pattern = re.compile(r'(\d{3,4}p)')
	match = pattern.search(raw_title)
	return match.group(0) if match else None


def extract_video_codec(raw_title: str)-> Optional[str]:
	pattern = re.compile(r'\b(h[. ]?264|x264|x265|h[. ]?265|hevc|xvid|divx|vp8|vp9|av1|mpeg[- ]?[24]|wmv|avc)\b', re.IGNORECASE)
	match = pattern.search(raw_title)
	return match.group(0) if match else None


def extract_audio_codec(raw_title: str)-> Optional[str]:
	pattern = re.compile(r'\b(DDP?[. ]?[257]\.1|AAC[. ]?[257]\.1|DDP|AAC|AC-?3|E-?AC-?3|TrueHD|DTS(?:-?HD)?(?:[. ]?[257]\.1)?|FLAC|MP3|WMA|PCM|LPCM|Atmos|OGG|Vorbis|ALAC|EAC-?3)\b', re.IGNORECASE)
	match = pattern.search(raw_title)
	return match.group(0) if match else None


def extract_upload_type(raw_title: str)-> Optional[str]:
	pattern = re.compile(r'\b(WEB[-. ]?DL|WEB(?:Rip)?|MAX|AMZN|ATVP|HULU|HMAX|BluRay|WEBRip|PROPER|iNTERNAL|MULTI|REPACK|FINAL)\b', re.IGNORECASE)
	match = pattern.search(raw_title)
	return match.group(0) if match else None


def extract_uploader(raw_title: str)-> Optional[str]:
	pattern = re.compile(r'(?:[-.](?:DiRT|SuccessfulCrab|EDITH|FLUX|CtrlHD|BAE|NTb|LAZYCUNTS|HiggsBoson|RUBiK|PSA|\[YTS\.MX\]|GGEZ|playWEB))(?:\[rarbg\])?$', re.IGNORECASE)
	match = pattern.search(raw_title)
	return match.group(0) if match else None


################################################################################
# end of parse_element.py
################################################################################