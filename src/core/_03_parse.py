# standard library imports
import logging
import re

# third-party imports
from dotenv import load_dotenv
import pandas as pd

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# config
# ------------------------------------------------------------------------------

# load environment variables
load_dotenv()

# logger config
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# title parse helper functions
# ------------------------------------------------------------------------------

def parse_item(media_item, media_type):
    """
    Parse the title of a new item to extract relevant information
    :param media_item: element from rss feed data frame
    :param media_type: either "movie" or "tv_show"
    :return: new item series to be inserted into master data frame
    """
    # define regex patterns for both item types
    resolution_pattern = re.compile(r'(\d{3,4}p)')
    video_codec_pattern = re.compile(r'\b(h[. ]?264|x264|x265|h[. ]?265|hevc|xvid|divx|vp8|vp9|av1|mpeg[- ]?[24]|wmv|avc)\b', re.IGNORECASE)
    upload_type_pattern = re.compile(r'\b(WEB[-. ]?DL|WEB(?:Rip)?|MAX|AMZN|ATVP|HULU|HMAX|BluRay|WEBRip|PROPER|iNTERNAL|MULTI|REPACK|FINAL)\b', re.IGNORECASE)
    audio_codec_pattern = re.compile(r'\b(DDP?[. ]?[257]\.1|AAC[. ]?[257]\.1|DDP|AAC|AC-?3|E-?AC-?3|TrueHD|DTS(?:-?HD)?(?:[. ]?[257]\.1)?|FLAC|MP3|WMA|PCM|LPCM|Atmos|OGG|Vorbis|ALAC|EAC-?3)\b', re.IGNORECASE)
    uploader_pattern = re.compile(r'(?:[-.](?:DiRT|SuccessfulCrab|EDITH|FLUX|CtrlHD|BAE|NTb|LAZYCUNTS|HiggsBoson|RUBiK|PSA|\[YTS\.MX\]|GGEZ|playWEB))(?:\[rarbg\])?$', re.IGNORECASE)

    # define regex for movie items only
    title_pattern = re.compile(r'^(.*?) \(')
    year_pattern = re.compile(r'\((\d{4})\)')

    # define regex for tv show items only
    tv_show_title_pattern = re.compile(r'^(.*?)(?:[. ]S\d{1,3}|[. ]Season)', re.IGNORECASE)
    tv_show_season_pattern = re.compile(r'[. ]s(\d{1,3})e', re.IGNORECASE)
    episode_pattern = re.compile(r'e(\d{1,3})[. ]', re.IGNORECASE)

    # define regex for tv season items only
    tv_season_season_pattern = re.compile(r'(?:S|Season\s?)(\d{1,2})', re.IGNORECASE)

    # search for patterns in both item types
    title = media_item['raw_title']
    if resolution_pattern.search(title) is not None:
        media_item['resolution'] = resolution_pattern.search(title).group(0)
    if video_codec_pattern.search(title) is not None:
        media_item['video_codec'] = video_codec_pattern.search(title).group(0)
    if upload_type_pattern.search(title) is not None:
        media_item['upload_type'] = upload_type_pattern.search(title).group(0)
    if audio_codec_pattern.search(title) is not None:
        media_item['audio_codec'] = audio_codec_pattern.search(title).group(0)
    if uploader_pattern.search(title) is not None:
        media_item['uploader'] = uploader_pattern.search(title).group(0)
        media_item['uploader'] = media_item['uploader'].strip('[').strip(']')

    # search for movie only patterns
    if media_type == 'movie':
        if title_pattern.search(title) is not None:
            media_item['movie_title'] = title_pattern.search(title).group(1)
        if year_pattern.search(title) is not None:
            media_item['release_year'] = year_pattern.search(title).group(1)
    # search for tv show only patterns
    elif media_type == 'tv_show':
        if tv_show_title_pattern.search(title) is not None:
            media_item['tv_show_name'] = tv_show_title_pattern.search(title).group(1)
            media_item['tv_show_name'] = media_item['tv_show_name'].replace('.', ' ').strip()
        if tv_show_season_pattern.search(title) is not None:
            media_item['season'] = tv_show_season_pattern.search(title).group(0)
            media_item['season'] = re.sub(r'\D', '', media_item['season'])
        if episode_pattern.search(title) is not None:
            media_item['episode'] = episode_pattern.search(title).group(0)
            media_item['episode'] = re.sub(r'\D', '', media_item['episode'])
    elif media_type == 'tv_season':
        if tv_show_title_pattern.search(title) is not None:
            media_item['tv_show_name'] = tv_show_title_pattern.search(title).group(1)
            media_item['tv_show_name'] = media_item['tv_show_name'].replace('.', ' ').strip()
        if tv_season_season_pattern.search(title) is not None:
            media_item['season'] = tv_season_season_pattern.search(title).group(0)
            media_item['season'] = re.sub(r'\D', '', media_item['season'])
    else:
        raise ValueError("Invalid item type. Must be 'movie' or 'tv_show'")

    return media_item

def verify_parse(
    media_item,
    media_type
):
    verified = False
    verification_fault = []

    release_year_populated = False
    resolution_populated = False

    movie_title_populated = False

    tv_show_name_populated = False
    season_populated = False
    episode_populated = False

    if media_type == 'movie':
        if media_item['movie_title'] is not None and media_item['movie_title'] != '':
            movie_title_populated = True
        else:
            verification_fault.append('movie_title not populated')
        if media_item['release_year'] is not None and media_item['release_year'] != '':
            release_year_populated = True
        else:
            verification_fault.append('release_year not populated')
        if media_item['resolution'] is not None and media_item['resolution'] != '':
            resolution_populated = True
        else:
            verification_fault.append('resolution not populated')
        if movie_title_populated and release_year_populated and resolution_populated:
            verified = True
    elif media_type == 'tv_show':
        if media_item['tv_show_name'] is not None and media_item['tv_show_name'] != '':
            tv_show_name_populated = True
        else:
            verification_fault.append('tv_show_name not populated')
        if media_item['season'] is not None and media_item['season'] != '':
            season_populated = True
        else:
            verification_fault.append('season not populated')
        if media_item['episode'] is not None and media_item['episode'] != '':
            episode_populated = True
        else:
            verification_fault.append('episode not populated')
        if tv_show_name_populated and season_populated and episode_populated:
            verified = True
    elif media_type == 'tv_season':
        if media_item['tv_show_name'] is not None and media_item['tv_show_name'] != '':
            tv_show_name_populated = True
        else:
            verification_fault.append('tv_show_name not populated')
        if media_item['season'] is not None and media_item['season'] != '':
            season_populated = True
        else:
            verification_fault.append('season not populated')
        if tv_show_name_populated and season_populated:
            verified = True
    else:
        logging.error("invalid verification type. Must be 'movie' or 'tv_show'")
        raise ValueError("invalid verification type. Must be 'movie' or 'tv_show'")

    output = {
        'verified': verified,
        'verification_fault': verification_fault

    }

    return output

# ------------------------------------------------------------------------------
# full title parse pipeline
# ------------------------------------------------------------------------------

def parse_media(media_type):
    """
    Full ingest pipeline for either movies or tv shows
    :param media_type: either "movie" or "tv_show"
    :return:
    """
    #media_type='movie'
    #media_type='tv_season'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='ingested'
    )

    media_parsed = pd.DataFrame()

    # iterate through all new movies, parse data from the title and add to new dataframe
    if len(media) > 0:
        media_parsed = media.copy().iloc[0:0]

        for index in media.index:
            try:
                parsed_item = parse_item(
                    media_item=media.loc[index].copy(),
                    media_type=media_type
                )
                # confirm that parsing is complete and all essential values are populated
                parse_result = verify_parse(
                    media_item=parsed_item,
                    media_type=media_type
                )
                if parse_result['verified']:
                    media_parsed = pd.concat([media_parsed, parsed_item.to_frame().T])
                    logging.info(f"parsed: {media.loc[index, 'raw_title']}")
                else:
                    logging.error(f"failed to parse: {media.loc[index, 'raw_title']}")
                    logging.error(f"parse_media_items error: {parse_result['verification_fault']}")
            except Exception as e:
                logging.error(f"parse_media_items error: {e}")
                logging.error(f"failed to parse: {media.loc[index, 'raw_title']}")

    if len(media_parsed) > 0:
        # write parsed data back to the database
        utils.update_db_media_table(
            media_type=media_type,
            media_old=media,
            media_new=media_parsed
        )

        # update status of successfully parsed items
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=media_parsed.index.tolist(),
            new_status='parsed'
        )

# ------------------------------------------------------------------------------
# end of _03_parse.py
# ------------------------------------------------------------------------------