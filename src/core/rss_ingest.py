from dotenv import load_dotenv
import feedparser
import os
import pandas as pd
import pickle
from src.utils import logger
import re

# load environment variables
load_dotenv()


# ------------------------------------------------------------------------------
# rss ingest helper functions
# ------------------------------------------------------------------------------


def rss_ingest(rss_url):
    """
    ping rss feed and store the input
    :param rss_url: url of rss feed
    :return:
    """
    # ping rss feed
    feed = feedparser.parse(rss_url)

    # print terminal message
    logger("ingesting from: " + str(feed.channel.title))

    return feed


def rss_entries_to_dataframe(feed, feed_type):
    """
    Convert RSS feed entries into a pandas DataFrame
    :param feed: extract rss feed
    :param feed_type: type of feed, either "movie" or "tv_show"
    :return: DataFrame containing the RSS feed entries
    """
    # Extract the entries
    entries = feed['entries']

    # Extract relevant fields from each entry
    extracted_data = []

    if feed_type == 'movie':
        for entry in entries:
            extracted_data.append({
                'hash': entry.links[1].href.split('/')[-1],
                'raw_title': entry.title,
                'torrent_link': entry.links[1].href,
                'published_timestamp': entry.published,
            })
    elif feed_type == 'tv_show':
        for entry in entries:
            extracted_data.append({
                'hash': entry.get('tv_info_hash'),
                'tv_show_name': entry.get('tv_show_name'),
                'magnet_link': entry.get('link'),
                'published_timestamp': entry.get('published'),
                'summary': entry.get('summary'),
                'raw_title': entry.get('title'),
                'feed_id': entry.get('id'),
                'tv_show_id': entry.get('tv_show_id'),
                'tv_episode_id': entry.get('tv_episode_id'),
                'tv_external_id': entry.get('tv_external_id'),
            })
    else:
        raise ValueError("Invalid feed type. Must be 'movie' or 'tv_show'")

    # convert all of the hash values to lower case
    for entry in extracted_data:
        entry['hash'] = entry['hash'].lower()

    # Convert extracted data to DataFrame
    feed_items = pd.DataFrame(extracted_data)
    feed_items.set_index('hash', inplace=True)

    return feed_items


def parse_new_item(new_item, item_type):
    """
    Parse the title of a new item to extract relevant information
    :param new_item: element from rss feed data frame
    :param item_type: either "movie" or "tv_show"
    :return: new item series to be inserted into master data frame
    """
    # define regex patterns for both item types
    resolution_pattern = re.compile(r'(\d{3,4}p)')
    video_codec_pattern = re.compile(r'\b(h264|x264|x265|H 264|H 265)\b', re.IGNORECASE)
    upload_type_pattern = re.compile(r'\b(WEB DL|WEB|MAX|AMZN)\b', re.IGNORECASE)
    audio_codec_pattern = re.compile(r'\b(DDP5\.1|AAC5\.1|DDP|AAC)\b', re.IGNORECASE)
    uploader_pattern = re.compile(r'\[YTS\.MX\]$')

    # define regex for movie items only
    title_pattern = re.compile(r'^(.*?) \(')
    year_pattern = re.compile(r'\((\d{4})\)')

    # define regex for tv show items only
    season_pattern = re.compile(r'S(\d{2})E')
    episode_pattern = re.compile(r'E(\d{2})')

    # search for patterns in both item types
    title = new_item['raw_title']
    if resolution_pattern.search(title) is not None:
        new_item['resolution'] = resolution_pattern.search(title).group(0)
    if video_codec_pattern.search(title) is not None:
        new_item['video_codec'] = video_codec_pattern.search(title).group(0)
    if upload_type_pattern.search(title) is not None:
        new_item['upload_type'] = upload_type_pattern.search(title).group(0)
    if audio_codec_pattern.search(title) is not None:
        new_item['audio_codec'] = audio_codec_pattern.search(title).group(0)
    if uploader_pattern.search(title) is not None:
        new_item['uploader'] = uploader_pattern.search(title).group(0)
        new_item['uploader'] = new_item['uploader'].strip('[').strip(']')

    # search for movie only patterns
    if item_type == 'movie':
        if title_pattern.search(title) is not None:
            new_item['movie_title'] = title_pattern.search(title).group(1)
        if year_pattern.search(title) is not None:
            new_item['release_year'] = year_pattern.search(title).group(1)
    # search for tv show only patterns
    elif item_type == 'tv_show':
        if season_pattern.search(title) is not None:
            new_item['season'] = season_pattern.search(title).group(0)
            new_item['season'] = re.sub(r'\D', '', new_item['season'])
        if episode_pattern.search(title) is not None:
            new_item['episode'] = episode_pattern.search(title).group(0)
            new_item['episode'] = re.sub(r'\D', '', new_item['episode'])
    else:
        raise ValueError("Invalid item type. Must be 'movie' or 'tv_show'")

    return new_item


# ------------------------------------------------------------------------------
# full ingest for either element type
# ------------------------------------------------------------------------------

def rss_full_ingest(ingest_type):
    """
    Full ingest pipeline for either movies or tv shows
    :param ingest_type: either "movie" or "tv_show"
    :return:
    """
    # read in existing data based on ingest_type
    if ingest_type == 'movie':
        master_df_dir = './data/movies.pkl'
    elif ingest_type == 'tv_show':
        master_df_dir = './data/tv_shows.pkl'
    else:
        raise ValueError("Invalid ingest type. Must be 'movie' or 'tv_show'")

    with open(master_df_dir, 'rb') as file:
        master_df = pickle.load(file)

    # retrieve rss feed based on ingest_type
    if ingest_type == 'movie':
        rss_url = os.getenv('movie_rss_url')
    elif ingest_type == 'tv_show':
        rss_url = os.getenv('tv_rss_url')

    feed = rss_ingest(rss_url)

    # convert feed to data frame
    feed_items = rss_entries_to_dataframe(
        feed=feed,
        feed_type=ingest_type
    )

    new_hashes = feed_items.index.difference(master_df.index)

    # iterate through all new movies, parse data from the title and add to the main data frame
    if len(new_hashes) > 0:
        new_items = feed_items.loc[new_hashes]

        for index in new_items.index:
            try:
                new_item = parse_new_item(
                    new_item=new_items.loc[index].copy(),
                    item_type=ingest_type
                )
                master_df.loc[index] = new_item
                # set the status at the current loop index value to ingested
                master_df.loc[index, 'status'] = 'ingested'
                logger(f"ingested: {master_df.loc[index, 'raw_title']}")
            except:
                logger(f"failed to ingest: {new_items.loc[index, 'raw_title']}")

    # Save the updated tv_shows DataFrame
    with open(master_df_dir, 'wb') as file:
        pickle.dump(master_df, file)

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------