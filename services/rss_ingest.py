from re import search
import feedparser
import pandas as pd
import re
import json

# ------------------------------------------------------------------------------
# read in static parameters
# ------------------------------------------------------------------------------

# read in filter_parameters.json and store as dict
with open('./config/parameters.json') as f:
    parameters = json.load(f)

# ------------------------------------------------------------------------------
# yts rss ingest helper functions
# ------------------------------------------------------------------------------

def yts_rss_ingest(rss_url):
    """
    ping rss feed and store the input
    :param rss_url: url of rss feed
    :return:
    """
    # ping rss feed
    feed = feedparser.parse(rss_url)

    # store the input
    print(feed.feed.title)
    print(feed.feed.updated)

    return feed


def yts_rss_entries_to_dataframe(entries):
    """
    Convert RSS feed entries into a pandas DataFrame
    :param entries: List of RSS feed entries
    :return: DataFrame containing the RSS feed entries
    """
    # Extract relevant fields from each entry
    data = []
    for entry in entries:
        data.append({
            'entry_title': entry.title,
            'hash_id': entry.links[1].href.split('/')[-1],
            'link': entry.link,
            'published': entry.published,
            'updated': entry.updated,
            'guid': entry.guid
        })

    # Create DataFrame from the extracted data
    df = pd.DataFrame(data)
    return df


def parse_movie_info(movie_strings):
    # Define the columns for the DataFrame
    columns = ['original_string', 'movie_title', 'release_year', 'resolution', 'upload_format', 'audio_format', 'encoding', 'uploader']
    data = []

    # Define regex patterns for each field
    title_pattern = re.compile(r'^(.*?) \(')
    year_pattern = re.compile(r'\((\d{4})\)')
    resolution_pattern = re.compile(r'\[(\d{3,4}p)\]')
    upload_format_pattern = re.compile(r'\[(BluRay|WEBRip)\]')
    audio_format_pattern = re.compile(r'\[(5\.1|10bit)\]')
    encoding_pattern = re.compile(r'\[(x264|x265)\]')
    uploader_pattern = re.compile(r'\[YTS\.MX\]$')

    for string in movie_strings:
        movie_title = title_pattern.search(string)
        release_year = year_pattern.search(string)
        resolution = resolution_pattern.search(string)
        upload_format = upload_format_pattern.search(string)
        audio_format = audio_format_pattern.search(string)
        encoding = encoding_pattern.search(string)
        uploader = uploader_pattern.search(string)

        # Append the parsed data to the list
        data.append([
            string,
            movie_title.group(1).strip() if movie_title else None,
            release_year.group(1) if release_year else None,
            resolution.group(1) if resolution else None,
            upload_format.group(1) if upload_format else None,
            audio_format.group(1) if audio_format else None,
            encoding.group(1) if encoding else None,
            'YTS.MX' if uploader else None
        ])

    # Create DataFrame from the parsed data
    df = pd.DataFrame(data, columns=columns)
    return df


# ------------------------------------------------------------------------------
# full movie ingest pipeline
# ------------------------------------------------------------------------------

# website to generate rss feed https://yts.torrentbay.st/rss-guide

def ingest_via_yts():
    # retrieve movie rss feed
    rss_url = "https://yts.mx/rss/"
    feed = yts_rss_ingest(rss_url)

    # covert to data frame
    feed_df = yts_rss_entries_to_dataframe(feed['entries'])

    # parse entry_title field for additional data fields
    parsed_df = parse_movie_info(feed_df["entry_title"])

    # merge feed_df and parsed_df but to do add any redundant columns
    feed_df = pd.concat([feed_df, parsed_df], axis=1)

    # save feed_df locally in a format that makes sense
    feed_df.to_csv('feed_df.csv')


# ------------------------------------------------------------------------------
# show rss ingest helper functions
# ------------------------------------------------------------------------------


def tv_show_rss_ingest(rss_url = parameters["tv_rss_url"]):
    """
    ping rss feed and store the input
    :param rss_url: url of rss feed
    :return:
    """
    # ping rss feed
    feed = feedparser.parse(rss_url)

    # store the input
    print(feed.channel.title)

    # store raw rss feed locally for analysis as json
    with open('./data/show_feed.json', 'w') as f:
        json.dump(feed, f, indent=4)

    return feed


def tv_show_rss_entries_to_dataframe(tv_show_feed):
    """
    Convert RSS feed entries into a pandas DataFrame
    :param feed: List of RSS feed entries
    :param tv_shows: empty data frame to store tv show data
    :return: DataFrame containing the RSS feed entries
    """
    # Extract the entries
    entries = tv_show_feed['entries']

    # Extract relevant fields from each entry
    extracted_data = []
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

    # convert all of the hash values to lower case
    for entry in extracted_data:
        entry['hash'] = entry['hash'].lower()

    # Convert extracted data to DataFrame
    feed_tv_shows = pd.DataFrame(extracted_data)
    feed_tv_shows.set_index('hash', inplace=True)

    return feed_tv_shows


def parse_tv_shows(new_tv_show):
    # Define regex patterns for each field
    season_pattern = re.compile(r'S(\d{2})E')
    episode_pattern = re.compile(r'E(\d{2})')
    resolution_pattern = re.compile(r'(\d{3,4}p)')
    video_codec_pattern = re.compile(r'\b(h264|x264|x265|H 264|H 265)\b', re.IGNORECASE)
    upload_type_pattern = re.compile(r'\b(WEB DL|WEB|MAX|AMZN)\b', re.IGNORECASE)
    audio_codec_pattern = re.compile(r'\b(DDP5\.1|AAC5\.1|DDP|AAC)\b', re.IGNORECASE)

    # search for patterns
    title = new_tv_show['raw_title']
    if season_pattern.search(title).group(0) is not None:
        new_tv_show['season'] = season_pattern.search(title).group(0)
    if () is not None:
        new_tv_show['season'] = ()
    if () is not None:
        new_tv_show['season'] = ()
    if () is not None:
        new_tv_show['season'] = ()
    if () is not None:
        new_tv_show['season'] = ()
    if () is not None:
        new_tv_show['season'] = ()

    new_tv_show['episode'] = episode_pattern.search(title).group(0)
    new_tv_show['resolution'] = resolution_pattern.search(title).group(0)
    new_tv_show['video_codec'] = video_codec_pattern.search(title).group(0)
    new_tv_show['upload_type'] = upload_type_pattern.search(title).group(0)
    new_tv_show['audio_codec'] = audio_codec_pattern.search(title).group(0)

    return new_tv_show


# ------------------------------------------------------------------------------
# rss ingest for tv
# ------------------------------------------------------------------------------

def tv_show_full_ingest():
    # read in existing show data
    tv_shows = pd.read_csv('./data/tv_shows.csv')
    tv_shows.set_index('hash', inplace=True)

    # retrieve rss feed
    tv_show_feed = tv_show_rss_ingest()

    # convert feed items to data frame
    feed_tv_shows = tv_show_rss_entries_to_dataframe(tv_show_feed)

    # add tv_shows which have not been ingested to main data frame
    new_hashes = feed_tv_shows.index.difference(tv_shows.index)

    if len(new_hashes) > 0:
        new_tv_shows = pd.DataFrame(columns=tv_shows.columns)
        new_tv_shows = pd.concat([new_tv_shows, feed_tv_shows.loc[new_hashes]])

        for index in new_tv_shows.index:
            new_tv_show = new_tv_shows.loc[index].copy()
            new_tv_show = parse_tv_shows(new_tv_show)
            new_tv_shows.loc[index] = new_tv_show




    for index in new_hashes:
        print(index)

    # parse fields for new tv fields contained in raw_title
    new_tv_shows = tv_shows[tv_shows['status'].isna()].copy()
    new_tv_shows = parse_tv_shows(new_tv_shows)

    # updated status of
    for index in new_hashes:
        print(index)

        print(new_tv_shows.loc[index])

        print(f"ingested: {new_tv_shows.loc[index]} with hash {index}")


        tv_shows.loc[index]["status"] = "ingested"

    # Save the updated tv_shows DataFrame
    tv_shows.to_csv('./data/tv_shows.csv', index=True)

