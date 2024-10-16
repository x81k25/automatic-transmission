import os
from dotenv import load_dotenv
import pandas as pd
import pickle
from src.utils import rpcf, sshf, logger


# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# set directories from .env
download_dir=os.getenv('download_dir')
tv_show_dir=os.getenv('tv_show_dir')
movie_dir=os.getenv('movie_dir')

# ------------------------------------------------------------------------------
# torrent clean-up functions
# ------------------------------------------------------------------------------

def movie_cleanup():
    """
    using transmission-rpc, get status of all torrents; save the status to a
    pandas dataframe; iterate through and removed all completed torrents
    from transmission; after they have been removed move them from the download
    folder to the appropriate tv or movie folder
    :return:
    """
    # instantiate transmission client
    transmission_client = rpcf.get_transmission_client()

    # get status of all torrents in transmission and save to dataframe
    torrents = transmission_client.get_torrents()

    # iterate through all of the elements in the torrents object
    # and save them to a pandas dataframe
    torrent_list = [
        {
            'hash': torrent.info_hash,
            'name': torrent.name,
            'status': torrent.status,
            'progress': torrent.progress,
            'downloaded': torrent.downloaded_ever,
            'uploaded': torrent.uploaded_ever,
            'peers': torrent.peers_connected,
            'rate_download': torrent.rate_download,
            'rate_upload': torrent.rate_upload
        }
        for torrent in torrents #if torrent.status == 'downloading'
    ]

    #return torrents_df
    torrent_df = pd.DataFrame(torrent_list)

    # iterate through the dataframe and remove all completed torrents
    for index, row in torrent_df.iterrows():
        if row.progress == 100:
            transmission_client.remove_torrent(row['hash'])
            logger(f"torrent {row['name']} has been removed")

    sshf.print_dump_contents()

    sshf.move_tv_show(
        file_name='Saturday.Night.Live.S50E01.Jean.Smart.1080p.WEB.h264-EDITH[TGx]'
        ,show='saturday-night-live-1975'
        ,season='s50'
    )

    sshf.move_movie(
        file_name="Can't Hardly Wait (1998) [1080p] [BluRay] [5.1] [YTS.MX]"
    )

def remove_tv_shows(tv_show):
    # Instantiate transmission client
    transmission_client = rpcf.get_transmission_client()

    # iterate through each hash and get torrent data
    try:
        torrent = transmission_client.get_torrent(tv_show.hash)

        # remove completed downloads and update status
        if torrent.progress == 100.0:
            tv_show["file_name"] = torrent.name
            transmission_client.remove_torrent(tv_show.hash)
            tv_show["status"] = 'downloaded'
            logger(f"download successful: {tv_show.raw_title} with hash {tv_show.hash}")
    except:
        logger(f"error downloading: {tv_show.raw_title} with hash {tv_show.hash}")

    return tv_show


def transfer_tv_shows(tv_show, download_dir, tv_show_dir):
    transmission_client = rpcf.get_transmission_client()

    try:
        sshf.move_tv_show(
            download_dir = download_dir,
            tv_show_dir = tv_show_dir,
            file_name = tv_show.file_name,
            tv_show_name = tv_show.tv_show_name,
            release_year= tv_show.release_year,
            season = tv_show.season
        )
        tv_show["status"] = 'completed'
        logger(f"transfer complete: {tv_show.raw_title} with hash {tv_show.hash}")
    except:
        logger(f"error transferring: {tv_show.raw_title} with hash {tv_show.hash}")

    return tv_show

# ------------------------------------------------------------------------------
# torrent clean-up full pipelines
# ------------------------------------------------------------------------------

def tv_show_cleanup():
    # Read in tv_shows.pkl
    with open('./data/tv_shows.pkl', 'rb') as file:
        tv_shows = pickle.load(file)

    # remove downloaded torrents
    for index, row in tv_shows.iterrows():
        if row['status'] == 'downloading':
            row.hash = index
            updated_row = remove_tv_shows(row)
            tv_shows.loc[index] = updated_row

    # transfer completed torrents
    for index, row in tv_shows.iterrows():
        if row['status'] == 'downloaded':
            row.hash = index
            updated_row = transfer_tv_shows(row, download_dir, tv_show_dir)
            tv_shows.loc[index] = updated_row

    # Save the updated tv_shows DataFrame
    with open('./data/tv_shows.pkl', 'wb') as file:
        pickle.dump(tv_shows, file)


