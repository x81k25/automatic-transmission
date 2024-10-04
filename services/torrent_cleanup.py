import os
from dotenv import load_dotenv
from transmission_rpc import Client as Transmission_client
import pandas as pd
from ssh_functions import sshf
from rpc_functions import rpcf

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# load environment variables for SSH connection details
hostname = os.getenv('server-ip')
ssh_username = os.getenv('server-username')
ssh_password = os.getenv('server-password')
ssh_port = 22  # Default SSH port

# transmission connection details
transmission_username = os.getenv('transmission-username')
transmission_password = os.getenv('transmission-password')
transmisson_port = 9091

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
            print(f"Torrent {row['name']} has been removed")

    sshf.print_dump_contents()

    sshf.move_tv_show(
        file_name='Saturday.Night.Live.S50E01.Jean.Smart.1080p.WEB.h264-EDITH[TGx]'
        ,show='saturday-night-live-1975'
        ,season='s50'
    )

    sshf.move_movie(
        file_name="Can't Hardly Wait (1998) [1080p] [BluRay] [5.1] [YTS.MX]"
    )

def remove_tv_shows(tv_shows):
    # Instantiate transmission client
    transmission_client = rpcf.get_transmission_client()

    # Get the torrent all hash from tv_shows DataFrame
    torrent_hashes = tv_shows['hash']

    # iterate through each hash and get torrent data
    for hash_id in torrent_hashes:
        # Query transmission for the status of the download
        torrent = transmission_client.get_torrent(hash_id)

    # Get the file names
    file_names = [file.name for file in torrent.files()]

    # Save the updated tv_shows DataFrame
    print(file_names)

    return tv_shows


def transfer_tv_shows(tv_shows):
    for index, row in tv_shows.iterrows():
        if row['status'] == 'downloaded':

            hash_id = row['hash']
            try:
                # Query transmission for the status of the download
                torrent = transmission_client.get_torrent(hash_id)
                if torrent.progress == 100:
                    # Remove the torrent from transmission
                    transmission_client.remove_torrent(hash_id)
                    # If removal was successful, change status to "downloaded"
                    tv_shows.at[index, 'status'] = 'downloaded'
                    print(
                        f"Download successful: {row['raw_title']} with hash {hash_id}")
            except Exception as e:
                print(
                    f"Failed to update status for {row['raw_title']} with hash {hash_id}. Error: {e}")


# ------------------------------------------------------------------------------
# torrent clean-up full pipelines
# ------------------------------------------------------------------------------


def tv_show_cleanup():
    # red in tv_shows.csv
    tv_shows = pd.read_csv('./data/tv_shows.csv')

    tv_shows = remove_tv_shows(tv_shows)

    tv_shows = transfer_tv_shows(tv_shows)



