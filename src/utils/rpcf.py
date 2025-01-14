import os
from dotenv import load_dotenv
from transmission_rpc import Client as Transmission_client

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# transmission connection details
hostname=os.getenv('SERVER_IP')
transmission_username = os.getenv('TRANSMISSION_USERNAME')
transmission_password = os.getenv('TRANSMISSION_PASSWORD')
transmission_port = os.getenv('TRANSMISSION_PORT')

# ------------------------------------------------------------------------------
# functions
# ------------------------------------------------------------------------------

def get_transmission_client():
    """
    Instantiate a transmission client
    :return: Transmission_client object
    """
    return Transmission_client(
        host=hostname,
        port=transmission_port,
        username=transmission_username,
        password=transmission_password
    )


def return_current_torrents():
    """
    hit transmission rpc and return all current torrents

    :return: DataFrame containing all relevant torrent information if torrents exist, None if no torrents
    """
    import pandas as pd

    # Get transmission client
    client = get_transmission_client()

    # Get all torrents
    torrents = client.get_torrents()

    # If no torrents, return None
    if not torrents:
        return None

    # Create list to store torrent data
    torrent_data = []

    # Extract relevant information from each torrent
    for torrent in torrents:
        torrent_info = {
            'hash': torrent.hashString,
            'id': torrent.id,
            'name': torrent.name,
            'status': torrent.status,
            'magnet_link': torrent.magnet_link,
            'progress': round(torrent.progress, 2),
            'size': torrent.total_size,
            'upload_speed': torrent.rate_upload,
            'download_speed': torrent.rate_download,
            'peers_connected': torrent.peers_connected,
            'eta': torrent.eta,
            'download_dir': torrent.download_dir
        }
        torrent_data.append(torrent_info)

    # Convert to dataframe
    torrent_df = pd.DataFrame(torrent_data)
    return torrent_df


def purge_torrent_queue():
    """
    purge entire queue of torrents
    :return:
    """
    # Instantiate transmission client
    transmission_client = get_transmission_client()

    # Get all torrents
    torrents = transmission_client.get_torrents()

    # Iterate through each torrent and remove it
    for torrent in torrents:
        try:
            transmission_client.remove_torrent(torrent.id, delete_data=True)
            print(f"Removed torrent: {torrent.name} with hash {torrent.hashString}")
        except Exception as e:
            print(f"Failed to remove torrent: {torrent.name} with hash {torrent.hashString}. Error: {e}")

# ------------------------------------------------------------------------------
# end of rpcf.py
# ------------------------------------------------------------------------------