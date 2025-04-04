# standard library imports
import os
import re

# third-party imports
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
# create client function
# ------------------------------------------------------------------------------

def get_transmission_client(port: int = transmission_port):
    """
    Instantiate a transmission client
    :param port: server port for transmission client being used
    :return: Transmission_client object
    """
    return Transmission_client(
        host=hostname,
        port=port,
        username=transmission_username,
        password=transmission_password
    )

# ------------------------------------------------------------------------------
# functions to retrieve data
# ------------------------------------------------------------------------------

def get_torrent_info(hash: str):
    """
    using hash, retrieve torrent metadata from transmission client

    :param hash: individual hash of the torrent
    :return: dict of all torrent parameters stored in transmission client
    """
    transmission_client = get_transmission_client()

    torrent = transmission_client.get_torrent(hash)

    return torrent


def return_current_torrents(port: int = transmission_port):
    """
    hit transmission rpc and return all current torrents

    :return: DataFrame containing all relevant torrent information if torrents exist, None if no torrents
    """
    # Get transmission client
    client = get_transmission_client(port)

    # Get all torrents
    torrents = client.get_torrents()

    # If no torrents, return None
    if not torrents:
        return None

    # Create list to store torrent data
    torrent_data = {}

    # Extract relevant information from each torrent
    for torrent in torrents:
        torrent_datum = {
            torrent.hashString: {
                'id': torrent.id,
                'name': torrent.name,
                'status': torrent.status,
                'torrent_source': torrent.magnet_link,
                'progress': round(torrent.progress, 2),
                'size': torrent.total_size,
                'upload_speed': torrent.rate_upload,
                'download_speed': torrent.rate_download,
                'peers_connected': torrent.peers_connected,
                'eta': torrent.eta,
                'download_dir': torrent.download_dir
            }
        }
        torrent_data.update(torrent_datum)

    return torrent_data

# ------------------------------------------------------------------------------
# functions to add/remove torrents
# ------------------------------------------------------------------------------

def add_media_item(torrent_source: str):
    """
    add media item to transmission client
    :param torrent_source: any acceptable format of torrent link
    """
    transmission_client = get_transmission_client()

    # Regex pattern for a 40-character hex string (SHA-1 hash)
    hash_pattern = re.compile(r'^[0-9a-f]{40}$')

    # check if the input is pure hash, and if so format as magnet
    if hash_pattern.match(torrent_source):
        torrent_source = f"magnet:?xt=urn:btih:{torrent_source}"

    # send to transmission
    transmission_client.add_torrent(torrent_source)


def remove_media_item(hash: str):
    """
    remove media item from transmission client
    :param hash: hash of the torrent to remove
    """
    transmission_client = get_transmission_client()
    transmission_client.remove_torrent(hash, delete_data=True)


def purge_torrent_queue():
    """
    purge entire queue of torrents
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