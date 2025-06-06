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
load_dotenv(override=True)

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

def get_media_item_info(hash: str):
    """
    using hash, retrieve media_item metadata from transmission client

    :param hash: individual hash of the media_item
    :return: dict of all media_item parameters stored in transmission client
    """
    transmission_client = get_transmission_client()

    media_item = transmission_client.get_torrent(hash)

    return media_item


def return_current_media_items(port: int = transmission_port) -> dict | None:
    """
    hit transmission rpc and return all current media_items

    :param port: transmission daemon port
    :return: dict containing all relevant media_item information if media_items exist, None if no media_items
    """
    # Get transmission client
    client = get_transmission_client(port)

    # Get all media_items
    media_items = client.get_torrents()

    # If no media_items, return None
    if not media_items:
        return None

    # Create list to store media_item data
    media_item_data = {}

    # Extract relevant information from each media_item
    for media_item in media_items:
        media_item_datum = {
            media_item.hashString: {
                'id': media_item.id,
                'name': media_item.name,
                'status': media_item.status,
                'media_item_source': media_item.magnet_link,
                'progress': round(media_item.progress, 2),
                'size': media_item.total_size,
                'upload_speed': media_item.rate_upload,
                'download_speed': media_item.rate_download,
                'peers_connected': media_item.peers_connected,
                'eta': media_item.eta,
                'download_dir': media_item.download_dir
            }
        }
        media_item_data.update(media_item_datum)

    return media_item_data


def return_current_item_count(port: int = transmission_port) -> int:
    """
    hit transmission rpc and return count of active items

    :return: DataFrame containing all relevant media_item information if media_items exist, None if no media_items

    :param port: transmission daemon port
    :debug: port=30091
    """
    # Get transmission client
    client = get_transmission_client(port)

    # Get all media_items
    media_items = client.get_torrents()

    # If no media_items, return None
    return len(media_items)


# ------------------------------------------------------------------------------
# functions to add/remove media_items
# ------------------------------------------------------------------------------

def add_media_item(media_item_source: str):
    """
    add media item to transmission client
    :param media_item_source: any acceptable format of media_item link
    """
    transmission_client = get_transmission_client()

    # Regex pattern for a 40-character hex string (SHA-1 hash)
    hash_pattern = re.compile(r'^[0-9a-f]{40}$')

    # check if the input is pure hash, and if so format as magnet
    if hash_pattern.match(media_item_source):
        media_item_source = f"magnet:?xt=urn:btih:{media_item_source}"

    # send to transmission
    transmission_client.add_torrent(media_item_source)


def remove_media_item(hash: str):
    """
    remove media item from transmission client
    :param hash: hash of the media_item to remove
    """
    transmission_client = get_transmission_client()
    transmission_client.remove_torrent(hash, delete_data=True)


def purge_media_item_queue():
    """
    purge entire queue of media_items
    """
    # Instantiate transmission client
    transmission_client = get_transmission_client()

    # Get all media_items
    media_items = transmission_client.get_torrents()

    # Iterate through each media_item and remove it
    for media_item in media_items:
        try:
            transmission_client.remove_torrent(media_item.id, delete_data=True)
            print(f"Removed media_item: {media_item.name} with hash {media_item.hashString}")
        except Exception as e:
            print(f"Failed to remove media_item: {media_item.name} with hash {media_item.hashString}. Error: {e}")

# ------------------------------------------------------------------------------
# end of rpcf.py
# ------------------------------------------------------------------------------