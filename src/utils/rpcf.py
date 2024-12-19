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
transmission_port = 9091
transmission_username = os.getenv('TRANSMISSION_USERNAME')
transmission_password = os.getenv('TRANSMISSION_PASSWORD')

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
# test
# ------------------------------------------------------------------------------

#purge_torrent_queue()