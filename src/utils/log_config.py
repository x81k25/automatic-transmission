# standard library imports
import logging
import os

# third-party imports
from dotenv import load_dotenv

def setup_logging():
    """
    Configure logging based on AT_LOG_LEVEL environment variable.

    Sets up different logging formats and levels for INFO vs DEBUG,
    and configures third-party library log levels.
    """
    # Load environment variables
    load_dotenv(override=True)

    log_level = os.getenv('AT_LOG_LEVEL') or "DEBUG"

    if log_level == "INFO":
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        logging.getLogger("paramiko").setLevel(logging.WARNING)
    elif log_level == "DEBUG":
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        logging.getLogger("paramiko").setLevel(logging.INFO)
    else:
        # Default fallback for any other log level values
        logging.basicConfig(
            level=getattr(logging, log_level.upper(), logging.INFO),
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        logging.getLogger("paramiko").setLevel(logging.WARNING)