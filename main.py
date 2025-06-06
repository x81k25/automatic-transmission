# standard library imports
import argparse

# local/custom imports
import src.core as core

# ------------------------------------------------------------------------------
# end-to-end pipeline for downloading all media
# ------------------------------------------------------------------------------

def full_pipeline(debug: bool):
    """full pipeline for downloading contents"""
    core.rss_ingest()
    core.collect_media()
    core.parse_media()
    core.filter_files()
    core.collect_metadata()
    core.filter_media()
    core.initiate_media_download()
    core.check_downloads()
    core.transfer_media()
    core.cleanup_media()


# -------------------------------------------------------------------------------
# main functions
# -------------------------------------------------------------------------------

def main():
    """
    Main function for command-line interface
    :return:
    """
    parser = argparse.ArgumentParser(
        description="Command-line interface for running functions"
    )

    # Add global debug flag
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode"
    )

    args = parser.parse_args()

    # Use the mapping to call full_pipeline
    full_pipeline(debug=args.debug)

# ------------------------------------------------------------------------------
# main clause
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------
# end of main.py
# ------------------------------------------------------------------------------