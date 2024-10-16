from src.core import rss_ingest, metadata_collection, torrent_initiation, torrent_cleanup
import argparse

# ------------------------------------------------------------------------------
# full tv show pipeline
# ------------------------------------------------------------------------------

def movie_pipeline():
    print("the movie pipeline will go here")

#-------------------------------------------------------------------------------
# to execute the tv show pipeline
# cd C:\Users\jpeck\py\automatic-transmission & python main.py tv_show_pipeline
#-------------------------------------------------------------------------------

def tv_show_pipeline():
    rss_ingest.tv_show_full_ingest()

    metadata_collection.get_tv_show_metadata()

    torrent_initiation.initiate_tv_shows()

    torrent_cleanup.tv_show_cleanup()

def main():
    # Create an argument parser
    parser = argparse.ArgumentParser(
        description="Command-line interface for running functions"
    )

    # Create subparsers for different functions
    subparsers = parser.add_subparsers(
        dest="command",
        help="Available functions"
    )

    # Subparser for function_one
    parser_one = subparsers.add_parser(
        "movie_pipeline",
        help="Execute movie_pipeline"
    )

    # Subparser for function_two, with additional parameters
    parser_two = (
        subparsers.add_parser(
            "tv_show_pipeline",
            help="Execute function two")
    )

    # Parse the arguments from the command line
    args = parser.parse_args()

    # Logic to call the correct function
    if args.command == "movie_pipeline":
        movie_pipeline()
    elif args.command == "tv_show_pipeline":
        tv_show_pipeline()
    else:
        parser.print_help()

# ------------------------------------------------------------------------------
# test
# ------------------------------------------------------------------------------

#tv_show_pipeline()

# ------------------------------------------------------------------------------
# main clause
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()