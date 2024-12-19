import os
import unittest
import logging
import pickle
from pathlib import Path
import pandas as pd
from src.core._01_rss_ingest import rss_feed_ingest
from src.core._01_rss_ingest import rss_entries_to_dataframe
from src.core._01_rss_ingest import rss_ingest
from dotenv import load_dotenv

class TestRSSIngest(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test method"""
        # Configure logging to capture any warnings
        logging.basicConfig(level=logging.WARNING)
        self.logger = logging.getLogger()

        # Setup test data directory
        self.test_data_dir = Path("./data")

    def test_rss_feed_ingest_movie(self):
        """Test that movie rss ingest executes without raising exceptions"""
        # Verify movie RSS URL is available
        load_dotenv()

        movie_rss_url = os.getenv('MOVIE_RSS_URL')
        self.assertIsNotNone(
            movie_rss_url,
            "MOVIE_RSS_URL environment variable is not set"
        )
        self.assertTrue(
            len(movie_rss_url) > 0,
            "MOVIE_RSS_URL environment variable is empty"
        )

        print(f"Using movie RSS URL: {movie_rss_url}")

        try:
            rss_feed_ingest(rss_url=movie_rss_url)  # Changed from os.getenv('MOVIE_RSS_URL') to 'movie'
        except Exception as e:
            self.fail(f"Movie ingest raised an exception: {str(e)}")

    def test_rss_feed_ingest_tv_show(self):
        """Test that TV show rss ingest executes without raising exceptions"""
        # Verify TV show RSS URL is available
        load_dotenv()

        tv_show_rss_url = os.getenv('TV_SHOW_RSS_URL')
        self.assertIsNotNone(
            tv_show_rss_url,
            "TV_SHOW_RSS_URL environment variable is not set"
        )
        self.assertTrue(
            len(tv_show_rss_url) > 0,
            "TV_SHOW_RSS_URL environment variable is empty"
        )

        print(f"Using TV show RSS URL: {tv_show_rss_url}")

        try:
            rss_feed_ingest(rss_url=tv_show_rss_url)
        except Exception as e:
            self.fail(f"TV show ingest raised an exception: {str(e)}")


    def test_rss_entries_to_dataframe_movie(self):
        """Test the rss_entries_to_dataframe function with movie feed type"""
        # Load test input data
        with open(self.test_data_dir / "_01_feed_movie.pkl", "rb") as f:
            feed = pickle.load(f)

        # Load expected results
        with open(self.test_data_dir / "_01_feed_items_movie.pkl", "rb") as f:
            expected_df = pickle.load(f)

        # Execute function
        result_df = rss_entries_to_dataframe(feed, "movie")

        # Verify results
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Additional specific checks
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertEqual(result_df.index.name, 'hash')
        self.assertTrue(
            all(result_df.index.str.islower()))  # Verify all hashes are lowercase

        # Check required columns exist
        required_columns = {'raw_title', 'torrent_link', 'published_timestamp'}
        self.assertTrue(
            all(col in result_df.columns for col in required_columns))


    def test_rss_entries_to_dataframe_tv_show(self):
        """Test the rss_entries_to_dataframe function with movie feed type"""
        # Load test input data
        with open(self.test_data_dir / "_01_feed_tv_show.pkl", "rb") as f:
            feed = pickle.load(f)

        # Load expected results
        with open(self.test_data_dir / "_01_feed_items_tv_show.pkl", "rb") as f:
            expected_df = pickle.load(f)

        # Execute function
        result_df = rss_entries_to_dataframe(feed, "tv_show")

        # Verify results
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Additional specific checks
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertEqual(result_df.index.name, 'hash')
        self.assertTrue(
            all(result_df.index.str.islower()))  # Verify all hashes are lowercase

        # Check required columns exist
        required_columns = {
            'tv_show_name',
            'magnet_link',
            "raw_title",
            'published_timestamp'}
        self.assertTrue(
            all(col in result_df.columns for col in required_columns))


    def test_rss_ingest_movie(self):
        """Test that movie ingest executes without raising exceptions"""
        try:
            rss_ingest(media_type='movie')
        except Exception as e:
            self.fail(f"Movie ingest raised an exception: {str(e)}")


    def test_rss_ingest_tv_show(self):
        """Test that TV show ingest executes without raising exceptions"""
        try:
            rss_ingest(media_type='tv_show')
        except Exception as e:
            self.fail(f"TV show ingest raised an exception: {str(e)}")


if __name__ == '__main__':
    unittest.main()