import os
import unittest
import logging
import pickle
from pathlib import Path
import pandas as pd

import main
import src.core as core

class TestRSSIngest(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test method"""
        # Configure logging to capture any warnings
        logging.basicConfig(level=logging.WARNING)
        self.logger = logging.getLogger()

        # Setup test data directory
        self.test_data_dir = Path("./data")

    def test_full_pipeline_movie(self):
        """
        test the main parse_media function with _03_parse.py
        will be executed 3 times in succession
        """
        try:
            main.full_pipeline(media_type='movie')
            main.full_pipeline(media_type='movie')
            main.full_pipeline(media_type='movie')
        except Exception as e:
            self.fail(f"movie ingest raised an exception: {str(e)}")


    def test_full_pipeline_tv_show(self):
        """
        test the main parse_media function with _03_parse.py
        will be executed 3 times in succession
        """
        try:
            main.full_pipeline(media_type='tv_show')
            main.full_pipeline(media_type='tv_show')
            main.full_pipeline(media_type='tv_show')
        except Exception as e:
            self.fail(f"tv show ingest raised an exception: {str(e)}")