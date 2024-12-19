import os
import unittest
import logging
import pickle
from pathlib import Path
import pandas as pd
from src.core._03_parse import parse_media
import main

class TestRSSIngest(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test method"""
        # Configure logging to capture any warnings
        logging.basicConfig(level=logging.WARNING)
        self.logger = logging.getLogger()

        # Setup test data directory
        self.test_data_dir = Path("./data")

if __name__ == '__main__':
    unittest.main()