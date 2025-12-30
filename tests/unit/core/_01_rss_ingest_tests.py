import pytest
from unittest.mock import patch, MagicMock, call
import os
import polars as pl
from src.core._01_rss_ingest import *
from src.data_models import *
from tests.fixtures.core._01_rss_ingest_fixtures import *

class TestRssIngest:
    """Test cases for _01_rss_ingest functions."""

    @patch('feedparser.parse')
    def test_rss_feed_ingest(self, mock_parse, rss_feed_ingest_cases):
        """Test all rss_feed_ingest scenarios from fixture."""
        for case in rss_feed_ingest_cases:
            mock_parse.reset_mock()
            
            # Setup mock feed data
            mock_feed = MagicMock()
            mock_feed.__getitem__.side_effect = lambda key: case["mock_feed_data"][key]
            mock_feed.channel.title = case["mock_feed_data"]["channel"]["title"]
            mock_parse.return_value = mock_feed
            
            # Call function
            result = rss_feed_ingest(case["rss_url"], case["rss_source"])
            
            # Verify feedparser was called with correct URL
            mock_parse.assert_called_once_with(case["rss_url"])
            
            # Verify results
            assert len(result) == len(case["expected_entries"]), (
                f"Failed for {case['description']}: "
                f"expected {len(case['expected_entries'])} entries, got {len(result)}"
            )
            
            for i, (actual, expected) in enumerate(zip(result, case["expected_entries"])):
                assert actual["rss_source"] == expected["rss_source"], (
                    f"Failed for {case['description']} entry {i}: "
                    f"expected rss_source={expected['rss_source']}, got {actual['rss_source']}"
                )
                if "title" in expected:
                    assert actual["title"] == expected["title"], (
                        f"Failed for {case['description']} entry {i}: "
                        f"expected title={expected['title']}, got {actual['title']}"
                    )


    @patch('src.core._01_rss_ingest.utils.extract_hash_from_direct_download_url')
    @patch('src.core._01_rss_ingest.utils.extract_hash_from_magnet_link')
    @patch('src.core._01_rss_ingest.utils.classify_media_type')
    def test_format_entries(self, mock_classify, mock_extract_magnet, 
                          mock_extract_direct, format_entries_cases):
        """Test all format_entries scenarios from fixture."""
        for case in format_entries_cases:
            # Reset mocks
            mock_classify.reset_mock()
            mock_extract_magnet.reset_mock()
            mock_extract_direct.reset_mock()
            
            # Setup mocks based on RSS source
            if case["input_entry"]["rss_source"] == "yts.mx":
                mock_extract_direct.return_value = case["expected_output"]["hash"]
            elif case["input_entry"]["rss_source"] == "episodefeed.com":
                mock_extract_magnet.return_value = case["expected_output"]["hash"]
                mock_classify.return_value = case["expected_output"]["media_type"]
            
            # Call function
            result = format_entries(case["input_entry"])
            
            # Verify results
            assert result == case["expected_output"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected_output']}, got {result}"
            )
            
            # Verify correct extraction methods were called
            if case["input_entry"]["rss_source"] == "yts.mx":
                mock_extract_direct.assert_called_once()
                mock_extract_magnet.assert_not_called()
            elif case["input_entry"]["rss_source"] == "episodefeed.com":
                mock_extract_magnet.assert_called_once()
                mock_extract_direct.assert_not_called()
                mock_classify.assert_called_once_with(case["input_entry"]["title"])


    @patch.dict(os.environ, {}, clear=True)
    @patch('src.core._01_rss_ingest.utils.insert_items_to_db')
    @patch('src.core._01_rss_ingest.utils.compare_hashes_to_db')
    @patch('src.core._01_rss_ingest.utils.classify_media_type')
    @patch('src.core._01_rss_ingest.utils.extract_hash_from_magnet_link')
    @patch('src.core._01_rss_ingest.utils.extract_hash_from_direct_download_url')
    @patch('feedparser.parse')
    def test_rss_ingest_workflow_integration(self, mock_parse, mock_extract_direct,
                                           mock_extract_magnet, mock_classify,
                                           mock_compare_hashes, mock_insert_db,
                                           rss_ingest_workflow_cases, caplog):
        """Test rss_ingest workflow integration scenarios from fixture."""
        for case in rss_ingest_workflow_cases:
            # Reset mocks and logs
            mock_parse.reset_mock()
            mock_extract_direct.reset_mock()
            mock_extract_magnet.reset_mock()
            mock_classify.reset_mock()
            mock_compare_hashes.reset_mock()
            mock_insert_db.reset_mock()
            caplog.clear()
            
            # Setup extraction mocks to return the hash from the URL
            mock_extract_direct.side_effect = lambda url: url.split('/')[-1]
            mock_extract_magnet.side_effect = lambda url: url.split(':')[-1].split('&')[0]
            mock_classify.side_effect = lambda title: ("tv_season" if "Complete" in title or "S0" in title and not "E0" in title 
                                                      else "tv_show" if "S0" in title and "E0" in title 
                                                      else "movie")
            
            # Set environment variables
            os.environ.update(case["env_vars"])
            
            # Setup mock feed data
            def parse_side_effect(url):
                mock_feed = MagicMock()
                # Find the index of this URL in the AT_RSS_URLS
                urls = case["env_vars"]["AT_RSS_URLS"].split(',')
                if url in urls:
                    idx = urls.index(url)
                    if idx < len(case["mock_feed_data"]):
                        feed_data = case["mock_feed_data"][idx]
                        mock_feed.__getitem__.side_effect = lambda key: feed_data[key]
                        mock_feed.channel.title = feed_data["channel"]["title"]
                return mock_feed
            
            mock_parse.side_effect = parse_side_effect
            
            # Setup mock database comparison
            def compare_hashes_side_effect(hashes):
                # Return hashes that are NOT in the mock_db_hashes (i.e., new hashes)
                return [h for h in hashes if h not in case["mock_db_hashes"]]
            
            mock_compare_hashes.side_effect = compare_hashes_side_effect
            
            # Execute the function
            with caplog.at_level(logging.ERROR):
                rss_ingest()
            
            # Check for expected error logs
            if "expected_error_log" in case:
                error_logs = [r.message for r in caplog.records if r.levelname == "ERROR"]
                assert any(case["expected_error_log"] in log for log in error_logs), (
                    f"Failed for {case['description']}: "
                    f"expected error log containing '{case['expected_error_log']}', "
                    f"got {error_logs}"
                )
                # If there's an error, we shouldn't proceed with DB operations
                mock_insert_db.assert_not_called()
                continue
            
            # Verify database insert calls
            assert mock_insert_db.call_count == case["expected_db_insert_count"], (
                f"Failed for {case['description']}: "
                f"expected {case['expected_db_insert_count']} DB insert calls, "
                f"got {mock_insert_db.call_count}"
            )
            
            # Verify the content of inserted items if any
            if case["expected_insert_items"]:
                # Get the DataFrame that was passed to insert
                call_args = mock_insert_db.call_args
                actual_media = call_args.kwargs['media']

                assert isinstance(actual_media, pl.DataFrame), (
                    f"Failed for {case['description']}: "
                    f"expected pl.DataFrame, got {type(actual_media)}"
                )

                assert actual_media.height == len(case["expected_insert_items"]), (
                    f"Failed for {case['description']}: "
                    f"expected {len(case['expected_insert_items'])} items to insert, "
                    f"got {actual_media.height}"
                )

                # Sort both actual and expected by hash for consistent comparison
                actual_sorted = actual_media.sort("hash")
                expected_sorted = sorted(case["expected_insert_items"],
                                       key=lambda x: x["hash"])

                for i, expected in enumerate(expected_sorted):
                    row = actual_sorted.row(i, named=True)
                    for field, expected_value in expected.items():
                        assert row.get(field) == expected_value, (
                            f"Failed for {case['description']} item {i}: "
                            f"expected {field}={expected_value}, got {row.get(field)}"
                        )