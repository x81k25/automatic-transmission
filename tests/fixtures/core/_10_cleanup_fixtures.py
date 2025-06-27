import pytest
from datetime import datetime, timedelta, UTC
from src.data_models import *

@pytest.fixture
def get_delay_multiple_cases():
    """Test scenarios for get_delay_multiple function."""
    return [
        {
            "description": "Normal case with target 10, current 5",
            "target_active_items": "10",
            "current_item_count": 5,
            "expected_multiple": 2.0
        },
        {
            "description": "Equal target and current items",
            "target_active_items": "10",
            "current_item_count": 10,
            "expected_multiple": 1.0
        },
        {
            "description": "Current items exceed target",
            "target_active_items": "10",
            "current_item_count": 20,
            "expected_multiple": 0.5
        },
        {
            "description": "Zero target items - no modulation",
            "target_active_items": "0",
            "current_item_count": 5,
            "expected_multiple": 1.0
        },
        {
            "description": "Zero current items - avoid division by zero",
            "target_active_items": "10",
            "current_item_count": 0,
            "expected_multiple": 1.0
        },
        {
            "description": "Fractional target",
            "target_active_items": "5.5",
            "current_item_count": 2,
            "expected_multiple": 2.75
        }
    ]


@pytest.fixture
def get_delay_multiple_error_cases():
    """Test error scenarios for get_delay_multiple function."""
    return [
        {
            "description": "Negative target items raises ValueError",
            "target_active_items": "-1",
            "current_item_count": 5,
            "expected_error": ValueError,
            "expected_error_message": "TARGET_ACTIVE_ITEMS value of -1.0 is less than 0 and no permitted"
        }
    ]


@pytest.fixture
def cleanup_transferred_media_cases():
    """Test scenarios for cleanup_transferred_media function."""
    base_time = datetime.now(UTC)
    return [
        {
            "description": "No transferred media to cleanup",
            "input_media": None,
            "modulated_delay": 86400.0,  # 1 day in seconds
            "expected_db_update_calls": 0,
            "expected_remove_calls": 0,
            "expected_outputs": []
        },
        {
            "description": "Single item exceeds delay and gets removed successfully",
            "input_media": [
                {
                    "hash": "transferred123456789012345678901234567890",
                    "pipeline_status": "transferred",
                    "updated_at": base_time - timedelta(days=2),  # 2 days ago
                    "media_type": "movie",
                    "media_title": "Transferred Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "modulated_delay": 86400.0,  # 1 day in seconds
            "expected_db_update_calls": 1,
            "expected_remove_calls": 1,
            "expected_outputs": [
                {
                    "hash": "transferred123456789012345678901234567890",
                    "pipeline_status": "complete"
                }
            ]
        },
        {
            "description": "Item within delay period is not removed",
            "input_media": [
                {
                    "hash": "recent123456789012345678901234567890123",
                    "pipeline_status": "transferred",
                    "updated_at": base_time - timedelta(hours=12),  # 12 hours ago
                    "media_type": "movie",
                    "media_title": "Recent Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "modulated_delay": 86400.0,  # 1 day in seconds
            "expected_db_update_calls": 0,
            "expected_remove_calls": 0,
            "expected_outputs": []
        },
        {
            "description": "Mixed items - some exceed delay, some don't",
            "input_media": [
                {
                    "hash": "old123456789012345678901234567890123456",
                    "pipeline_status": "transferred",
                    "updated_at": base_time - timedelta(days=3),  # 3 days ago
                    "media_type": "movie",
                    "media_title": "Old Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                },
                {
                    "hash": "new123456789012345678901234567890123456",
                    "pipeline_status": "transferred", 
                    "updated_at": base_time - timedelta(hours=6),  # 6 hours ago
                    "media_type": "tv_show",
                    "media_title": "New Show",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "modulated_delay": 86400.0,  # 1 day in seconds
            "expected_db_update_calls": 1,
            "expected_remove_calls": 1,
            "expected_outputs": [
                {
                    "hash": "old123456789012345678901234567890123456",
                    "pipeline_status": "complete"
                }
            ]
        },
        {
            "description": "Removal fails with exception",
            "input_media": [
                {
                    "hash": "error123456789012345678901234567890123456",
                    "pipeline_status": "transferred",
                    "updated_at": base_time - timedelta(days=2),
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "modulated_delay": 86400.0,
            "removal_exception": Exception("Mock removal error"),
            "expected_db_update_calls": 1,
            "expected_remove_calls": 1,
            "expected_outputs": [
                {
                    "hash": "error123456789012345678901234567890123456",
                    "error_status": True,
                    "error_condition": "Mock removal error"
                }
            ]
        }
    ]


@pytest.fixture
def cleanup_hung_items_cases():
    """Test scenarios for cleanup_hung_items function."""
    base_time = datetime.now(UTC)
    return [
        {
            "description": "No current items in daemon",
            "current_transmission_items": None,
            "input_media": None,
            "modulated_delay": 259200.0,  # 3 days in seconds
            "expected_db_update_calls": 0,
            "expected_remove_calls": 0,
            "expected_outputs": []
        },
        {
            "description": "No media found for current hashes",
            "current_transmission_items": {
                "hash123456789012345678901234567890123456": {"name": "Movie.mkv"}
            },
            "input_media": None,
            "modulated_delay": 259200.0,
            "expected_db_update_calls": 0,
            "expected_remove_calls": 0,
            "expected_outputs": []
        },
        {
            "description": "Single hung item exceeds delay and gets removed",
            "current_transmission_items": {
                "hung123456789012345678901234567890123456": {"name": "Hung.Movie.mkv"}
            },
            "input_media": [
                {
                    "hash": "hung123456789012345678901234567890123456",
                    "pipeline_status": "downloading",
                    "updated_at": base_time - timedelta(days=5),  # 5 days ago
                    "media_type": "movie",
                    "media_title": "Hung Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "modulated_delay": 259200.0,  # 3 days in seconds
            "expected_db_update_calls": 1,
            "expected_remove_calls": 1,
            "expected_outputs": [
                {
                    "hash": "hung123456789012345678901234567890123456",
                    "pipeline_status": "rejected",
                    "rejection_status": "rejected",
                    "rejection_reason": "exceeded time limit"
                }
            ]
        },
        {
            "description": "Item within delay period is not removed",
            "current_transmission_items": {
                "recent123456789012345678901234567890123456": {"name": "Recent.Movie.mkv"}
            },
            "input_media": [
                {
                    "hash": "recent123456789012345678901234567890123456",
                    "pipeline_status": "downloading",
                    "updated_at": base_time - timedelta(days=1),  # 1 day ago
                    "media_type": "movie",
                    "media_title": "Recent Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "modulated_delay": 259200.0,  # 3 days in seconds
            "expected_db_update_calls": 0,
            "expected_remove_calls": 0,
            "expected_outputs": []
        },
        {
            "description": "Hung item removal fails with exception",
            "current_transmission_items": {
                "error123456789012345678901234567890123456": {"name": "Error.Movie.mkv"}
            },
            "input_media": [
                {
                    "hash": "error123456789012345678901234567890123456",
                    "pipeline_status": "downloading",
                    "updated_at": base_time - timedelta(days=5),
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "rejection_status": "accepted",
                    "error_status": False
                }
            ],
            "modulated_delay": 259200.0,
            "removal_exception": Exception("Mock hung removal error"),
            "expected_db_update_calls": 1,
            "expected_remove_calls": 1,
            "expected_outputs": [
                {
                    "hash": "error123456789012345678901234567890123456",
                    "error_status": True,
                    "error_condition": "Mock hung removal error"
                }
            ]
        }
    ]


@pytest.fixture
def cleanup_media_workflow_cases():
    """Test scenarios for cleanup_media workflow integration."""
    return [
        {
            "description": "Normal cleanup with valid environment variables",
            "env_vars": {
                "TRANSFERRED_ITEM_CLEANUP_DELAY": "1",
                "HUNG_ITEM_CLEANUP_DELAY": "3",
                "TARGET_ACTIVE_ITEMS": "10"
            },
            "current_item_count": 5,
            "transferred_media": None,
            "current_transmission_items": None,
            "hung_media": None,
            "expected_cleanup_transferred_calls": 1,
            "expected_cleanup_hung_calls": 1,
            "expected_delay_multiple": 2.0
        },
        {
            "description": "Zero delays with items to cleanup",
            "env_vars": {
                "TRANSFERRED_ITEM_CLEANUP_DELAY": "0",
                "HUNG_ITEM_CLEANUP_DELAY": "0",
                "TARGET_ACTIVE_ITEMS": "0"
            },
            "current_item_count": 0,
            "transferred_media": None,
            "current_transmission_items": None,
            "hung_media": None,
            "expected_cleanup_transferred_calls": 1,
            "expected_cleanup_hung_calls": 1,
            "expected_delay_multiple": 1.0
        }
    ]


@pytest.fixture  
def cleanup_media_error_cases():
    """Test error scenarios for cleanup_media workflow."""
    return [
        {
            "description": "Negative TRANSFERRED_ITEM_CLEANUP_DELAY raises ValueError",
            "env_vars": {
                "TRANSFERRED_ITEM_CLEANUP_DELAY": "-1",
                "HUNG_ITEM_CLEANUP_DELAY": "3",
                "TARGET_ACTIVE_ITEMS": "10"
            },
            "expected_error": ValueError,
            "expected_error_message": "TRANSFERRED_ITEM_CLEANUP_DELAY value of -1.0 is less than 0 and no permitted"
        },
        {
            "description": "Negative HUNG_ITEM_CLEANUP_DELAY does not raise error (due to bug in source)", 
            "env_vars": {
                "TRANSFERRED_ITEM_CLEANUP_DELAY": "1",
                "HUNG_ITEM_CLEANUP_DELAY": "-3",
                "TARGET_ACTIVE_ITEMS": "10"
            }
        }
    ]