import pytest
import yaml
import os
from pathlib import Path

class TestYamlLoading:
    """Test YAML file loading for _03_parse functions."""

    def test_yaml_file_loading(self):
        """Test that string-special-conditions.yaml can be loaded properly."""

        # Get project root dynamically for cross-platform compatibility
        project_root = Path(__file__).parent.parent.parent.parent
        yaml_path = project_root / 'config' / 'string-special-conditions.yaml'

        with open(yaml_path, 'r') as file:
            yaml_content = yaml.safe_load(file)

        # Assertions
        assert yaml_content is not None, f"Could not load YAML from {yaml_path}"
        assert 'pre_processing_replacements' in yaml_content, "YAML missing 'pre_processing_replacements' key"
        assert isinstance(yaml_content['pre_processing_replacements'], list), "pre_processing_replacements should be a list"

    def test_yaml_content_structure(self):
        """Test that YAML contains expected replacement patterns."""

        # Get project root dynamically
        project_root = Path(__file__).parent.parent.parent.parent
        yaml_path = project_root / 'config' / 'string-special-conditions.yaml'

        with open(yaml_path, 'r') as file:
            special_conditions = yaml.safe_load(file)

        # Verify structure matches what parse_media_items expects
        replacements = special_conditions['pre_processing_replacements']

        # Should contain the UIndex replacement we see in test data
        uindex_found = any("UIndex" in replacement[0] for replacement in replacements)
        assert uindex_found, "Should contain UIndex replacement pattern"

        # Each replacement should be a 2-item list [old_string, new_string]
        for replacement in replacements:
            assert len(replacement) == 2, f"Each replacement should have 2 items, got: {replacement}"
            assert isinstance(replacement[0], str), "Old string should be string"
            assert isinstance(replacement[1], str), "New string should be string"