"""Tests for data extraction module."""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from src.extract import APIExtractor, FileExtractor, DataExtractor


class TestAPIExtractor:
    """Test API extractor."""
    
    @patch('src.extract.requests.get')
    def test_extract_list_response(self, mock_get):
        """Test extraction with list response."""
        mock_response = Mock()
        mock_response.json.return_value = [{"id": 1, "name": "test"}]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        extractor = APIExtractor("http://example.com/api")
        result = extractor.extract()
        
        assert len(result) == 1
        assert result[0]["id"] == 1
    
    @patch('src.extract.requests.get')
    def test_extract_dict_response(self, mock_get):
        """Test extraction with dict response."""
        mock_response = Mock()
        mock_response.json.return_value = {"data": [{"id": 1}]}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        extractor = APIExtractor("http://example.com/api")
        result = extractor.extract()
        
        assert len(result) == 1


class TestFileExtractor:
    """Test file extractor."""
    
    def test_extract_json(self):
        """Test JSON file extraction."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([{"id": 1, "name": "test"}], f)
            temp_path = f.name
        
        try:
            extractor = FileExtractor(temp_path)
            result = extractor.extract()
            
            assert len(result) == 1
            assert result[0]["id"] == 1
        finally:
            Path(temp_path).unlink()
    
    def test_extract_csv(self):
        """Test CSV file extraction."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,value\n1,test,100\n")
            temp_path = f.name
        
        try:
            extractor = FileExtractor(temp_path)
            result = extractor.extract()
            
            assert len(result) == 1
            assert result[0]["id"] == 1
        finally:
            Path(temp_path).unlink()

