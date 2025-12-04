"""Tests for ETL pipeline."""

import pytest
from unittest.mock import Mock, MagicMock

from src.pipeline import ETLPipeline
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader


class MockExtractor(DataExtractor):
    """Mock extractor for testing."""
    
    def extract(self):
        return [{"id": 1, "name": "test"}]


class MockTransformer(DataTransformer):
    """Mock transformer for testing."""
    
    def transform(self, data):
        return data


class MockLoader(DataLoader):
    """Mock loader for testing."""
    
    def load(self, data):
        return True


def test_pipeline_success():
    """Test successful pipeline execution."""
    extractor = MockExtractor()
    transformer = MockTransformer()
    loader = MockLoader()
    
    pipeline = ETLPipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader
    )
    
    result = pipeline.run()
    
    assert result["success"] is True
    assert result["records_extracted"] == 1
    assert result["records_loaded"] == 1


def test_pipeline_without_transformer():
    """Test pipeline without transformer."""
    extractor = MockExtractor()
    loader = MockLoader()
    
    pipeline = ETLPipeline(
        extractor=extractor,
        loader=loader
    )
    
    result = pipeline.run()
    
    assert result["success"] is True


def test_pipeline_stats():
    """Test pipeline statistics tracking."""
    extractor = MockExtractor()
    loader = MockLoader()
    
    pipeline = ETLPipeline(
        extractor=extractor,
        loader=loader
    )
    
    pipeline.run()
    stats = pipeline.get_stats()
    
    assert stats["runs"] == 1
    assert stats["successful_runs"] == 1
    assert stats["records_processed"] == 1

