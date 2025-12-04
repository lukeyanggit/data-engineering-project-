"""Tests for data transformation module."""

import pytest
from datetime import datetime

from src.transform import StandardTransformer, DataValidator


class TestStandardTransformer:
    """Test standard transformer."""
    
    def test_drop_duplicates(self):
        """Test duplicate removal."""
        data = [
            {"id": 1, "name": "test"},
            {"id": 1, "name": "test"},
            {"id": 2, "name": "other"}
        ]
        
        transformer = StandardTransformer(drop_duplicates=True)
        result = transformer.transform(data)
        
        assert len(result) == 2
    
    def test_rename_columns(self):
        """Test column renaming."""
        data = [{"old_name": "value"}]
        
        transformer = StandardTransformer(rename_columns={"old_name": "new_name"})
        result = transformer.transform(data)
        
        assert "new_name" in result[0]
        assert "old_name" not in result[0]
    
    def test_date_conversion(self):
        """Test date column conversion."""
        data = [{"date": "2023-01-01", "value": 100}]
        
        transformer = StandardTransformer(date_columns=["date"])
        result = transformer.transform(data)
        
        assert "_transformed_at" in result[0]


class TestDataValidator:
    """Test data validator."""
    
    def test_required_columns(self):
        """Test required column validation."""
        data = [
            {"id": 1, "name": "test"},
            {"id": 2}  # Missing name
        ]
        
        validator = DataValidator(required_columns=["id", "name"])
        valid, invalid = validator.validate(data)
        
        assert len(valid) == 1
        assert len(invalid) == 1
        assert "name" in invalid[0].get("_validation_errors", [""])[0]
    
    def test_column_types(self):
        """Test column type validation."""
        data = [
            {"id": 1, "value": 100},
            {"id": "2", "value": 100}  # id should be int
        ]
        
        validator = DataValidator(column_types={"id": int})
        valid, invalid = validator.validate(data)
        
        assert len(valid) == 1
        assert len(invalid) == 1

