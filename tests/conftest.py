"""Pytest configuration and fixtures."""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return [
        {"id": 1, "name": "Test 1", "value": 100},
        {"id": 2, "name": "Test 2", "value": 200},
        {"id": 3, "name": "Test 3", "value": 300}
    ]

