"""
Data Extraction Module
Handles data extraction from various sources (APIs, files, databases).
"""

import json
import logging
import requests
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class DataExtractor:
    """Base class for data extraction."""
    
    def extract(self) -> List[Dict]:
        """Extract data from source."""
        raise NotImplementedError


class APIExtractor(DataExtractor):
    """Extract data from REST APIs."""
    
    def __init__(self, url: str, headers: Optional[Dict] = None, params: Optional[Dict] = None):
        """
        Initialize API extractor.
        
        Args:
            url: API endpoint URL
            headers: Optional HTTP headers
            params: Optional query parameters
        """
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
    
    def extract(self) -> List[Dict]:
        """Extract data from API."""
        try:
            logger.info(f"Extracting data from API: {self.url}")
            response = requests.get(self.url, headers=self.headers, params=self.params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Try common keys
                for key in ['data', 'results', 'items', 'records']:
                    if key in data:
                        return data[key] if isinstance(data[key], list) else [data[key]]
                return [data]
            else:
                logger.warning(f"Unexpected API response format: {type(data)}")
                return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Error extracting data from API: {e}")
            raise


class FileExtractor(DataExtractor):
    """Extract data from files (JSON, CSV, etc.)."""
    
    def __init__(self, file_path: str, file_type: str = 'auto'):
        """
        Initialize file extractor.
        
        Args:
            file_path: Path to the file
            file_type: File type ('json', 'csv', 'auto')
        """
        self.file_path = Path(file_path)
        self.file_type = file_type or self._detect_file_type()
    
    def _detect_file_type(self) -> str:
        """Detect file type from extension."""
        ext = self.file_path.suffix.lower()
        if ext == '.json':
            return 'json'
        elif ext == '.csv':
            return 'csv'
        else:
            raise ValueError(f"Unsupported file type: {ext}")
    
    def extract(self) -> List[Dict]:
        """Extract data from file."""
        try:
            logger.info(f"Extracting data from file: {self.file_path}")
            
            if not self.file_path.exists():
                raise FileNotFoundError(f"File not found: {self.file_path}")
            
            if self.file_type == 'json':
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
                    elif isinstance(data, dict):
                        return [data]
                    else:
                        return []
            
            elif self.file_type == 'csv':
                df = pd.read_csv(self.file_path)
                return df.to_dict('records')
            
            else:
                raise ValueError(f"Unsupported file type: {self.file_type}")
                
        except Exception as e:
            logger.error(f"Error extracting data from file: {e}")
            raise


class DatabaseExtractor(DataExtractor):
    """Extract data from databases."""
    
    def __init__(self, connection_string: str, query: str):
        """
        Initialize database extractor.
        
        Args:
            connection_string: Database connection string
            query: SQL query to execute
        """
        self.connection_string = connection_string
        self.query = query
    
    def extract(self) -> List[Dict]:
        """Extract data from database."""
        try:
            logger.info("Extracting data from database")
            # This is a placeholder - implement with actual database library
            # Example: psycopg2 for PostgreSQL, pymongo for MongoDB, etc.
            import sqlalchemy
            engine = sqlalchemy.create_engine(self.connection_string)
            df = pd.read_sql(self.query, engine)
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error extracting data from database: {e}")
            raise

