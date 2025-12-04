"""
Data Loading Module
Handles loading data to various destinations (databases, files, data warehouses).
"""

import logging
from typing import List, Dict, Optional
from pathlib import Path
import json
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class DataLoader:
    """Base class for data loading."""
    
    def load(self, data: List[Dict]) -> bool:
        """Load data to destination."""
        raise NotImplementedError


class DatabaseLoader(DataLoader):
    """Load data to SQL databases."""
    
    def __init__(
        self,
        connection_string: str,
        table_name: str,
        if_exists: str = 'append',
        batch_size: int = 1000
    ):
        """
        Initialize database loader.
        
        Args:
            connection_string: Database connection string
            table_name: Target table name
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            batch_size: Number of records to insert per batch
        """
        self.connection_string = connection_string
        self.table_name = table_name
        self.if_exists = if_exists
        self.batch_size = batch_size
        self.engine = None
    
    def _get_engine(self):
        """Get or create database engine."""
        if self.engine is None:
            self.engine = create_engine(self.connection_string)
        return self.engine
    
    def load(self, data: List[Dict]) -> bool:
        """Load data to database."""
        if not data:
            logger.warning("No data to load")
            return False
        
        try:
            logger.info(f"Loading {len(data)} records to table: {self.table_name}")
            df = pd.DataFrame(data)
            
            engine = self._get_engine()
            
            # Load in batches for large datasets
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(df), self.batch_size):
                batch = df.iloc[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                logger.info(f"Loading batch {batch_num}/{total_batches} ({len(batch)} records)")
                
                batch.to_sql(
                    self.table_name,
                    engine,
                    if_exists=self.if_exists if i == 0 else 'append',
                    index=False,
                    method='multi'
                )
            
            logger.info(f"Successfully loaded {len(data)} records to {self.table_name}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Database error loading data: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading data to database: {e}")
            raise


class FileLoader(DataLoader):
    """Load data to files."""
    
    def __init__(self, file_path: str, file_type: str = 'json', mode: str = 'w'):
        """
        Initialize file loader.
        
        Args:
            file_path: Output file path
            file_type: File type ('json', 'csv', 'parquet')
            mode: File write mode ('w' for overwrite, 'a' for append)
        """
        self.file_path = Path(file_path)
        self.file_type = file_type
        self.mode = mode
    
    def load(self, data: List[Dict]) -> bool:
        """Load data to file."""
        if not data:
            logger.warning("No data to load")
            return False
        
        try:
            logger.info(f"Loading {len(data)} records to file: {self.file_path}")
            
            # Create parent directory if it doesn't exist
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            
            if self.file_type == 'json':
                if self.mode == 'a' and self.file_path.exists():
                    # Append to existing JSON file
                    with open(self.file_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                    if isinstance(existing_data, list):
                        existing_data.extend(data)
                        data = existing_data
                    else:
                        data = [existing_data] + data
                
                with open(self.file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str)
            
            elif self.file_type == 'csv':
                df = pd.DataFrame(data)
                df.to_csv(self.file_path, mode=self.mode, index=False, header=(self.mode == 'w'))
            
            elif self.file_type == 'parquet':
                df = pd.DataFrame(data)
                df.to_parquet(self.file_path, index=False)
            
            else:
                raise ValueError(f"Unsupported file type: {self.file_type}")
            
            logger.info(f"Successfully loaded {len(data)} records to {self.file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data to file: {e}")
            raise


class DataWarehouseLoader(DataLoader):
    """Load data to data warehouses (e.g., BigQuery, Snowflake, Redshift)."""
    
    def __init__(self, connection_string: str, table_name: str, schema: Optional[str] = None):
        """
        Initialize data warehouse loader.
        
        Args:
            connection_string: Warehouse connection string
            table_name: Target table name
            schema: Optional schema name
        """
        self.connection_string = connection_string
        self.table_name = table_name
        self.schema = schema
    
    def load(self, data: List[Dict]) -> bool:
        """Load data to data warehouse."""
        # This is a placeholder - implement with specific warehouse libraries
        # Example: google-cloud-bigquery for BigQuery, snowflake-connector for Snowflake
        logger.info(f"Loading {len(data)} records to data warehouse: {self.table_name}")
        # Implementation would go here
        return True

