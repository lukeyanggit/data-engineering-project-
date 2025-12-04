"""
Data Transformation Module
Handles data cleaning, validation, and transformation.
"""

import logging
from typing import Dict, List, Optional, Callable, Tuple
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Base class for data transformation."""
    
    def transform(self, data: List[Dict]) -> List[Dict]:
        """Transform data."""
        raise NotImplementedError


class StandardTransformer(DataTransformer):
    """Standard data transformation with common operations."""
    
    def __init__(
        self,
        drop_duplicates: bool = True,
        drop_null_threshold: float = 0.5,
        date_columns: Optional[List[str]] = None,
        rename_columns: Optional[Dict[str, str]] = None,
        custom_transforms: Optional[List[Callable]] = None
    ):
        """
        Initialize transformer.
        
        Args:
            drop_duplicates: Whether to drop duplicate records
            drop_null_threshold: Threshold for dropping columns with too many nulls (0-1)
            date_columns: List of column names to convert to datetime
            rename_columns: Dictionary mapping old names to new names
            custom_transforms: List of custom transformation functions
        """
        self.drop_duplicates = drop_duplicates
        self.drop_null_threshold = drop_null_threshold
        self.date_columns = date_columns or []
        self.rename_columns = rename_columns or {}
        self.custom_transforms = custom_transforms or []
    
    def transform(self, data: List[Dict]) -> List[Dict]:
        """Transform data using configured operations."""
        if not data:
            logger.warning("No data to transform")
            return []
        
        try:
            logger.info(f"Transforming {len(data)} records")
            df = pd.DataFrame(data)
            
            # Rename columns
            if self.rename_columns:
                df = df.rename(columns=self.rename_columns)
                logger.info(f"Renamed columns: {self.rename_columns}")
            
            # Convert date columns
            for col in self.date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logger.info(f"Converted {col} to datetime")
            
            # Drop columns with too many nulls
            if self.drop_null_threshold > 0:
                null_ratio = df.isnull().sum() / len(df)
                cols_to_drop = null_ratio[null_ratio > self.drop_null_threshold].index.tolist()
                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)
                    logger.info(f"Dropped columns with >{self.drop_null_threshold*100}% nulls: {cols_to_drop}")
            
            # Drop duplicates
            if self.drop_duplicates:
                initial_count = len(df)
                df = df.drop_duplicates()
                duplicates_removed = initial_count - len(df)
                if duplicates_removed > 0:
                    logger.info(f"Removed {duplicates_removed} duplicate records")
            
            # Apply custom transformations
            for transform_func in self.custom_transforms:
                df = transform_func(df)
                logger.info(f"Applied custom transformation: {transform_func.__name__}")
            
            # Add metadata
            df['_transformed_at'] = datetime.utcnow().isoformat()
            
            result = df.to_dict('records')
            logger.info(f"Transformation complete: {len(result)} records")
            return result
            
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            raise


class DataValidator:
    """Validate data quality."""
    
    def __init__(
        self,
        required_columns: Optional[List[str]] = None,
        column_types: Optional[Dict[str, type]] = None,
        value_constraints: Optional[Dict[str, Callable]] = None
    ):
        """
        Initialize validator.
        
        Args:
            required_columns: List of required column names
            column_types: Dictionary mapping column names to expected types
            value_constraints: Dictionary mapping column names to validation functions
        """
        self.required_columns = required_columns or []
        self.column_types = column_types or {}
        self.value_constraints = value_constraints or {}
    
    def validate(self, data: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate data and return valid and invalid records.
        
        Returns:
            Tuple of (valid_records, invalid_records)
        """
        if not data:
            return [], []
        
        valid_records = []
        invalid_records = []
        
        for record in data:
            is_valid = True
            errors = []
            
            # Check required columns
            for col in self.required_columns:
                if col not in record or record[col] is None:
                    is_valid = False
                    errors.append(f"Missing required column: {col}")
            
            # Check column types
            for col, expected_type in self.column_types.items():
                if col in record and record[col] is not None:
                    if not isinstance(record[col], expected_type):
                        is_valid = False
                        errors.append(f"Invalid type for {col}: expected {expected_type}, got {type(record[col])}")
            
            # Check value constraints
            for col, constraint_func in self.value_constraints.items():
                if col in record and record[col] is not None:
                    try:
                        if not constraint_func(record[col]):
                            is_valid = False
                            errors.append(f"Value constraint failed for {col}")
                    except Exception as e:
                        is_valid = False
                        errors.append(f"Error validating {col}: {e}")
            
            if is_valid:
                valid_records.append(record)
            else:
                record['_validation_errors'] = errors
                invalid_records.append(record)
        
        logger.info(f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid")
        return valid_records, invalid_records

