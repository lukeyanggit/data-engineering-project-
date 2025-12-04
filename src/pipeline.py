"""
ETL Pipeline Module
Orchestrates the complete ETL process.
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime
import traceback

from .extract import DataExtractor
from .transform import DataTransformer, DataValidator
from .load import DataLoader

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Complete ETL pipeline orchestrator."""
    
    def __init__(
        self,
        extractor: DataExtractor,
        transformer: Optional[DataTransformer] = None,
        validator: Optional[DataValidator] = None,
        loader: Optional[DataLoader] = None,
        name: str = "ETL Pipeline"
    ):
        """
        Initialize ETL pipeline.
        
        Args:
            extractor: Data extractor instance
            transformer: Optional data transformer instance
            validator: Optional data validator instance
            loader: Data loader instance
            name: Pipeline name for logging
        """
        self.extractor = extractor
        self.transformer = transformer
        self.validator = validator
        self.loader = loader
        self.name = name
        self.stats = {
            'runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'records_processed': 0,
            'records_failed': 0
        }
    
    def run(self) -> Dict:
        """
        Run the complete ETL pipeline.
        
        Returns:
            Dictionary with execution results and statistics
        """
        start_time = datetime.utcnow()
        self.stats['runs'] += 1
        
        logger.info(f"Starting {self.name}")
        
        result = {
            'success': False,
            'start_time': start_time.isoformat(),
            'end_time': None,
            'duration_seconds': None,
            'records_extracted': 0,
            'records_transformed': 0,
            'records_validated': 0,
            'records_loaded': 0,
            'errors': []
        }
        
        try:
            # Extract
            logger.info("Step 1: Extraction")
            raw_data = self.extractor.extract()
            result['records_extracted'] = len(raw_data)
            self.stats['records_processed'] += len(raw_data)
            logger.info(f"Extracted {len(raw_data)} records")
            
            if not raw_data:
                logger.warning("No data extracted, skipping remaining steps")
                result['success'] = True
                return result
            
            # Transform
            transformed_data = raw_data
            if self.transformer:
                logger.info("Step 2: Transformation")
                transformed_data = self.transformer.transform(raw_data)
                result['records_transformed'] = len(transformed_data)
                logger.info(f"Transformed {len(transformed_data)} records")
            
            # Validate
            valid_data = transformed_data
            invalid_data = []
            if self.validator:
                logger.info("Step 3: Validation")
                valid_data, invalid_data = self.validator.validate(transformed_data)
                result['records_validated'] = len(valid_data)
                result['records_invalid'] = len(invalid_data)
                logger.info(f"Validated: {len(valid_data)} valid, {len(invalid_data)} invalid")
                
                if invalid_data:
                    self.stats['records_failed'] += len(invalid_data)
                    logger.warning(f"Found {len(invalid_data)} invalid records")
            
            # Load
            if self.loader and valid_data:
                logger.info("Step 4: Loading")
                load_success = self.loader.load(valid_data)
                if load_success:
                    result['records_loaded'] = len(valid_data)
                    logger.info(f"Loaded {len(valid_data)} records")
                else:
                    raise Exception("Failed to load data")
            
            # Success
            end_time = datetime.utcnow()
            result['end_time'] = end_time.isoformat()
            result['duration_seconds'] = (end_time - start_time).total_seconds()
            result['success'] = True
            self.stats['successful_runs'] += 1
            
            logger.info(f"{self.name} completed successfully in {result['duration_seconds']:.2f} seconds")
            
        except Exception as e:
            end_time = datetime.utcnow()
            result['end_time'] = end_time.isoformat()
            result['duration_seconds'] = (end_time - start_time).total_seconds()
            result['success'] = False
            result['errors'].append({
                'error': str(e),
                'traceback': traceback.format_exc()
            })
            self.stats['failed_runs'] += 1
            
            logger.error(f"{self.name} failed: {e}")
            logger.error(traceback.format_exc())
        
        return result
    
    def get_stats(self) -> Dict:
        """Get pipeline statistics."""
        return self.stats.copy()

