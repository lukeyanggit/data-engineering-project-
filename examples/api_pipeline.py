"""
API Pipeline Example
Demonstrates extracting data from a REST API.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract import APIExtractor
from src.transform import StandardTransformer, DataValidator
from src.load import FileLoader
from src.pipeline import ETLPipeline
from src.utils import setup_logging

def main():
    """Run an API-based ETL pipeline."""
    # Setup logging
    setup_logging(level="INFO")
    
    # Example: Using JSONPlaceholder API (public test API)
    api_url = "https://jsonplaceholder.typicode.com/posts"
    
    # Setup pipeline components
    extractor = APIExtractor(
        url=api_url,
        headers={"Accept": "application/json"}
    )
    
    transformer = StandardTransformer(
        drop_duplicates=True,
        date_columns=[]  # Add date columns if API returns them
    )
    
    validator = DataValidator(
        required_columns=["id", "title"],
        column_types={"id": int}
    )
    
    loader = FileLoader("data/output/api_data.json")
    
    # Create and run pipeline
    pipeline = ETLPipeline(
        extractor=extractor,
        transformer=transformer,
        validator=validator,
        loader=loader,
        name="API Pipeline"
    )
    
    # Run pipeline
    result = pipeline.run()
    
    # Print results
    print("\n" + "="*50)
    print("API Pipeline Results")
    print("="*50)
    print(f"Success: {result['success']}")
    if result['success']:
        print(f"Records Extracted: {result['records_extracted']}")
        print(f"Records Validated: {result.get('records_validated', 0)}")
        print(f"Records Loaded: {result['records_loaded']}")
    else:
        print("Errors:")
        for error in result.get('errors', []):
            print(f"  - {error['error']}")
    print("="*50)

if __name__ == "__main__":
    main()

