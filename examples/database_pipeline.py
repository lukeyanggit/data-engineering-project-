"""
Database Pipeline Example
Demonstrates loading data to a PostgreSQL database.
Note: Requires Docker services to be running or a configured database.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract import FileExtractor
from src.transform import StandardTransformer
from src.load import DatabaseLoader
from src.pipeline import ETLPipeline
from src.utils import setup_logging

def main():
    """Run a database-based ETL pipeline."""
    # Setup logging
    setup_logging(level="INFO")
    
    # Database connection string
    # Update these values based on your database configuration
    connection_string = (
        "postgresql://postgres:postgres@localhost:5432/data_engineering"
    )
    
    # Create sample data if needed
    data_file = Path("data/sample_data.json")
    if not data_file.exists():
        print("Creating sample data...")
        data_file.parent.mkdir(parents=True, exist_ok=True)
        import json
        sample_data = [
            {"id": i, "name": f"Record {i}", "value": i * 10, "category": f"Cat {i % 3}"}
            for i in range(1, 21)
        ]
        with open(data_file, 'w') as f:
            json.dump(sample_data, f, indent=2)
    
    # Setup pipeline components
    extractor = FileExtractor(str(data_file))
    
    transformer = StandardTransformer(
        drop_duplicates=True,
        drop_null_threshold=0.5
    )
    
    loader = DatabaseLoader(
        connection_string=connection_string,
        table_name="processed_data",
        if_exists="append",  # or "replace" to overwrite
        batch_size=100
    )
    
    # Create and run pipeline
    pipeline = ETLPipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        name="Database Pipeline"
    )
    
    # Run pipeline
    result = pipeline.run()
    
    # Print results
    print("\n" + "="*50)
    print("Database Pipeline Results")
    print("="*50)
    print(f"Success: {result['success']}")
    if result['success']:
        print(f"Records Extracted: {result['records_extracted']}")
        print(f"Records Transformed: {result['records_transformed']}")
        print(f"Records Loaded: {result['records_loaded']}")
        print(f"\nData loaded to table: processed_data")
        print(f"Query with: SELECT * FROM processed_data;")
    else:
        print("Errors:")
        for error in result.get('errors', []):
            print(f"  - {error['error']}")
    print("="*50)

if __name__ == "__main__":
    main()

