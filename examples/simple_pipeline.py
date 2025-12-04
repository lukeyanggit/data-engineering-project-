"""
Simple Pipeline Example
Demonstrates basic ETL pipeline usage.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract import FileExtractor
from src.transform import StandardTransformer
from src.load import FileLoader
from src.pipeline import ETLPipeline
from src.utils import setup_logging

def main():
    """Run a simple ETL pipeline."""
    # Setup logging
    setup_logging(level="INFO")
    
    # Create sample data file if it doesn't exist
    data_file = Path("data/sample_data.json")
    if not data_file.exists():
        print("Creating sample data...")
        data_file.parent.mkdir(parents=True, exist_ok=True)
        import json
        sample_data = [
            {"id": i, "name": f"Item {i}", "value": i * 10}
            for i in range(1, 11)
        ]
        with open(data_file, 'w') as f:
            json.dump(sample_data, f, indent=2)
    
    # Setup pipeline components
    extractor = FileExtractor(str(data_file))
    transformer = StandardTransformer(
        drop_duplicates=True,
        drop_null_threshold=0.5
    )
    loader = FileLoader("data/output/processed.json")
    
    # Create and run pipeline
    pipeline = ETLPipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        name="Simple Pipeline"
    )
    
    # Run pipeline
    result = pipeline.run()
    
    # Print results
    print("\n" + "="*50)
    print("Pipeline Results")
    print("="*50)
    print(f"Success: {result['success']}")
    print(f"Duration: {result['duration_seconds']:.2f} seconds")
    print(f"Records Extracted: {result['records_extracted']}")
    print(f"Records Transformed: {result['records_transformed']}")
    print(f"Records Loaded: {result['records_loaded']}")
    print("="*50)

if __name__ == "__main__":
    main()

