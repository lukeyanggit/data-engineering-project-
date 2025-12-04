"""
Main Entry Point
Run the ETL pipeline.
"""

import argparse
import sys
from pathlib import Path

from src.utils import setup_logging
from src.config import Config, default_config
from src.extract import FileExtractor, APIExtractor
from src.transform import StandardTransformer, DataValidator
from src.load import DatabaseLoader, FileLoader
from src.pipeline import ETLPipeline


def create_sample_data():
    """Create sample data file for testing."""
    import json
    from datetime import datetime, timedelta
    
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    sample_data = []
    base_date = datetime.now()
    
    for i in range(100):
        sample_data.append({
            "id": i + 1,
            "name": f"Record {i + 1}",
            "value": 100 + i * 10,
            "timestamp": (base_date - timedelta(days=i)).isoformat(),
            "category": f"Category {i % 5}",
            "status": "active" if i % 2 == 0 else "inactive"
        })
    
    sample_file = data_dir / "sample_data.json"
    with open(sample_file, 'w') as f:
        json.dump(sample_data, f, indent=2)
    
    print(f"Created sample data file: {sample_file}")
    return str(sample_file)


def main():
    """Main function to run the ETL pipeline."""
    parser = argparse.ArgumentParser(description="Data Engineering ETL Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.json",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["file", "api", "sample"],
        default="sample",
        help="Data source type"
    )
    parser.add_argument(
        "--source-path",
        type=str,
        help="Path to source data file (for file source)"
    )
    parser.add_argument(
        "--api-url",
        type=str,
        help="API URL (for api source)"
    )
    parser.add_argument(
        "--output",
        type=str,
        choices=["file", "database"],
        default="file",
        help="Output destination"
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="data/output/processed_data.json",
        help="Output file path (for file output)"
    )
    parser.add_argument(
        "--create-sample",
        action="store_true",
        help="Create sample data file"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config_path = Path(args.config)
    if config_path.exists():
        config = Config(str(config_path))
    else:
        config = Config()
        # Use defaults
        for key, value in default_config.items():
            config.set(key, value)
    
    # Setup logging
    log_config = config.get("logging", {})
    setup_logging(
        level=log_config.get("level", "INFO"),
        log_file=log_config.get("file")
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Data Engineering Pipeline")
    
    # Create sample data if requested
    if args.create_sample:
        create_sample_data()
        return
    
    # Setup extractor
    if args.source == "sample":
        sample_file = create_sample_data()
        extractor = FileExtractor(sample_file)
    elif args.source == "file":
        if not args.source_path:
            logger.error("--source-path required for file source")
            sys.exit(1)
        extractor = FileExtractor(args.source_path)
    elif args.source == "api":
        if not args.api_url:
            logger.error("--api-url required for api source")
            sys.exit(1)
        extractor = APIExtractor(args.api_url)
    else:
        logger.error(f"Unknown source type: {args.source}")
        sys.exit(1)
    
    # Setup transformer
    pipeline_config = config.get("pipeline", {})
    transformer = StandardTransformer(
        drop_duplicates=pipeline_config.get("drop_duplicates", True),
        drop_null_threshold=pipeline_config.get("drop_null_threshold", 0.5)
    )
    
    # Setup validator
    validator = DataValidator(
        required_columns=["id", "name"],
        column_types={"id": int, "value": (int, float)}
    )
    
    # Setup loader
    if args.output == "file":
        Path(args.output_path).parent.mkdir(parents=True, exist_ok=True)
        loader = FileLoader(args.output_path, file_type="json")
    elif args.output == "database":
        db_config = config.get("database", {})
        connection_string = (
            f"postgresql://{db_config.get('user', 'postgres')}:"
            f"{db_config.get('password', 'postgres')}@"
            f"{db_config.get('host', 'localhost')}:"
            f"{db_config.get('port', 5432)}/"
            f"{db_config.get('database', 'data_engineering')}"
        )
        loader = DatabaseLoader(
            connection_string,
            table_name="processed_data",
            batch_size=pipeline_config.get("batch_size", 1000)
        )
    else:
        logger.error(f"Unknown output type: {args.output}")
        sys.exit(1)
    
    # Create and run pipeline
    pipeline = ETLPipeline(
        extractor=extractor,
        transformer=transformer,
        validator=validator,
        loader=loader,
        name="Main ETL Pipeline"
    )
    
    result = pipeline.run()
    
    # Print results
    print("\n" + "="*50)
    print("Pipeline Execution Results")
    print("="*50)
    print(f"Success: {result['success']}")
    print(f"Duration: {result['duration_seconds']:.2f} seconds")
    print(f"Records Extracted: {result['records_extracted']}")
    print(f"Records Transformed: {result['records_transformed']}")
    print(f"Records Validated: {result.get('records_validated', 0)}")
    print(f"Records Loaded: {result['records_loaded']}")
    if result.get('records_invalid', 0) > 0:
        print(f"Records Invalid: {result['records_invalid']}")
    if result['errors']:
        print(f"\nErrors: {len(result['errors'])}")
        for error in result['errors']:
            print(f"  - {error['error']}")
    print("="*50)
    
    # Print statistics
    stats = pipeline.get_stats()
    print("\nPipeline Statistics:")
    print(f"  Total Runs: {stats['runs']}")
    print(f"  Successful: {stats['successful_runs']}")
    print(f"  Failed: {stats['failed_runs']}")
    print(f"  Records Processed: {stats['records_processed']}")
    print(f"  Records Failed: {stats['records_failed']}")
    
    sys.exit(0 if result['success'] else 1)


if __name__ == "__main__":
    import logging
    main()

