# Data Engineering Project

A comprehensive, production-ready data engineering project with ETL pipeline capabilities. This project provides a modular architecture for extracting, transforming, and loading data from various sources to multiple destinations.

## Features

- **Modular ETL Pipeline**: Clean separation of extraction, transformation, and loading logic
- **Multiple Data Sources**: Support for APIs, files (JSON, CSV), and databases
- **Data Validation**: Built-in data quality checks and validation
- **Flexible Transformations**: Configurable data cleaning and transformation operations
- **Multiple Destinations**: Load to files, databases, or data warehouses
- **Docker Support**: Easy setup with Docker Compose for PostgreSQL and Redis
- **Comprehensive Testing**: Unit tests with pytest
- **Logging & Monitoring**: Built-in logging and pipeline statistics
- **Configuration Management**: JSON and environment variable support

## Project Structure

```
data-engineering-project/
├── src/                    # Source code
│   ├── __init__.py
│   ├── extract.py         # Data extraction modules
│   ├── transform.py       # Data transformation modules
│   ├── load.py            # Data loading modules
│   ├── pipeline.py        # ETL pipeline orchestrator
│   ├── config.py          # Configuration management
│   └── utils.py           # Utility functions
├── tests/                  # Test files
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_pipeline.py
│   └── conftest.py
├── config/                 # Configuration files
│   └── config.json
├── data/                   # Data directories (created at runtime)
│   ├── raw/               # Raw input data
│   └── output/            # Processed output data
├── logs/                   # Log files (created at runtime)
├── docker/                 # Docker setup files
│   └── init.sql           # Database initialization
├── main.py                 # Main entry point
├── requirements.txt       # Python dependencies
├── docker-compose.yml     # Docker services configuration
├── pytest.ini            # Pytest configuration
├── setup.py              # Package setup
└── README.md             # This file
```

## Installation

### Prerequisites

- Python 3.8 or higher
- Docker and Docker Compose (optional, for database services)
- pip (Python package manager)

### Setup

1. **Clone the repository** (or navigate to the project directory)

2. **Create a virtual environment** (recommended):
   ```bash
   python -m venv venv
   
   # On Windows
   venv\Scripts\activate
   
   # On Linux/Mac
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up Docker services** (optional):
   ```bash
   docker-compose up -d
   ```
   This will start PostgreSQL and Redis containers.

## Usage

### Basic Usage

Run the pipeline with sample data:
```bash
python main.py --source sample --output file
```

### Command Line Options

```bash
python main.py [OPTIONS]

Options:
  --config PATH              Path to configuration file (default: config/config.json)
  --source {file,api,sample} Data source type (default: sample)
  --source-path PATH         Path to source data file (for file source)
  --api-url URL              API URL (for api source)
  --output {file,database}   Output destination (default: file)
  --output-path PATH         Output file path (for file output)
  --create-sample            Create sample data file
```

### Examples

1. **Process sample data to file**:
   ```bash
   python main.py --source sample --output file --output-path data/output/result.json
   ```

2. **Process JSON file**:
   ```bash
   python main.py --source file --source-path data/raw/input.json --output file
   ```

3. **Process data from API**:
   ```bash
   python main.py --source api --api-url https://api.example.com/data --output file
   ```

4. **Load to PostgreSQL database**:
   ```bash
   python main.py --source sample --output database
   ```
   (Make sure Docker services are running or database is configured)

5. **Create sample data**:
   ```bash
   python main.py --create-sample
   ```

### Programmatic Usage

You can also use the pipeline programmatically:

```python
from src.extract import FileExtractor
from src.transform import StandardTransformer, DataValidator
from src.load import FileLoader
from src.pipeline import ETLPipeline

# Setup components
extractor = FileExtractor("data/raw/input.json")
transformer = StandardTransformer(
    drop_duplicates=True,
    drop_null_threshold=0.5
)
validator = DataValidator(
    required_columns=["id", "name"],
    column_types={"id": int}
)
loader = FileLoader("data/output/result.json")

# Create and run pipeline
pipeline = ETLPipeline(
    extractor=extractor,
    transformer=transformer,
    validator=validator,
    loader=loader
)

result = pipeline.run()
print(f"Success: {result['success']}")
print(f"Records processed: {result['records_extracted']}")
```

## Configuration

### Configuration File

Edit `config/config.json` to customize settings:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "data_engineering",
    "user": "postgres",
    "password": "postgres"
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "logs/pipeline.log"
  },
  "pipeline": {
    "batch_size": 1000,
    "drop_duplicates": true,
    "drop_null_threshold": 0.5
  }
}
```

### Environment Variables

You can override configuration using environment variables with the `DE_` prefix:

```bash
export DE_DATABASE_HOST=localhost
export DE_DATABASE_PORT=5432
export DE_LOGGING_LEVEL=DEBUG
```

## Testing

Run tests with pytest:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_extract.py

# Run with verbose output
pytest -v
```

## Docker Services

### Start Services

```bash
docker-compose up -d
```

### Stop Services

```bash
docker-compose down
```

### View Logs

```bash
docker-compose logs -f postgres
```

### Database Connection

Once services are running, you can connect to PostgreSQL:

- Host: `localhost`
- Port: `5432`
- Database: `data_engineering`
- User: `postgres`
- Password: `postgres`

## Architecture

### Extraction Module (`src/extract.py`)

Supports multiple data sources:
- **APIExtractor**: Extract from REST APIs
- **FileExtractor**: Extract from JSON/CSV files
- **DatabaseExtractor**: Extract from SQL databases

### Transformation Module (`src/transform.py`)

Data cleaning and transformation:
- **StandardTransformer**: Common transformations (deduplication, null handling, date conversion)
- **DataValidator**: Data quality validation

### Loading Module (`src/load.py`)

Multiple destination support:
- **DatabaseLoader**: Load to SQL databases
- **FileLoader**: Load to JSON/CSV/Parquet files
- **DataWarehouseLoader**: Load to data warehouses (extensible)

### Pipeline Orchestrator (`src/pipeline.py`)

Coordinates the complete ETL process:
- Error handling and logging
- Statistics tracking
- Result reporting

## Extending the Project

### Adding a New Data Source

1. Create a new extractor class inheriting from `DataExtractor`:

```python
from src.extract import DataExtractor

class CustomExtractor(DataExtractor):
    def extract(self):
        # Your extraction logic
        return data
```

### Adding Custom Transformations

```python
from src.transform import StandardTransformer

def custom_transform(df):
    # Your transformation logic
    return df

transformer = StandardTransformer(
    custom_transforms=[custom_transform]
)
```

### Adding a New Destination

1. Create a new loader class inheriting from `DataLoader`:

```python
from src.load import DataLoader

class CustomLoader(DataLoader):
    def load(self, data):
        # Your loading logic
        return True
```

## Best Practices

1. **Error Handling**: Always handle exceptions in custom extractors/transformers/loaders
2. **Logging**: Use the logging module for debugging and monitoring
3. **Validation**: Validate data before loading to catch issues early
4. **Testing**: Write tests for custom components
5. **Configuration**: Use configuration files or environment variables for settings
6. **Documentation**: Document custom transformations and data schemas

## Troubleshooting

### Database Connection Issues

- Ensure Docker services are running: `docker-compose ps`
- Check database credentials in `config/config.json`
- Verify network connectivity to database host

### Import Errors

- Ensure virtual environment is activated
- Install dependencies: `pip install -r requirements.txt`
- Check Python version: `python --version` (should be 3.8+)

### File Not Found Errors

- Ensure data directories exist or are created automatically
- Check file paths are relative to project root or absolute

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run tests and ensure they pass
6. Submit a pull request

## License

This project is open source and available under the MIT License.

## Future Enhancements

- [ ] Airflow/Dagster integration for workflow orchestration
- [ ] Support for more data sources (Kafka, S3, etc.)
- [ ] Real-time streaming pipeline
- [ ] Data quality metrics dashboard
- [ ] Automated testing in CI/CD
- [ ] Support for more data warehouses (BigQuery, Snowflake, Redshift)
- [ ] Data lineage tracking
- [ ] Incremental data processing

## Support

For issues, questions, or contributions, please open an issue on the repository.
