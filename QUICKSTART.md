# Quick Start Guide

Get up and running with the Data Engineering Project in 5 minutes!

## Step 1: Install Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Step 2: Run Your First Pipeline

The simplest way to get started:

```bash
python main.py --source sample --output file
```

This will:
1. Create sample data automatically
2. Extract the data
3. Transform it (remove duplicates, handle nulls)
4. Validate it
5. Load it to `data/output/processed_data.json`

## Step 3: View Results

Check the output file:
```bash
# Windows
type data\output\processed_data.json

# Linux/Mac
cat data/output/processed_data.json
```

## Step 4: Try Different Sources

### Process a JSON file:
```bash
# First, create a sample file
python main.py --create-sample

# Then process it
python main.py --source file --source-path data/sample_data.json --output file
```

### Process data from an API:
```bash
python main.py --source api --api-url https://jsonplaceholder.typicode.com/posts --output file
```

## Step 5: Use Docker (Optional)

If you want to use PostgreSQL:

```bash
# Start Docker services
docker-compose up -d

# Run pipeline with database output
python main.py --source sample --output database

# Check the database
docker exec -it data_engineering_db psql -U postgres -d data_engineering -c "SELECT * FROM processed_data LIMIT 5;"
```

## Step 6: Run Tests

```bash
pytest
```

## Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Check out [examples/](examples/) for more advanced usage
- Customize `config/config.json` for your needs
- Explore the source code in [src/](src/)

## Common Issues

**Import errors?**
- Make sure virtual environment is activated
- Run `pip install -r requirements.txt`

**Database connection errors?**
- Make sure Docker is running: `docker-compose ps`
- Check database credentials in `config/config.json`

**File not found?**
- The pipeline creates directories automatically
- Check file paths are correct

## Need Help?

- Check the [README.md](README.md) for detailed documentation
- Review example scripts in [examples/](examples/)
- Run tests to verify installation: `pytest`

Happy data engineering! 🚀

