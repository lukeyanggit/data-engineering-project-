-- Initialize database schema

CREATE TABLE IF NOT EXISTS processed_data (
    id SERIAL PRIMARY KEY,
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processed_data_created_at ON processed_data(created_at);

-- Create a generic staging table for ETL operations
CREATE TABLE IF NOT EXISTS staging_data (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_data_status ON staging_data(status);

