"""
Configuration Management Module
Handles loading and managing configuration settings.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class Config:
    """Configuration manager."""
    
    def __init__(self, config_path: Optional[str] = None, env_prefix: str = "DE_"):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to JSON configuration file
            env_prefix: Prefix for environment variables
        """
        self.config: Dict[str, Any] = {}
        self.env_prefix = env_prefix
        
        # Load from file if provided
        if config_path:
            self.load_from_file(config_path)
        
        # Override with environment variables
        self.load_from_env()
    
    def load_from_file(self, config_path: str):
        """Load configuration from JSON file."""
        try:
            path = Path(config_path)
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
                logger.info(f"Loaded configuration from {config_path}")
            else:
                logger.warning(f"Configuration file not found: {config_path}")
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")
            raise
    
    def load_from_env(self):
        """Load configuration from environment variables."""
        for key, value in os.environ.items():
            if key.startswith(self.env_prefix):
                # Remove prefix and convert to nested dict keys
                config_key = key[len(self.env_prefix):].lower()
                self.config[config_key] = value
                logger.debug(f"Loaded config from env: {config_key}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any):
        """Set configuration value."""
        self.config[key] = value
    
    def to_dict(self) -> Dict[str, Any]:
        """Get configuration as dictionary."""
        return self.config.copy()


# Default configuration
default_config = {
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
        "drop_duplicates": True,
        "drop_null_threshold": 0.5
    }
}

