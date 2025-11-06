"""
Configuration management for TED Awards scraper.
Environment-based configuration with sensible defaults.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration."""

    # Database configuration - SQLite
    DB_PATH = Path(os.getenv('DB_PATH', './data/tedawards.db'))

    # Data storage directory
    TED_DATA_DIR = Path(os.getenv('TED_DATA_DIR', './data'))

    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


config = Config()
