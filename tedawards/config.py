"""
Configuration management for TED Awards scraper.
Environment-based configuration with sensible defaults.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager

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


# Database engine and session factory
def get_engine():
    """Get SQLAlchemy engine for database connection."""
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    database_url = f"sqlite:///{config.DB_PATH}"
    return create_engine(
        database_url,
        echo=False,
        connect_args={"check_same_thread": False}
    )


engine = get_engine()
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def get_session() -> Session:
    """Get database session as context manager."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
