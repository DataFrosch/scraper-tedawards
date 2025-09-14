import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'tedawards')
    DB_USER = os.getenv('DB_USER', 'tedawards')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'tedawards')

    TED_DATA_DIR = Path(os.getenv('TED_DATA_DIR', './data'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


config = Config()