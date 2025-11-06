import logging
import os
import requests
import tarfile
from pathlib import Path
from datetime import date, timedelta
from typing import List
from contextlib import contextmanager
from dotenv import load_dotenv
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .parsers import ParserFactory
from .models import Base, TEDDocument, ContractingBody, Contract, Award, Contractor
from .schema import TedAwardDataModel, TedParserResultModel

load_dotenv()

logger = logging.getLogger(__name__)

# Database setup
DB_PATH = Path(os.getenv('DB_PATH', './data/tedawards.db'))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    echo=False,
    connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

# Data directory setup
DATA_DIR = Path(os.getenv('TED_DATA_DIR', './data'))
DATA_DIR.mkdir(exist_ok=True)

# Parser factory (module-level singleton)
parser_factory = ParserFactory()

# Initialize database schema
Base.metadata.create_all(engine)


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


def get_day_number(target_date: date) -> int:
    """Calculate TED day number for a given date."""
    year = target_date.year
    start_of_year = date(year, 1, 1)
    days_since_start = (target_date - start_of_year).days + 1
    return year * 100000 + days_since_start


def download_and_extract(package_url: str, target_date: date, data_dir: Path = DATA_DIR) -> List[Path]:
    """Download and extract daily package, return list of XML and ZIP files."""
    date_str = target_date.strftime('%Y-%m-%d')
    archive_path = data_dir / f"{date_str}.tar.gz"
    extract_dir = data_dir / date_str

    # Check if already downloaded and extracted - case insensitive
    existing_xml = (list(extract_dir.glob('**/*.xml')) + list(extract_dir.glob('**/*.XML'))) if extract_dir.exists() else []
    existing_zip = (list(extract_dir.glob('**/*.zip')) + list(extract_dir.glob('**/*.ZIP'))) if extract_dir.exists() else []

    if existing_xml or existing_zip:
        logger.info(f"Using existing data for {date_str}")
        return existing_xml + existing_zip

    # Download package
    logger.info(f"Downloading package from {package_url}")
    try:
        response = requests.get(package_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to download package: {e}")
        raise

    # Save and extract
    archive_path.write_bytes(response.content)
    logger.debug(f"Downloaded {len(response.content)} bytes")

    extract_dir.mkdir(exist_ok=True)
    try:
        with tarfile.open(archive_path, 'r:gz') as tar_file:
            tar_file.extractall(extract_dir)
    except tarfile.TarError as e:
        logger.error(f"Failed to extract package: {e}")
        raise

    # Clean up archive file
    archive_path.unlink()

    # Look for both XML files (newer format) and ZIP files (2007 text format) - case insensitive
    xml_files = list(extract_dir.glob('**/*.xml')) + list(extract_dir.glob('**/*.XML'))
    zip_files = list(extract_dir.glob('**/*.zip')) + list(extract_dir.glob('**/*.ZIP'))

    all_files = xml_files + zip_files
    return all_files


def process_file(file_path: Path) -> TedParserResultModel:
    """Process a single file (XML or ZIP) and return parser result."""
    try:
        # Get appropriate parser for this file
        parser = parser_factory.get_parser(file_path)
        if not parser:
            logger.debug(f"No parser available for {file_path.name}")
            return None

        # Parse file - returns TedParserResultModel
        result = parser.parse_xml_file(file_path)
        if not result:
            logger.debug(f"Failed to parse {file_path.name} with {parser.get_format_name()}")
            return None

        logger.debug(f"Parsed {file_path.name} using {parser.get_format_name()}, found {len(result.awards)} award documents")
        return result

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        raise


def save_awards(session: Session, awards: List[TedAwardDataModel]) -> int:
    """Save award data to database."""
    count = 0

    for award_data in awards:
        try:
            # Insert document with INSERT OR IGNORE
            insert_func = sqlite_insert if engine.dialect.name == 'sqlite' else pg_insert
            stmt = insert_func(TEDDocument).values(**award_data.document.model_dump())
            stmt = stmt.on_conflict_do_nothing(index_elements=['doc_id'])
            session.execute(stmt)
            session.flush()

            # Get document (either newly inserted or existing)
            doc = session.execute(
                select(TEDDocument).where(TEDDocument.doc_id == award_data.document.doc_id)
            ).scalar_one()

            # Insert contracting body
            cb_data = award_data.contracting_body.model_dump()
            cb_data['ted_doc_id'] = doc.doc_id
            cb = ContractingBody(**cb_data)
            session.add(cb)
            session.flush()

            # Insert contract
            contract_data = award_data.contract.model_dump()
            contract_data['ted_doc_id'] = doc.doc_id
            contract_data['contracting_body_id'] = cb.id
            contract_data.pop('performance_nuts_code', None)
            contract = Contract(**contract_data)
            session.add(contract)
            session.flush()

            # Insert awards and contractors
            for award_item in award_data.awards:
                award_dict = award_item.model_dump()
                contractors_data = award_dict.pop('contractors', [])
                award_dict['contract_id'] = contract.id

                award = Award(**award_dict)
                session.add(award)
                session.flush()

                # Insert contractors with INSERT OR IGNORE
                for contractor_data in contractors_data:
                    stmt = insert_func(Contractor).values(**contractor_data)
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=['official_name', 'country_code']
                    )
                    session.execute(stmt)
                    session.flush()

                    # Get contractor - use first() to handle duplicates
                    contractor = session.execute(
                        select(Contractor).where(
                            Contractor.official_name == contractor_data['official_name'],
                            Contractor.country_code == contractor_data.get('country_code')
                        )
                    ).first()

                    if contractor:
                        contractor = contractor[0]  # Extract from tuple
                        # Link award to contractor
                        if contractor not in award.contractors:
                            award.contractors.append(contractor)

            count += 1

        except Exception as e:
            logger.error(f"Error saving award {award_data.document.doc_id}: {e}")
            raise

    session.flush()
    return count


def scrape_date(target_date: date, data_dir: Path = DATA_DIR):
    """Scrape TED awards for a specific date."""
    day_number = get_day_number(target_date)
    package_url = f"https://ted.europa.eu/packages/daily/{day_number:09d}"

    logger.info(f"Scraping TED awards for {target_date} (day {day_number:09d})")

    # Download and extract daily package
    files = download_and_extract(package_url, target_date, data_dir)
    if not files:
        logger.warning(f"No files found for {target_date}")
        return

    # Process all files and collect awards
    all_awards = []
    for file_path in files:
        parser_result = process_file(file_path)
        if parser_result:
            all_awards.extend(parser_result.awards)

    # Save all awards in a single transaction
    if all_awards:
        with get_session() as session:
            saved = save_awards(session, all_awards)
            logger.info(f"Processed {saved} award notices for {target_date}")
    else:
        logger.info(f"No award notices found for {target_date}")


def backfill_range(start_date: date, end_date: date, data_dir: Path = DATA_DIR):
    """Backfill TED awards for a date range."""
    logger.info(f"Backfilling TED awards from {start_date} to {end_date}")

    current_date = start_date
    while current_date <= end_date:
        scrape_date(current_date, data_dir)
        current_date += timedelta(days=1)

    logger.info("Backfill completed")