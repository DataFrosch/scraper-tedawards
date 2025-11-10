import logging
import os
import requests
import tarfile
from pathlib import Path
from typing import List, Optional
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
DB_PATH = Path(os.getenv('DB_PATH', './tedawards.db'))
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


def get_package_number(year: int, issue: int) -> int:
    """Calculate TED package number from year and OJ issue number."""
    return year * 100000 + issue


def get_last_downloaded_issue(year: int, data_dir: Path = DATA_DIR) -> Optional[int]:
    """Get the last downloaded issue number for a given year by checking data directory.

    Args:
        year: Year to check
        data_dir: Directory where packages are stored

    Returns:
        Last downloaded issue number for the year, or None if no data exists
    """
    # Find all package directories for this year (format: yyyynnnnn)
    year_prefix = str(year)
    matching_dirs = []

    for item in data_dir.iterdir():
        if item.is_dir() and item.name.startswith(year_prefix) and len(item.name) == 9:
            try:
                package_num = int(item.name)
                pkg_year = package_num // 100000
                pkg_issue = package_num % 100000
                if pkg_year == year:
                    matching_dirs.append(pkg_issue)
            except ValueError:
                continue

    if not matching_dirs:
        return None

    return max(matching_dirs)


def download_and_extract(package_number: int, data_dir: Path = DATA_DIR) -> Optional[List[Path]]:
    """Download and extract daily package, return list of XML and ZIP files.

    Args:
        package_number: TED package number (yyyynnnnn format)
        data_dir: Directory to store downloaded data

    Returns:
        List of file paths from the package, or None if package doesn't exist (404)
    """
    package_url = f"https://ted.europa.eu/packages/daily/{package_number:09d}"
    package_str = f"{package_number:09d}"
    archive_path = data_dir / f"{package_str}.tar.gz"
    extract_dir = data_dir / package_str

    # Check if already downloaded and extracted
    if extract_dir.exists():
        existing_files = list(extract_dir.glob('**/*'))
        existing_files = [f for f in existing_files if f.is_file()]
        if existing_files:
            logger.debug(f"Using existing data for package {package_str}")
            return existing_files

    # Download package
    logger.debug(f"Downloading package {package_str} from {package_url}")
    try:
        response = requests.get(package_url, timeout=30)
        response.raise_for_status()
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            logger.debug(f"Package not available (404): {package_str}")
            return None
        logger.error(f"Failed to download package {package_str}: {e}")
        raise
    except requests.RequestException as e:
        logger.error(f"Failed to download package {package_str}: {e}")
        raise

    # Save and extract
    archive_path.write_bytes(response.content)
    logger.debug(f"Downloaded {len(response.content)} bytes for package {package_str}")

    extract_dir.mkdir(exist_ok=True)
    try:
        with tarfile.open(archive_path, 'r:gz') as tar_file:
            tar_file.extractall(extract_dir, filter='data')
    except tarfile.TarError as e:
        logger.error(f"Failed to extract package {package_str}: {e}")
        raise

    # Clean up archive file
    archive_path.unlink()

    # Return all files - let parsers decide what they can handle
    all_files = list(extract_dir.glob('**/*'))
    all_files = [f for f in all_files if f.is_file()]
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
    """Save award data to database with proper deduplication using RETURNING."""
    count = 0
    insert_func = sqlite_insert if engine.dialect.name == 'sqlite' else pg_insert

    for award_data in awards:
        try:
            # Insert/update document with RETURNING to get doc_id
            doc_values = award_data.document.model_dump()
            stmt = insert_func(TEDDocument).values(**doc_values)
            stmt = stmt.on_conflict_do_update(
                index_elements=['doc_id'],
                set_={'doc_id': stmt.excluded.doc_id}  # No-op update to trigger RETURNING
            ).returning(TEDDocument.doc_id)
            doc_id = session.execute(stmt).scalar_one()

            # Insert/update contracting body with RETURNING to get id
            cb_data = award_data.contracting_body.model_dump()
            cb_hash = award_data.contracting_body.entity_hash
            cb_data['entity_hash'] = cb_hash

            stmt = insert_func(ContractingBody).values(**cb_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['entity_hash'],
                set_={'entity_hash': stmt.excluded.entity_hash}  # No-op update
            ).returning(ContractingBody.id)
            cb_id = session.execute(stmt).scalar_one()

            # Insert document-contracting body relationship with INSERT OR IGNORE
            from .models import document_contracting_bodies
            stmt = insert_func(document_contracting_bodies).values(
                ted_doc_id=doc_id,
                contracting_body_id=cb_id
            )
            stmt = stmt.on_conflict_do_nothing()
            session.execute(stmt)

            # Insert/update contract with RETURNING to get id
            contract_data = award_data.contract.model_dump()
            contract_data['ted_doc_id'] = doc_id
            contract_data['contracting_body_id'] = cb_id
            contract_data.pop('performance_nuts_code', None)

            stmt = insert_func(Contract).values(**contract_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['ted_doc_id', 'title'],
                set_={'ted_doc_id': stmt.excluded.ted_doc_id}  # No-op update
            ).returning(Contract.id)
            contract_id = session.execute(stmt).scalar_one()

            # Insert awards and contractors
            for award_item in award_data.awards:
                award_dict = award_item.model_dump()
                contractors_data = award_dict.pop('contractors', [])
                award_dict['contract_id'] = contract_id

                # Insert/update award with RETURNING to get id
                stmt = insert_func(Award).values(**award_dict)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['contract_id', 'award_title', 'conclusion_date'],
                    set_={'contract_id': stmt.excluded.contract_id}  # No-op update
                ).returning(Award.id)
                award_id = session.execute(stmt).scalar_one()

                # Insert contractors and link to awards
                for contractor_item in award_item.contractors:
                    contractor_data = contractor_item.model_dump()
                    contractor_hash = contractor_item.entity_hash
                    contractor_data['entity_hash'] = contractor_hash

                    stmt = insert_func(Contractor).values(**contractor_data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['entity_hash'],
                        set_={'entity_hash': stmt.excluded.entity_hash}  # No-op update
                    ).returning(Contractor.id)
                    contractor_id = session.execute(stmt).scalar_one()

                    # Insert award-contractor relationship with INSERT OR IGNORE
                    from .models import award_contractors
                    stmt = insert_func(award_contractors).values(
                        award_id=award_id,
                        contractor_id=contractor_id
                    )
                    stmt = stmt.on_conflict_do_nothing()
                    session.execute(stmt)

            count += 1

        except Exception as e:
            logger.error(f"Error saving award {award_data.document.doc_id}: {e}")
            raise

    session.flush()
    return count


def scrape_package(package_number: int, data_dir: Path = DATA_DIR) -> int:
    """Scrape TED awards for a specific package number. Returns number of awards processed.

    Args:
        package_number: TED package number to scrape
        data_dir: Directory for storing downloaded packages

    Returns:
        Number of awards processed
    """
    # Download and extract daily package
    files = download_and_extract(package_number, data_dir)
    if files is None:
        return 0

    # Process all files and collect awards
    # Filter for English-only files to avoid processing all language variants
    english_files = [
        f for f in files
        if (
            # TED META XML: en_*_meta_org.zip or EN_*_META_ORG.ZIP
            f.name.lower().startswith('en_') and '_meta_org.' in f.name.lower()
        ) or (
            # TED INTERNAL_OJS: *.en files
            f.suffix.lower() == '.en'
        ) or (
            # TED 2.0 and eForms: all languages in one file (*.xml)
            f.suffix.lower() == '.xml'
        )
    ]

    all_awards = []
    for file_path in english_files:
        parser_result = process_file(file_path)
        if parser_result:
            all_awards.extend(parser_result.awards)

    # Save all awards in a single transaction
    if all_awards:
        with get_session() as session:
            saved = save_awards(session, all_awards)
            logger.info(f"Package {package_number:09d}: Processed {saved} award notices")
            return saved
    else:
        logger.debug(f"Package {package_number:09d}: No award notices found")
        return 0


def scrape_year(year: int, start_issue: Optional[int] = None, max_issue: int = 300, data_dir: Path = DATA_DIR, force_reimport: bool = False):
    """Scrape TED awards for all available packages in a year.

    Args:
        year: The year to scrape
        start_issue: Starting OJ issue number (default: auto-resume from last downloaded issue + 1, or 1 if none)
        max_issue: Maximum issue number to try (default: 300, sufficient for most years)
        data_dir: Directory for storing downloaded packages
        force_reimport: If True, reimport data from all already-downloaded archives (starting from issue 1)
    """
    Base.metadata.create_all(engine)

    # Auto-resume from last downloaded issue if start_issue not specified (unless force_reimport)
    if start_issue is None:
        if force_reimport:
            start_issue = 1
            logger.info(f"Force reimport: processing all downloaded data from issue 1")
        else:
            last_issue = get_last_downloaded_issue(year, data_dir)
            if last_issue is not None:
                start_issue = last_issue + 1
                logger.info(f"Resuming from issue {start_issue} (last downloaded: {last_issue})")
            else:
                start_issue = 1
                logger.info(f"No existing data found for year {year}, starting from issue 1")

    logger.info(f"Scraping TED awards for year {year} (starting from issue {start_issue}, stopping after 10 consecutive 404s)")

    total_processed = 0
    consecutive_404s = 0
    max_consecutive_404s = 10  # Stop after 10 consecutive 404s

    for issue in range(start_issue, max_issue + 1):
        package_number = get_package_number(year, issue)

        # Download and extract package
        files = download_and_extract(package_number, data_dir)

        if files is None:
            # Package doesn't exist (404)
            consecutive_404s += 1
            if consecutive_404s >= max_consecutive_404s:
                logger.info(f"Stopping after {max_consecutive_404s} consecutive 404s at issue {issue}")
                break
            continue

        # Reset 404 counter on success
        consecutive_404s = 0

        # Process all files and collect awards
        english_files = [
            f for f in files
            if (
                # TED META XML: en_*_meta_org.zip or EN_*_META_ORG.ZIP
                f.name.lower().startswith('en_') and '_meta_org.' in f.name.lower()
            ) or (
                # TED INTERNAL_OJS: *.en files
                f.suffix.lower() == '.en'
            ) or (
                # TED 2.0 and eForms: all languages in one file (*.xml)
                f.suffix.lower() == '.xml'
            )
        ]

        all_awards = []
        for file_path in english_files:
            parser_result = process_file(file_path)
            if parser_result:
                all_awards.extend(parser_result.awards)

        # Save all awards in a single transaction
        if all_awards:
            with get_session() as session:
                saved = save_awards(session, all_awards)
                total_processed += saved
                logger.info(f"Package {package_number:09d}: Processed {saved} award notices")

    logger.info(f"Year {year} completed: Processed {total_processed} total award notices")


def scrape_year_range(start_year: int, end_year: int, data_dir: Path = DATA_DIR, force_reimport: bool = False):
    """Scrape TED awards for a range of years.

    Args:
        start_year: First year to scrape
        end_year: Last year to scrape (inclusive)
        data_dir: Directory for storing downloaded packages
        force_reimport: If True, reprocess already-downloaded archives
    """
    Base.metadata.create_all(engine)

    logger.info(f"Scraping TED awards from {start_year} to {end_year}")

    for year in range(start_year, end_year + 1):
        scrape_year(year, data_dir=data_dir, force_reimport=force_reimport)

    logger.info("Scraping completed")