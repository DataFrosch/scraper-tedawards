"""TED Europa portal — download and import EU-wide procurement data."""

import logging
import os
import requests
import tarfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional

from ...db import (
    engine,
    get_session,
    get_imported_package_numbers,
    record_package,
    save_document_core,
)
from ...models import Base
from ...schema import AwardDataModel
from ...parsers import ted_v2, eforms_ubl

logger = logging.getLogger(__name__)

# Data directory setup
DATA_DIR = Path(os.getenv("TED_DATA_DIR", "./data"))
DATA_DIR.mkdir(exist_ok=True)


def try_parse_award(file_path: Path) -> Optional[List[AwardDataModel]]:
    """Parse file if it's an award notice, return None otherwise.

    Reads first 3KB to detect format, then delegates to appropriate parser.
    """
    with open(file_path, "rb") as f:
        header = f.read(3000).decode("utf-8", errors="ignore")

    # eForms: root element tells us document type directly
    if "<ContractAwardNotice" in header:
        return eforms_ubl.parse_xml_file(file_path)

    # TED 2.0: check root + document type code 7 (Contract award)
    if "<TED_EXPORT" in header and 'CODE="7"' in header:
        return ted_v2.parse_xml_file(file_path)

    return None


def get_package_number(year: int, issue: int) -> int:
    """Calculate TED package number from year and OJ issue number."""
    return year * 100000 + issue


def download_package(package_number: int, data_dir: Path = DATA_DIR) -> bool:
    """Download and extract a single daily package.

    Args:
        package_number: TED package number (yyyynnnnn format)
        data_dir: Directory to store downloaded data

    Returns:
        True if downloaded successfully, False if package doesn't exist (404)
    """
    package_url = f"https://ted.europa.eu/packages/daily/{package_number:09d}"
    package_str = f"{package_number:09d}"
    archive_path = data_dir / f"{package_str}.tar.gz"
    extract_dir = data_dir / package_str

    # Skip if already downloaded and extracted
    if extract_dir.exists():
        existing_files = [f for f in extract_dir.glob("**/*") if f.is_file()]
        if existing_files:
            logger.info(f"Package {package_str}: already downloaded")
            return True

    # Download package
    logger.info(f"Package {package_str}: downloading")
    try:
        response = requests.get(package_url, timeout=30)
        response.raise_for_status()
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            logger.info(f"Package {package_str}: not found")
            return False
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
        with tarfile.open(archive_path, "r:gz") as tar_file:
            tar_file.extractall(extract_dir, filter="data")
    except tarfile.TarError as e:
        logger.error(f"Failed to extract package {package_str}: {e}")
        raise

    # Clean up archive file
    archive_path.unlink()

    return True


def get_package_files(
    package_number: int, data_dir: Path = DATA_DIR
) -> Optional[List[Path]]:
    """Get list of files from an already-downloaded package.

    Args:
        package_number: TED package number (yyyynnnnn format)
        data_dir: Directory where packages are stored

    Returns:
        List of file paths, or None if package not downloaded
    """
    package_str = f"{package_number:09d}"
    extract_dir = data_dir / package_str

    if not extract_dir.exists():
        return None

    files = [f for f in extract_dir.glob("**/*") if f.is_file()]
    return files if files else None


def download_year(year: int, max_issue: int = 300, data_dir: Path = DATA_DIR):
    """Download TED packages for a year.

    Args:
        year: The year to download
        max_issue: Maximum issue number to try (default: 300)
        data_dir: Directory for storing downloaded packages
    """
    logger.info(
        f"Downloading TED packages for year {year} (issues 1-{max_issue}, stopping after 10 consecutive 404s)"
    )

    total_downloaded = 0
    consecutive_404s = 0
    max_consecutive_404s = 10

    imported = get_imported_package_numbers(year)

    for issue in range(1, max_issue + 1):
        package_number = get_package_number(year, issue)

        # Already imported (per the DB) — skip the HTTP fetch; it's known-good.
        # This keeps download resumable even after ./data was deleted.
        if package_number in imported:
            consecutive_404s = 0
            continue

        success = download_package(package_number, data_dir)

        if not success:
            consecutive_404s += 1
            if consecutive_404s >= max_consecutive_404s:
                logger.info(
                    f"Stopping after {max_consecutive_404s} consecutive 404s at issue {issue}"
                )
                break
            continue

        consecutive_404s = 0
        total_downloaded += 1

    logger.info(f"Year {year}: Downloaded {total_downloaded} packages")


def get_downloaded_packages(year: int, data_dir: Path = DATA_DIR) -> List[int]:
    """Get list of downloaded package numbers for a year.

    Args:
        year: Year to check
        data_dir: Directory where packages are stored

    Returns:
        Sorted list of package numbers that have been downloaded
    """
    year_prefix = str(year)
    packages = []

    for item in data_dir.iterdir():
        if item.is_dir() and item.name.startswith(year_prefix) and len(item.name) == 9:
            try:
                package_num = int(item.name)
                pkg_year = package_num // 100000
                if pkg_year == year:
                    packages.append(package_num)
            except ValueError:
                continue

    return sorted(packages)


def import_package(
    package_number: int,
    data_dir: Path = DATA_DIR,
    executor: Optional[ThreadPoolExecutor] = None,
) -> int:
    """Import awards from a single downloaded package.

    All documents are saved in a single transaction per package.
    Parsing is done in the provided thread pool executor while
    database saving runs sequentially on the calling thread.

    Args:
        package_number: TED package number (yyyynnnnn format)
        data_dir: Directory where packages are stored
        executor: Thread pool for parallel XML parsing (created if None)

    Returns:
        Number of award notices imported
    """
    files = get_package_files(package_number, data_dir)
    if files is None:
        logger.warning(f"Package {package_number:09d} not found in {data_dir}")
        return 0

    xml_files = [f for f in files if f.suffix.lower() == ".xml"]

    own_executor = executor is None
    if own_executor:
        executor = ThreadPoolExecutor()

    try:
        parsed = executor.map(try_parse_award, xml_files)

        count = 0
        with get_session() as session:
            for awards in parsed:
                if not awards:
                    continue
                for award_data in awards:
                    if save_document_core(session, award_data):
                        count += 1
            record_package(session, package_number, count)
    finally:
        if own_executor:
            executor.shutdown(wait=False)

    if count:
        logger.info(f"Package {package_number:09d}: Imported {count} award notices")
    return count


def import_year(year: int, data_dir: Path = DATA_DIR):
    """Import awards from all downloaded packages for a year.

    Args:
        year: The year to import
        data_dir: Directory where packages are stored
    """
    Base.metadata.create_all(engine)

    packages = get_downloaded_packages(year, data_dir)
    if not packages:
        logger.warning(f"No downloaded packages found for year {year}")
        return

    logger.info(f"Importing {len(packages)} packages for year {year}")

    total_imported = 0
    with ThreadPoolExecutor() as executor:
        for package_number in packages:
            total_imported += import_package(package_number, data_dir, executor)

    logger.info(f"Year {year}: Imported {total_imported} total award notices")


class TEDPortal:
    name = "ted"

    def download(self, start_year: int, end_year: int) -> None:
        for y in range(start_year, end_year + 1):
            download_year(y)

    def import_data(self, start_year: int, end_year: int) -> None:
        for y in range(start_year, end_year + 1):
            import_year(y)
