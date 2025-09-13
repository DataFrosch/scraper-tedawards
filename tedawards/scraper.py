import logging
import requests
import tarfile
from pathlib import Path
from datetime import date, timedelta
from typing import List
from .config import config
from .parser import TedXmlParser
from .database import DatabaseManager

logger = logging.getLogger(__name__)

class TedScraper:
    """Main scraper for TED awards data."""

    def __init__(self):
        self.parser = TedXmlParser()
        self.data_dir = config.TED_DATA_DIR
        self.data_dir.mkdir(exist_ok=True)

    def scrape_date(self, target_date: date):
        """Scrape TED awards for a specific date."""
        day_number = self._get_day_number(target_date)
        package_url = f"https://ted.europa.eu/packages/daily/{day_number:09d}"

        logger.info(f"Scraping TED awards for {target_date} (day {day_number:09d})")

        # Download and extract daily package
        xml_files = self._download_and_extract(package_url, target_date)
        if not xml_files:
            logger.warning(f"No XML files found for {target_date}")
            return

        # Process XML files
        processed = 0
        with DatabaseManager() as db:
            for xml_file in xml_files:
                if self._process_xml_file(xml_file, db):
                    processed += 1

        logger.info(f"Processed {processed} award notices for {target_date}")

    def backfill_range(self, start_date: date, end_date: date):
        """Backfill TED awards for a date range."""
        logger.info(f"Backfilling TED awards from {start_date} to {end_date}")

        current_date = start_date
        while current_date <= end_date:
            try:
                self.scrape_date(current_date)
            except Exception as e:
                logger.error(f"Error processing {current_date}: {e}")

            current_date += timedelta(days=1)

        logger.info("Backfill completed")

    def _get_day_number(self, target_date: date) -> int:
        """Calculate TED day number for a given date."""
        year = target_date.year
        start_of_year = date(year, 1, 1)
        days_since_start = (target_date - start_of_year).days + 1
        return year * 100000 + days_since_start

    def _download_and_extract(self, package_url: str, target_date: date) -> List[Path]:
        """Download and extract daily package, return list of XML files."""
        date_str = target_date.strftime('%Y-%m-%d')
        archive_path = self.data_dir / f"{date_str}.tar.gz"
        extract_dir = self.data_dir / date_str

        # Check if already downloaded and extracted
        if extract_dir.exists() and list(extract_dir.glob('**/*.xml')):
            logger.info(f"Using existing data for {date_str}")
            return list(extract_dir.glob('**/*.xml'))

        # Download package
        logger.info(f"Downloading package from {package_url}")
        try:
            response = requests.get(package_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to download package: {e}")
            return []

        # Save and extract
        archive_path.write_bytes(response.content)
        logger.info(f"Downloaded {len(response.content)} bytes")

        extract_dir.mkdir(exist_ok=True)
        try:
            with tarfile.open(archive_path, 'r:gz') as tar_file:
                tar_file.extractall(extract_dir)
        except tarfile.TarError as e:
            logger.error(f"Failed to extract package: {e}")
            return []

        # Clean up archive file
        archive_path.unlink()

        xml_files = list(extract_dir.glob('**/*.xml'))
        logger.info(f"Extracted {len(xml_files)} XML files")
        return xml_files

    def _process_xml_file(self, xml_file: Path, db: DatabaseManager) -> bool:
        """Process a single XML file and save to database."""
        try:
            # Check if already processed
            doc_id = xml_file.stem.replace('_', '-')
            if db.document_exists(doc_id):
                logger.debug(f"Skipping {xml_file.name} - already processed")
                return False

            # Parse XML
            data = self.parser.parse_xml_file(xml_file)
            if not data:
                return False

            # Save to database
            db.save_award_data(data)
            logger.debug(f"Processed {xml_file.name}")
            return True

        except Exception as e:
            logger.error(f"Error processing {xml_file}: {e}")
            return False