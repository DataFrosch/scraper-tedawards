import logging
import requests
import tarfile
from pathlib import Path
from datetime import date, timedelta
from typing import List
from .config import config
from .parsers import ParserFactory
from .database import DatabaseManager

logger = logging.getLogger(__name__)

class TedScraper:
    """Main scraper for TED awards data."""

    def __init__(self):
        self.parser_factory = ParserFactory()
        self.data_dir = config.TED_DATA_DIR
        self.data_dir.mkdir(exist_ok=True)

    def scrape_date(self, target_date: date):
        """Scrape TED awards for a specific date."""
        day_number = self._get_day_number(target_date)
        package_url = f"https://ted.europa.eu/packages/daily/{day_number:09d}"

        logger.info(f"Scraping TED awards for {target_date} (day {day_number:09d})")

        # Download and extract daily package
        files = self._download_and_extract(package_url, target_date)
        if not files:
            logger.warning(f"No files found for {target_date}")
            return

        # Process files (XML or ZIP) with batch processing
        processed = 0
        batch_size = 50  # Process files in batches to improve performance
        with DatabaseManager() as db:
            award_batch = []
            total_files = len(files)

            for i, file_path in enumerate(files):
                awards = self._process_file(file_path, db)
                if awards:
                    award_batch.extend(awards)
                    processed += len(awards)

                # Process batch when it reaches size limit or at end
                if len(award_batch) >= batch_size or i == total_files - 1:
                    if award_batch:
                        db.save_award_data_batch(award_batch)
                        logger.info(f"Saved batch of {len(award_batch)} awards ({i+1}/{total_files} files processed)")
                        award_batch.clear()

        logger.info(f"Processed {processed} award notices for {target_date}")

    def backfill_range(self, start_date: date, end_date: date):
        """Backfill TED awards for a date range."""
        logger.info(f"Backfilling TED awards from {start_date} to {end_date}")

        current_date = start_date
        while current_date <= end_date:
            self.scrape_date(current_date)

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
        logger.info(f"Downloaded {len(response.content)} bytes")

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
        logger.info(f"Extracted {len(xml_files)} XML files and {len(zip_files)} ZIP files")
        return all_files

    def _process_file(self, file_path: Path, db: DatabaseManager) -> List:
        """Process a single file (XML or ZIP) and return award data."""
        try:
            # For ZIP files, use a different document ID extraction logic - case insensitive
            if file_path.suffix.upper() == '.ZIP':
                # Extract date from ZIP filename (e.g., EN_20070103_001_UTF8_ORG.ZIP)
                import re
                match = re.search(r'(\d{8})', file_path.name)
                if match:
                    date_str = match.group(1)
                    doc_id = f"text-{date_str}-{file_path.name[:2]}"  # e.g., text-20070103-EN
                else:
                    doc_id = file_path.stem
            else:
                doc_id = file_path.stem.replace('_', '-')

            # Check if any document from this file already exists (simple optimization)
            # For text files, we'll need to check individual documents later
            if file_path.suffix.upper() != '.ZIP' and db.document_exists(doc_id):
                logger.debug(f"Skipping {file_path.name} - already processed")
                return []

            # Get appropriate parser for this file
            parser = self.parser_factory.get_parser(file_path)
            if not parser:
                logger.debug(f"No parser available for {file_path.name}")
                return []

            # Parse file
            data = parser.parse_xml_file(file_path)  # Method name is misleading but kept for compatibility
            if not data:
                logger.debug(f"Failed to parse {file_path.name} with {parser.get_format_name()}")
                return []

            logger.debug(f"Parsed {file_path.name} using {parser.get_format_name()}, found {len(data.awards)} awards")

            # Filter out already processed documents and return new ones
            new_awards = []
            for award_data in data.awards:
                if not db.document_exists(award_data.document.doc_id):
                    new_awards.append(award_data)
                else:
                    logger.debug(f"Skipping document {award_data.document.doc_id} - already exists")

            if new_awards:
                logger.debug(f"Processed {file_path.name} - {len(new_awards)} new awards")

            return new_awards

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            raise