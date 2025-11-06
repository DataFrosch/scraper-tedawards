import logging
import requests
import tarfile
from pathlib import Path
from datetime import date, timedelta
from typing import List
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .config import config, get_session, engine
from .parsers import ParserFactory
from .models import Base, TEDDocument, ContractingBody, Contract, Award, Contractor
from .schema import TedAwardDataModel, TedParserResultModel

logger = logging.getLogger(__name__)

class TedScraper:
    """Main scraper for TED awards data."""

    def __init__(self):
        self.parser_factory = ParserFactory()
        self.data_dir = config.TED_DATA_DIR
        self.data_dir.mkdir(exist_ok=True)

        # Ensure database schema exists
        Base.metadata.create_all(engine)

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

        with get_session() as session:
            award_batch = []
            total_files = len(files)

            for i, file_path in enumerate(files):
                parser_result = self._process_file(file_path)
                if parser_result:
                    # Parser returns TedParserResultModel with list of TedAwardDataModel
                    award_batch.extend(parser_result.awards)

                # Process batch when it reaches size limit or at end
                if len(award_batch) >= batch_size or i == total_files - 1:
                    if award_batch:
                        saved = self._save_award_batch(session, award_batch)
                        processed += saved
                        logger.info(f"Saved batch of {saved} awards ({i+1}/{total_files} files processed)")
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

    def _process_file(self, file_path: Path) -> TedParserResultModel:
        """Process a single file (XML or ZIP) and return parser result."""
        try:
            # Get appropriate parser for this file
            parser = self.parser_factory.get_parser(file_path)
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

    def _save_award_batch(self, session, award_batch: List[TedAwardDataModel]) -> int:
        """Save batch of award data to database."""
        count = 0

        for award_data in award_batch:
            try:
                # Insert document with INSERT OR IGNORE
                insert_func = sqlite_insert if engine.dialect.name == 'sqlite' else pg_insert
                stmt = insert_func(TEDDocument).values(**award_data.document.dict())
                stmt = stmt.on_conflict_do_nothing(index_elements=['doc_id'])
                session.execute(stmt)
                session.flush()

                # Get document (either newly inserted or existing)
                doc = session.execute(
                    select(TEDDocument).where(TEDDocument.doc_id == award_data.document.doc_id)
                ).scalar_one()

                # Insert contracting body
                cb_data = award_data.contracting_body.dict()
                cb_data['ted_doc_id'] = doc.doc_id
                cb = ContractingBody(**cb_data)
                session.add(cb)
                session.flush()

                # Insert contract
                contract_data = award_data.contract.dict()
                contract_data['ted_doc_id'] = doc.doc_id
                contract_data['contracting_body_id'] = cb.id
                contract_data.pop('performance_nuts_code', None)
                contract = Contract(**contract_data)
                session.add(contract)
                session.flush()

                # Insert awards and contractors
                for award_item in award_data.awards:
                    award_dict = award_item.dict()
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