"""
TED Text format parser for pre-XML formats (2007 and earlier).
Handles the legacy text-based format with structured fields.
"""

import logging
import re
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from .base import BaseParser
from ..schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)
from ..utils import FileDetector, DateParsingUtils

logger = logging.getLogger(__name__)


class TedTextParser(BaseParser):
    """Parser for TED text format used in 2007 and earlier."""

    def can_parse(self, file_path: Path) -> bool:
        """Check if this file uses TED text format (ZIP containing text files)."""
        return FileDetector.is_ted_text_format(file_path)

    def get_format_name(self) -> str:
        """Return the format name for this parser."""
        return "TED Text Format"

    def parse_xml_file(self, file_path: Path) -> Optional[TedParserResultModel]:
        """Parse a TED text format ZIP file and extract award data."""
        try:
            # This method name is misleading as it's not XML, but kept for interface compatibility
            return self._parse_text_zip_with_original_language_priority(file_path)
        except Exception as e:
            logger.error(f"Error parsing {file_path.name}: {e}")
            return None

    def _parse_text_zip_with_original_language_priority(self, zip_path: Path) -> Optional[TedParserResultModel]:
        """Parse ZIP file, but only return records where current file language matches original language."""
        try:
            # Extract current file's language from filename (e.g., EN_20070103_001_UTF8_ORG.ZIP -> EN)
            current_lang = zip_path.name[:2]

            # Parse current file only
            with zipfile.ZipFile(zip_path, 'r') as zf:
                names = zf.namelist()
                if not names:
                    logger.warning(f"No files found in {zip_path}")
                    return None

                text_content = zf.read(names[0]).decode('utf-8', errors='ignore')
                records = self._parse_text_content(text_content)

                # Filter for award records where current language matches original language
                award_records = []
                for record in records:
                    if record.get('TD') == '7 - Contract award':
                        nd = record.get('ND', '')
                        original_lang = record.get('OL', '')

                        # Only process this record if current language matches original language
                        if current_lang == original_lang:
                            award_data = self._convert_to_standard_format(record)
                            if award_data:
                                logger.debug(f"Processing ND {nd} in original language {original_lang}")
                                award_records.append(award_data)
                        else:
                            logger.debug(f"Skipping ND {nd} - original language {original_lang}, current file {current_lang}")

                if award_records:
                    return TedParserResultModel(awards=award_records)

        except Exception as e:
            logger.error(f"Error parsing ZIP file {zip_path}: {e}")

        return None


    def _parse_text_zip(self, zip_path: Path) -> Optional[Dict]:
        """Parse the ZIP file containing TED text format data."""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Get the first (and usually only) file in the ZIP
                names = zf.namelist()
                if not names:
                    logger.warning(f"No files found in {zip_path}")
                    return None

                text_content = zf.read(names[0]).decode('utf-8', errors='ignore')

                # Parse the text content into records
                records = self._parse_text_content(text_content)

                # Filter for award records only (TD: 7)
                award_records = []
                for record in records:
                    if record.get('TD') == '7 - Contract award':
                        award_data = self._convert_to_standard_format(record)
                        if award_data:
                            award_records.append(award_data)

                if award_records:
                    return {'awards': award_records}

        except Exception as e:
            logger.error(f"Error parsing ZIP file {zip_path}: {e}")

        return None

    def _parse_text_content(self, content: str) -> List[Dict]:
        """Parse the text content into individual records."""
        records = []
        lines = content.split('\n')
        current_record = {}
        current_field = None

        for line in lines:
            line = line.strip()

            # Skip header lines
            if line.startswith('***') or not line:
                continue

            # Check for record separator (e.g., "1.0/000001")
            if re.match(r'^\d+\.\d+/\d{6}$', line):
                # Save previous record if it exists
                if current_record:
                    records.append(current_record)

                # Start new record
                current_record = {'RECORD_ID': line}
                current_field = None
                continue

            # Check for field lines (e.g., "TI: Title", "PD: 20070103")
            field_match = re.match(r'^([A-Z]{2}):\s*(.*)$', line)
            if field_match:
                field_code, field_value = field_match.groups()
                current_record[field_code] = field_value.strip()
                current_field = field_code
                continue

            # Check for continuation lines (indented or starting with spaces)
            if current_field and (line.startswith('    ') or line.startswith('\t')):
                # Append to current field
                if current_field in current_record:
                    current_record[current_field] += '\n' + line.strip()
                continue

        # Add the last record
        if current_record:
            records.append(current_record)

        return records

    def _convert_to_standard_format(self, record: Dict) -> Optional[TedAwardDataModel]:
        """Convert TED text format record to standardized format."""
        try:
            # Extract universal document identifier (ND field is consistent across languages)
            nd = record.get('ND', '')  # Notice Document number (e.g., "1-2007")
            pd = record.get('PD', '')  # Publication date

            if nd and pd:
                # Use ND as the universal identifier across all languages
                document_id = f"nd-{nd}-{pd}".replace('/', '-')
                logger.debug(f"Using universal ND identifier: {document_id}")
            else:
                # Fallback to language-specific TED ID if ND not available
                record_id = record.get('RECORD_ID', '')
                if record_id:
                    document_id = record_id.split('/')[-1]  # Get the number part
                    logger.debug(f"Using language-specific TED ID: {document_id}")
                else:
                    # Last resort fallback
                    rn = record.get('RN', '')
                    if rn:
                        document_id = f"rn-{rn}"
                    else:
                        oj = record.get('OJ', '')
                        document_id = f"pd-{pd}-oj-{oj}".replace('/', '-')
                    logger.debug(f"Using fallback identifier: {document_id}")

            # Parse publication date
            pub_date_str = record.get('PD', '')
            pub_date = None
            if pub_date_str and len(pub_date_str) == 8:
                try:
                    pub_date = datetime.strptime(pub_date_str, '%Y%m%d').date()
                except ValueError:
                    logger.warning(f"Invalid date format: {pub_date_str}")

            # Extract reference number and other document details
            ref_number = record.get('RN', '')
            oj_ref = record.get('OJ', '')
            dispatch_date = record.get('DR', '')

            # Parse dates
            dispatch_date_obj = None
            if dispatch_date and len(dispatch_date) == 8:
                try:
                    dispatch_date_obj = datetime.strptime(dispatch_date, '%Y%m%d').date()
                except ValueError:
                    pass

            # Parse award date
            award_date = None
            award_date_str = self._parse_award_date(record.get('TX', ''))
            if award_date_str:
                try:
                    award_date = datetime.fromisoformat(award_date_str).date()
                except ValueError:
                    pass

            # Build award data using Pydantic models
            full_doc_id = f"{document_id}-2007"

            document = DocumentModel(
                doc_id=full_doc_id,
                edition='1',
                version='1',
                reception_id=ref_number,
                deletion_date=None,
                form_language=record.get('OL', ''),
                official_journal_ref=oj_ref,
                publication_date=pub_date,
                dispatch_date=dispatch_date_obj,
                original_language=record.get('OL', ''),
                source_country=record.get('CY', '')
            )

            contracting_body = ContractingBodyModel(
                official_name=record.get('AU', ''),
                address='',
                town=record.get('TW', ''),
                postal_code='',
                country_code=record.get('CY', ''),
                nuts_code='',
                contact_point='',
                phone='',
                email='',
                fax='',
                url_general='',
                url_buyer='',
                authority_type_code='',
                main_activity_code=''
            )

            contract = ContractModel(
                title=record.get('TI', ''),
                reference_number=ref_number,
                short_description=record.get('TX', ''),
                main_cpv_code=self._parse_main_cpv_code(record.get('PC', '')),
                contract_nature_code=self._parse_contract_nature_code(record.get('NC', '')),
                total_value=self._parse_contract_value(record.get('TX', '')),
                total_value_currency='EUR',
                procedure_type_code='',
                award_criteria_code='',
                performance_nuts_code=''
            )

            contractors = self._parse_contractors_as_models(record.get('TX', ''))

            award = AwardModel(
                award_title=record.get('TI', ''),
                conclusion_date=award_date,
                contract_number=ref_number,
                tenders_received=self._parse_offers_received(record.get('TX', '')),
                tenders_received_sme=None,
                tenders_received_other_eu=None,
                tenders_received_non_eu=None,
                tenders_received_electronic=None,
                awarded_value=self._parse_contract_value(record.get('TX', '')),
                awarded_value_currency='EUR',
                subcontracted_value=None,
                subcontracted_value_currency=None,
                subcontracting_description='',
                contractors=contractors
            )

            return TedAwardDataModel(
                document=document,
                contracting_body=contracting_body,
                contract=contract,
                awards=[award]
            )

        except Exception as e:
            logger.error(f"Error converting record to standard format: {e}")
            return None

    def _parse_main_cpv_code(self, pc_value: str) -> str:
        """Parse the main CPV code from PC field."""
        if not pc_value:
            return ''

        # Extract the first 8-digit code as the main one
        codes = re.findall(r'\b\d{8}\b', pc_value)
        return codes[0] if codes else ''

    def _parse_contract_nature_code(self, nc_value: str) -> str:
        """Parse contract nature code from NC field."""
        if '1 - Works' in nc_value or '1-Works' in nc_value:
            return '1'
        elif '2 - Supply' in nc_value or '2-Supply' in nc_value:
            return '2'
        elif '4 - Service' in nc_value or '4-Service' in nc_value:
            return '4'
        return ''

    def _parse_contract_type(self, nc_value: str) -> str:
        """Parse contract type from NC field."""
        if '1 - Works' in nc_value:
            return 'works'
        elif '2 - Supply' in nc_value:
            return 'supplies'
        elif '4 - Service' in nc_value:
            return 'services'
        return 'other'

    def _parse_cpv_codes(self, pc_value: str) -> List[str]:
        """Parse CPV codes from PC field."""
        if not pc_value:
            return []

        # Extract 8-digit codes from the text
        codes = re.findall(r'\b\d{8}\b', pc_value)
        return codes

    def _parse_contractors(self, tx_value: str) -> List[Dict]:
        """Extract contractor information from TX field."""
        contractors = []

        if not tx_value:
            return contractors

        # Look for "Name and address of successful tenderer" section
        success_match = re.search(r'Name and address of successful tenderer[:\s]*(.+?)(?=\n\s*\n|\Z)', tx_value, re.DOTALL | re.IGNORECASE)

        if success_match:
            contractor_text = success_match.group(1).strip()

            # Split by "Lot" if multiple lots
            lot_parts = re.split(r'Lot\s+\d+\s*[—\-]', contractor_text)

            for i, part in enumerate(lot_parts):
                if not part.strip():
                    continue

                # Extract company name (usually the first line or before the first comma)
                lines = part.strip().split('\n')
                if lines:
                    first_line = lines[0].strip()
                    # Company name is usually before the first comma or the entire first line
                    company_name = first_line.split(',')[0].strip()

                    contractors.append({
                        'official_name': company_name,
                        'address': part.strip(),
                        'town': '',
                        'postal_code': '',
                        'country_code': '',  # Will be extracted if pattern is found
                        'nuts_code': '',
                        'phone': '',
                        'email': '',
                        'fax': '',
                        'url': '',
                        'is_sme': False
                    })

        return contractors

    def _parse_contractors_as_models(self, tx_value: str) -> List[ContractorModel]:
        """Extract contractor information from TX field and return as ContractorModel objects."""
        contractors = []

        if not tx_value:
            return []

        # Look for "Name and address of successful tenderer" section
        success_match = re.search(r'Name and address of successful tenderer[:\s]*(.+?)(?=\n\s*\n|\Z)', tx_value, re.DOTALL | re.IGNORECASE)

        if success_match:
            contractor_text = success_match.group(1).strip()

            # Split by "Lot" if multiple lots
            lot_parts = re.split(r'Lot\s+\d+\s*[—\-]', contractor_text)

            for i, part in enumerate(lot_parts):
                if not part.strip():
                    continue

                # Extract company name (usually the first line or before the first comma)
                lines = part.strip().split('\n')
                if lines:
                    first_line = lines[0].strip()
                    # Company name is usually before the first comma or the entire first line
                    company_name = first_line.split(',')[0].strip()

                    contractors.append(ContractorModel(
                        official_name=company_name,
                        address=part.strip(),
                        town='',
                        postal_code='',
                        country_code='',  # Will be extracted if pattern is found
                        nuts_code='',
                        phone='',
                        email='',
                        fax='',
                        url='',
                        is_sme=False
                    ))

        return contractors

    def _parse_contract_value(self, tx_value: str) -> Optional[float]:
        """Extract contract value from TX field."""
        if not tx_value:
            return None

        # Look for "Contract value" section
        value_patterns = [
            r'Contract value[:\s]*.*?EUR\s*([\d,.\s]+)',
            r'Lot\s+\d+\s*[—\-].*?EUR\s*([\d,.\s]+)',
            r'EUR\s*([\d,.\s]+)'
        ]

        for pattern in value_patterns:
            match = re.search(pattern, tx_value, re.IGNORECASE)
            if match:
                value_str = match.group(1).replace(',', '').replace(' ', '')
                try:
                    return float(value_str)
                except ValueError:
                    continue

        return None

    def _parse_offers_received(self, tx_value: str) -> Optional[int]:
        """Extract number of offers received from TX field."""
        if not tx_value:
            return None

        # Look for "Number of tenders received" section
        offers_match = re.search(r'Number of tenders received[:\s]*(\d+)', tx_value, re.IGNORECASE)
        if offers_match:
            try:
                return int(offers_match.group(1))
            except ValueError:
                pass

        return None

    def _parse_award_date(self, tx_value: str) -> Optional[str]:
        """Extract award date from TX field."""
        if not tx_value:
            return None

        # Look for "Date of award" section
        date_match = re.search(r'Date of award[:\s]*(\d{1,2})\.(\d{1,2})\.(\d{4})', tx_value, re.IGNORECASE)
        if date_match:
            day, month, year = date_match.groups()
            try:
                award_date = datetime(int(year), int(month), int(day)).date()
                return award_date.isoformat()
            except ValueError:
                pass

        return None