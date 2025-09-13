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

logger = logging.getLogger(__name__)


class TedTextParser(BaseParser):
    """Parser for TED text format used in 2007 and earlier."""

    def can_parse(self, file_path: Path) -> bool:
        """Check if this file uses TED text format (ZIP containing text files)."""
        try:
            if not file_path.name.endswith('.ZIP'):
                return False

            # Check if it's a language-specific ZIP file pattern
            # Format: XX_YYYYMMDD_NNN_UTF8_ORG.ZIP (e.g., EN_20070103_001_UTF8_ORG.ZIP)
            pattern = r'^[A-Z]{2}_\d{8}_\d{3}_(UTF8|ISO)_ORG\.ZIP$'
            if not re.match(pattern, file_path.name):
                return False

            return True

        except Exception as e:
            logger.debug(f"Error checking if {file_path.name} is TED text format: {e}")
            return False

    def get_format_name(self) -> str:
        """Return the format name for this parser."""
        return "TED Text Format"

    def parse_xml_file(self, file_path: Path) -> Optional[Dict]:
        """Parse a TED text format ZIP file and extract award data."""
        try:
            # This method name is misleading as it's not XML, but kept for interface compatibility
            return self._parse_text_zip(file_path)
        except Exception as e:
            logger.error(f"Error parsing {file_path.name}: {e}")
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

    def _convert_to_standard_format(self, record: Dict) -> Optional[Dict]:
        """Convert TED text format record to standardized format."""
        try:
            # Extract basic document info
            document_id = record.get('RECORD_ID', '').split('/')[-1]  # Get the number part

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
            source_date = record.get('DS', '')

            # Parse dates
            dispatch_date_obj = None
            if dispatch_date and len(dispatch_date) == 8:
                try:
                    dispatch_date_obj = datetime.strptime(dispatch_date, '%Y%m%d').date()
                except ValueError:
                    pass

            # Build award data structure matching the expected database schema
            award_data = {
                'document': {
                    'doc_id': f"{document_id}-2007",
                    'edition': '1',
                    'version': '1',
                    'reception_id': ref_number,
                    'deletion_date': None,
                    'form_language': record.get('OL', 'EN'),
                    'official_journal_ref': oj_ref,
                    'publication_date': pub_date.isoformat() if pub_date else None,
                    'dispatch_date': dispatch_date_obj.isoformat() if dispatch_date_obj else None,
                    'original_language': record.get('OL', 'EN'),
                    'source_country': record.get('CY', '')
                },
                'contracting_body': {
                    'name': record.get('AU', ''),
                    'town': record.get('TW', ''),
                    'country_code': record.get('CY', ''),
                    'postal_code': '',
                    'address': '',
                    'phone': '',
                    'email': '',
                    'fax': '',
                    'nuts_code': '',
                    'main_activity': '',
                    'type': ''
                },
                'contract': {
                    'title': record.get('TI', ''),
                    'type': self._parse_contract_type(record.get('NC', '')),
                    'cpv_codes': self._parse_cpv_codes(record.get('PC', '')),
                    'description': record.get('TX', ''),
                    'award_criteria': '',
                    'procurement_method': '',
                    'framework_agreement': False,
                    'lots_division': False,
                    'electronic_auction': False,
                    'variants_accepted': False,
                    'options': False,
                    'eu_funds': False,
                    'additional_info': ''
                },
                'awards': [{
                    'lot_number': 1,
                    'title': record.get('TI', ''),
                    'description': record.get('TX', ''),
                    'contractors': self._parse_contractors(record.get('TX', '')),
                    'value': self._parse_contract_value(record.get('TX', '')),
                    'currency': 'EUR',
                    'award_date': self._parse_award_date(record.get('TX', '')),
                    'offers_received': self._parse_offers_received(record.get('TX', '')),
                    'contract_conclusion': True,
                    'non_award_reason': '',
                    'subcontracting': False
                }]
            }

            return award_data

        except Exception as e:
            logger.error(f"Error converting record to standard format: {e}")
            return None

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
                        'name': company_name,
                        'address': part.strip(),
                        'country_code': ''  # Will be extracted if pattern is found
                    })

        return contractors if contractors else [{'name': 'Not specified', 'address': '', 'country_code': ''}]

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