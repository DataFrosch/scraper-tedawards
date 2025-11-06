"""
Utility classes and functions shared across parsers.
"""

import logging
import re
from pathlib import Path
from typing import Optional, Dict, Any
from lxml import etree
from datetime import datetime, date

logger = logging.getLogger(__name__)


class XmlUtils:
    """Shared XML processing utilities for parsers."""

    @staticmethod
    def get_text(elem, xpath: str, default: str = '') -> str:
        """Get text content from xpath."""
        result = elem.xpath(xpath)
        return result[0].text if result and result[0].text else default

    @staticmethod
    def get_attr(elem, xpath: str, attr: str, default: str = '') -> str:
        """Get attribute value from xpath."""
        result = elem.xpath(xpath)
        return result[0].get(attr, default) if result else default

    @staticmethod
    def extract_text(elem) -> str:
        """Extract text content from an XML element, handling nested elements."""
        if elem is None:
            return ''

        # If the element has direct text
        if elem.text:
            # Handle cases where there might be nested elements
            text_parts = [elem.text.strip() if elem.text else '']

            # Add text from any nested elements
            for child in elem:
                if child.text:
                    text_parts.append(child.text.strip())
                if child.tail:
                    text_parts.append(child.tail.strip())

            # Join non-empty parts
            return ' '.join(part for part in text_parts if part)

        return ''

    @staticmethod
    def get_multiline_text(elem, xpath: str) -> str:
        """Get concatenated text from multiple P elements."""
        results = elem.xpath(xpath)
        return ' '.join(p.text for p in results if p.text).strip()

    @staticmethod
    def get_int(elem, xpath: str, default: Optional[int] = None) -> Optional[int]:
        """Get integer value from xpath."""
        text = XmlUtils.get_text(elem, xpath)
        return int(text) if text and text.isdigit() else default

    @staticmethod
    def get_decimal(elem, xpath: str, default: Optional[float] = None) -> Optional[float]:
        """Get decimal value from xpath."""
        text = XmlUtils.get_text(elem, xpath)
        try:
            return float(text) if text else default
        except ValueError:
            return default

    @staticmethod
    def get_text_with_namespace(elem, xpath: str, ns: Dict[str, str], default: Optional[str] = None) -> Optional[str]:
        """Get text content from xpath with namespace support."""
        try:
            result = elem.xpath(xpath, namespaces=ns)
            return result[0].text if result and result[0].text else default
        except Exception as e:
            logger.error(f"Error extracting text with namespace for xpath '{xpath}': {e}")
            raise

    @staticmethod
    def get_attr_with_namespace(elem, xpath: str, attr: str, ns: Dict[str, str], default: Optional[str] = None) -> Optional[str]:
        """Get attribute value from xpath with namespace support."""
        try:
            result = elem.xpath(xpath, namespaces=ns)
            return result[0].get(attr, default) if result else default
        except Exception as e:
            logger.error(f"Error extracting attribute '{attr}' with namespace for xpath '{xpath}': {e}")
            raise

    @staticmethod
    def get_decimal_from_text(text: str, default: Optional[float] = None) -> Optional[float]:
        """Convert text to decimal."""
        if not text:
            return default
        try:
            return float(text)
        except (ValueError, TypeError):
            return default


class FileDetector:
    """Utility for detecting file formats."""

    @staticmethod
    def is_ted_v2(file_path: Path) -> bool:
        """Check if this file uses any TED 2.0 format variant (R2.0.7, R2.0.8, R2.0.9)."""
        try:
            tree = etree.parse(file_path)
            root = tree.getroot()

            # Check for TED_EXPORT root element (namespace-agnostic check)
            # Different R2.0.x versions use different namespaces:
            # - R2.0.7/R2.0.8: http://publications.europa.eu/TED_schema/Export
            # - R2.0.9: http://publications.europa.eu/resource/schema/ted/R2.0.9/publication
            if not root.tag.endswith('}TED_EXPORT') and root.tag != 'TED_EXPORT':
                return False

            # Check if it's document type 7 (Contract award) - use namespace-agnostic xpath
            doc_type = root.xpath('.//*[local-name()="TD_DOCUMENT_TYPE"][@CODE="7"]')
            if not doc_type:
                return False

            # Must have either CONTRACT_AWARD (R2.0.7/R2.0.8) or F03_2014 (R2.0.9) form
            has_contract_award = len(root.xpath('.//*[local-name()="CONTRACT_AWARD"]')) > 0
            has_f03_2014 = len(root.xpath('.//*[local-name()="F03_2014"]')) > 0

            return has_contract_award or has_f03_2014

        except Exception as e:
            logger.debug(f"Error checking if {file_path.name} is TED 2.0 format: {e}")
            return False

    @staticmethod
    def is_eforms_ubl(file_path: Path) -> bool:
        """Check if this is an eForms UBL ContractAwardNotice format file."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(1000)  # Read first 1KB
            return ('ContractAwardNotice' in content and
                    'urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2' in content)
        except Exception as e:
            logger.debug(f"Error reading file {file_path.name} for eForms UBL detection: {e}")
            return False

    @staticmethod
    def is_ted_text_format(file_path: Path) -> bool:
        """Check if this file uses TED text format (ZIP containing text files)."""
        try:
            import zipfile

            # Check for both uppercase and lowercase zip extensions
            if not (file_path.name.upper().endswith('.ZIP')):
                return False

            # Check if it's a language-specific ZIP file pattern - case insensitive
            # Format patterns:
            # - XX_YYYYMMDD_NNN_UTF8_ORG.ZIP (e.g., EN_20070103_001_UTF8_ORG.ZIP)
            # - xx_yyyymmdd_nnn_utf8_org.zip (e.g., en_20080103_001_utf8_org.zip)
            # - xx_yyyymmdd_nnn_meta_org.zip (e.g., en_20080103_001_meta_org.zip) - PREFERRED
            pattern = r'^[a-zA-Z]{2}_\d{8}_\d+_(utf8|meta|iso)_org'

            # First check the wrapper filename
            if re.match(pattern, file_path.name, re.IGNORECASE):
                return True

            # If wrapper doesn't match, check the contents (for test fixtures with simplified names)
            try:
                with zipfile.ZipFile(file_path, 'r') as zf:
                    names = zf.namelist()
                    if names:
                        # Check if any file inside matches the pattern
                        for name in names:
                            if re.match(pattern, name, re.IGNORECASE):
                                return True
            except (zipfile.BadZipFile, OSError):
                pass

            return False
        except Exception as e:
            logger.debug(f"Error checking if {file_path.name} is TED text format: {e}")
            return False

    @staticmethod
    def is_award_notice(file_path: Path) -> bool:
        """Check if file contains award notice (document type 7)."""
        try:
            if file_path.suffix.lower() == '.zip':
                return True  # Text format detection is handled separately

            tree = etree.parse(file_path)
            root = tree.getroot()

            # Check for document type 7 in various formats
            doc_type = root.xpath('//*[local-name()="TD_DOCUMENT_TYPE"]/@CODE')
            return doc_type and doc_type[0] == '7'
        except Exception as e:
            logger.error(f"Error parsing file {file_path.name} for award notice detection: {e}")
            raise


class DateParsingUtils:
    """Consolidated date parsing utilities."""

    @staticmethod
    def normalize_date_string(date_str: Optional[str]) -> Optional[date]:
        """Helper to normalize date strings to date objects."""
        if not date_str:
            return None

        if isinstance(date_str, date):
            return date_str

        if isinstance(date_str, str):
            # Try various date formats
            formats = [
                '%Y-%m-%d',           # ISO format
                '%Y%m%d',             # YYYYMMDD
                '%d.%m.%Y',           # DD.MM.YYYY
                '%d/%m/%Y',           # DD/MM/YYYY
                '%m/%d/%Y',           # MM/DD/YYYY
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt).date()
                except ValueError:
                    continue

            # Try ISO format parsing
            try:
                return datetime.fromisoformat(date_str).date()
            except ValueError:
                pass

        return None

    @staticmethod
    def parse_award_date_from_text(text: str) -> Optional[str]:
        """Extract award date from text content."""
        if not text:
            return None

        # Look for "Date of award" section
        date_match = re.search(r'Date of award[:\s]*(\d{1,2})\.(\d{1,2})\.(\d{4})', text, re.IGNORECASE)
        if date_match:
            day, month, year = date_match.groups()
            try:
                award_date = datetime(int(year), int(month), int(day)).date()
                return award_date.isoformat()
            except ValueError:
                pass

        return None

    @staticmethod
    def parse_date_components(day_elem, month_elem, year_elem) -> Optional[date]:
        """Parse date from separate day/month/year elements."""
        if not all(elem is not None for elem in [day_elem, month_elem, year_elem]):
            return None

        try:
            return datetime(
                int(year_elem.text),
                int(month_elem.text),
                int(day_elem.text)
            ).date()
        except (ValueError, TypeError):
            return None