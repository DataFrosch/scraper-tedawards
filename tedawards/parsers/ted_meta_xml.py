"""
TED META XML format parser for legacy formats (2007-2013).
Handles the META XML format contained in ZIP archives.
"""

import logging
import zipfile
from pathlib import Path
from typing import List, Optional
from datetime import datetime
from lxml import etree

from .base import BaseParser
from ..schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)
from ..utils import FileDetector

logger = logging.getLogger(__name__)


class TedMetaXmlParser(BaseParser):
    """Parser for TED META XML format contained in ZIP archives."""

    def can_parse(self, file_path: Path) -> bool:
        """Check if this file uses TED META XML format."""
        return FileDetector.is_ted_text_format(file_path) and '_meta_org.' in file_path.name.lower()

    def get_format_name(self) -> str:
        """Return the format name for this parser."""
        return "TED META XML"

    def parse_xml_file(self, file_path: Path) -> Optional[TedParserResultModel]:
        """Parse a TED META XML format ZIP file and extract award data."""
        try:
            return self._parse_meta_xml_zip(file_path)
        except Exception as e:
            logger.error(f"Error parsing {file_path.name}: {e}")
            return None

    def _parse_meta_xml_zip(self, zip_path: Path) -> Optional[TedParserResultModel]:
        """Parse META XML ZIP file and extract all award notices."""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                names = zf.namelist()
                if not names:
                    logger.warning(f"No files found in {zip_path}")
                    return None

                # Read and parse XML content
                xml_content = zf.read(names[0]).decode('utf-8', errors='ignore')
                root = etree.fromstring(xml_content.encode('utf-8'))

                # Find all award documents
                award_records = []

                # Process CONTRACT_AWARD elements (original language files only: lg == lgorig)
                for doc in root.xpath('.//CONTRACT_AWARD[@category="orig"]'):
                    # Only process if document language matches original language
                    if doc.get('lg') == doc.get('lgorig'):
                        award_data = self._convert_meta_xml_to_standard_format(doc)
                        if award_data:
                            award_records.append(award_data)

                # Process OTH_NOT elements with award notices (original language files only: lg == lgorig)
                for doc in root.xpath('.//OTH_NOT[@category="orig"]'):
                    # Check if this is an award notice (natnotice code="7") AND original language matches current
                    natnotice = doc.xpath('.//natnotice[@code="7"]')
                    if natnotice and doc.get('lg') == doc.get('lgorig'):
                        award_data = self._convert_meta_xml_to_standard_format(doc)
                        if award_data:
                            award_records.append(award_data)

                if award_records:
                    logger.info(f"Found {len(award_records)} award records in {zip_path.name}")
                    return TedParserResultModel(awards=award_records)
                else:
                    logger.debug(f"No award records found in {zip_path.name}")
                    return None

        except Exception as e:
            logger.error(f"Error parsing META XML ZIP file {zip_path}: {e}")
            return None

    def _convert_meta_xml_to_standard_format(self, doc_elem) -> Optional[TedAwardDataModel]:
        """Convert META XML document element to standardized format."""
        try:
            # Extract document metadata from parent doc element
            doc_parent = doc_elem.getparent()
            if doc_parent is None:
                logger.warning("No parent doc element found")
                return None

            doc_id = doc_parent.get('id', '')
            current_lang = doc_elem.get('lg', '')
            original_lang = doc_elem.get('lgorig', '')

            if not original_lang:
                logger.warning(f"No original language found for document {doc_id}")
                return None

            # Extract codified data
            codifdata = doc_elem.xpath('.//codifdata')[0] if doc_elem.xpath('.//codifdata') else None
            if codifdata is None:
                logger.warning(f"No codified data found in document {doc_id}")
                return None

            # Get document reference and metadata
            nodocojs = codifdata.xpath('./nodocojs/text()')[0] if codifdata.xpath('./nodocojs/text()') else doc_id
            datedisp = codifdata.xpath('./datedisp/text()')[0] if codifdata.xpath('./datedisp/text()') else ''
            daterec = codifdata.xpath('./daterec/text()')[0] if codifdata.xpath('./daterec/text()') else ''
            isocountry = codifdata.xpath('./isocountry/text()')[0] if codifdata.xpath('./isocountry/text()') else ''

            # Parse publication date from refojs
            pub_date = None
            datepub = ''
            refojs = doc_elem.xpath('.//refojs')[0] if doc_elem.xpath('.//refojs') else None
            if refojs is not None:
                datepub = refojs.xpath('./datepub/text()')[0] if refojs.xpath('./datepub/text()') else ''
                if datepub and len(datepub) == 8:
                    try:
                        pub_date = datetime.strptime(datepub, '%Y%m%d').date()
                    except ValueError:
                        logger.warning(f"Invalid publication date format: {datepub}")

            # Parse dispatch date
            dispatch_date_obj = None
            if datedisp and len(datedisp) == 8:
                try:
                    dispatch_date_obj = datetime.strptime(datedisp, '%Y%m%d').date()
                except ValueError:
                    pass

            # Build universal document identifier
            full_doc_id = f"meta-{nodocojs}-{datepub}" if datepub else f"meta-{nodocojs}"

            # Extract title from tidoc
            title = ''
            tidoc = doc_elem.xpath('.//tidoc/p[1]/text()')
            if tidoc:
                title = tidoc[0].strip()

            # Extract contracting body info
            contracting_body_name = ''
            contracting_body_town = ''

            # Look for organization in contents
            org_elem = doc_elem.xpath('.//organisation/text()')
            if org_elem:
                contracting_body_name = org_elem[0].strip()

            # Look for town in contents
            town_elem = doc_elem.xpath('.//town/text()')
            if town_elem:
                contracting_body_town = town_elem[0].strip()

            # Extract CPV code
            main_cpv_code = ''
            originalcpv = codifdata.xpath('./originalcpv/@code')
            if originalcpv:
                main_cpv_code = originalcpv[0]

            # Extract contract value from contents if available
            contract_value = self._parse_xml_contract_value(doc_elem)

            # Extract contractor info from contents if available
            contractors = self._parse_xml_contractors(doc_elem)

            # Build award data using Pydantic models
            document = DocumentModel(
                doc_id=full_doc_id,
                edition='1',
                version='1',
                reception_id=nodocojs,
                deletion_date=None,
                form_language=current_lang,
                official_journal_ref=f"{datepub}/{nodocojs}" if datepub else nodocojs,
                publication_date=pub_date,
                dispatch_date=dispatch_date_obj,
                original_language=original_lang,
                source_country=isocountry
            )

            contracting_body = ContractingBodyModel(
                official_name=contracting_body_name,
                address='',
                town=contracting_body_town,
                postal_code='',
                country_code=isocountry,
                nuts_code=None,
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
                title=title,
                reference_number=nodocojs,
                short_description='',
                main_cpv_code=main_cpv_code,
                contract_nature_code='',
                total_value=contract_value,
                total_value_currency='EUR' if contract_value else None,
                procedure_type_code='',
                award_criteria_code='',
                performance_nuts_code=None
            )

            award = AwardModel(
                award_title=title,
                conclusion_date=dispatch_date_obj,
                contract_number=nodocojs,
                tenders_received=None,
                tenders_received_sme=None,
                tenders_received_other_eu=None,
                tenders_received_non_eu=None,
                tenders_received_electronic=None,
                awarded_value=contract_value,
                awarded_value_currency='EUR' if contract_value else None,
                subcontracted_value=None,
                subcontracted_value_currency=None,
                subcontracting_description='',
                contractors=contractors
            )

            logger.debug(f"Converted XML document {doc_id} (original: {original_lang}, current: {current_lang})")

            return TedAwardDataModel(
                document=document,
                contracting_body=contracting_body,
                contract=contract,
                awards=[award]
            )

        except Exception as e:
            logger.error(f"Error converting META XML document to standard format: {e}")
            return None

    def _parse_xml_contract_value(self, doc_elem) -> Optional[float]:
        """Extract contract value from XML contents."""
        try:
            # Look for EUR amounts in text content
            contents_text = ''
            for text_elem in doc_elem.xpath('.//contents//text()'):
                contents_text += text_elem + ' '

            if contents_text:
                # Simple regex to find EUR amounts
                import re
                value_patterns = [
                    r'EUR\s*([\d,.\s]+)',
                    r'€\s*([\d,.\s]+)',
                    r'([\d,.\s]+)\s*EUR',
                    r'([\d,.\s]+)\s*€'
                ]

                for pattern in value_patterns:
                    match = re.search(pattern, contents_text, re.IGNORECASE)
                    if match:
                        value_str = match.group(1).replace(',', '').replace(' ', '')
                        try:
                            return float(value_str)
                        except ValueError:
                            continue
        except Exception:
            pass
        return None

    def _parse_xml_contractors(self, doc_elem) -> List[ContractorModel]:
        """Extract contractor information from XML contents."""
        contractors = []
        try:
            # Look for organization names in contents
            org_elems = doc_elem.xpath('.//contents//organisation/text()')
            for org_name in org_elems:
                if org_name.strip():
                    contractors.append(ContractorModel(
                        official_name=org_name.strip(),
                        address='',
                        town='',
                        postal_code='',
                        country_code=None,  # Use None instead of empty string for foreign key constraint
                        nuts_code=None,
                        phone='',
                        email='',
                        fax='',
                        url='',
                        is_sme=False
                    ))
        except Exception as e:
            logger.debug(f"Error extracting contractors from XML: {e}")

        return contractors