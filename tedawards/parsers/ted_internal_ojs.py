"""
TED INTERNAL_OJS format parser for R2.0.5 (2008).
Handles the INTERNAL_OJS wrapper format with language-specific files.
"""

import logging
import re
from pathlib import Path
from typing import Optional, List
from datetime import datetime
from lxml import etree

from .base import BaseParser
from ..schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)

logger = logging.getLogger(__name__)


class TedInternalOjsParser(BaseParser):
    """Parser for TED INTERNAL_OJS format (R2.0.5, 2008)."""

    def can_parse(self, file_path: Path) -> bool:
        """Check if this file uses INTERNAL_OJS format."""
        try:
            # Check for .en extension (English language files only)
            if not file_path.suffix.lower() == '.en':
                return False

            tree = etree.parse(file_path)
            root = tree.getroot()

            # Check for INTERNAL_OJS root element
            if root.tag != 'INTERNAL_OJS':
                return False

            # Check if it's an award notice (NAT_NOTICE = 7)
            nat_notice = root.xpath('.//BIB_DOC_S/NAT_NOTICE/text()')
            if not nat_notice or nat_notice[0] != '7':
                return False

            # Must have CONTRACT_AWARD_SUM form
            has_award_form = len(root.xpath('.//CONTRACT_AWARD_SUM')) > 0

            return has_award_form

        except Exception as e:
            logger.debug(f"Error checking if {file_path.name} is INTERNAL_OJS format: {e}")
            return False

    def get_format_name(self) -> str:
        """Return the format name for this parser."""
        return "TED INTERNAL_OJS R2.0.5"

    def parse_xml_file(self, xml_file: Path) -> Optional[TedParserResultModel]:
        """Parse an INTERNAL_OJS XML file and extract award data."""
        try:
            tree = etree.parse(xml_file)
            root = tree.getroot()

            logger.debug(f"Processing {xml_file.name} as INTERNAL_OJS R2.0.5")

            # Extract all components
            document_info = self._extract_document_info(root, xml_file)
            if not document_info:
                return None

            contracting_body = self._extract_contracting_body(root)
            if not contracting_body:
                logger.debug(f"No contracting body found in {xml_file.name}")
                return None

            contract_info = self._extract_contract_info(root)
            if not contract_info:
                logger.debug(f"No contract info found in {xml_file.name}")
                return None

            awards = self._extract_awards(root)
            if not awards:
                logger.debug(f"No awards found in {xml_file.name}")
                return None

            # Convert to Pydantic models
            document_model = DocumentModel(**document_info)
            contracting_body_model = ContractingBodyModel(**contracting_body)
            contract_model = ContractModel(**contract_info)
            award_models = [AwardModel(**award) for award in awards]

            # Return single award data model
            return TedParserResultModel(
                awards=[
                    TedAwardDataModel(
                        document=document_model,
                        contracting_body=contracting_body_model,
                        contract=contract_model,
                        awards=award_models
                    )
                ]
            )

        except Exception as e:
            logger.error(f"Error parsing INTERNAL_OJS file {xml_file}: {e}")
            return None

    def _extract_document_info(self, root, xml_file: Path) -> Optional[dict]:
        """Extract document metadata from BIB_INFO section."""
        try:
            bib_info = root.xpath('.//BIB_INFO')[0]
            bib_doc_s = root.xpath('.//BIB_DOC_S')[0]

            # Extract OJS reference info
            ref_ojs = bib_info.xpath('.//REF_OJS')[0]
            no_oj = self._get_text(ref_ojs, './/NO_OJ')
            date_pub = self._get_text(ref_ojs, './/DATE_PUB')
            lg_oj = self._get_text(ref_ojs, './/LG_OJ')

            # Extract document reference (use direct child to avoid REF_NOTICE)
            no_doc_ojs_elems = bib_doc_s.xpath('./NO_DOC_OJS/text()')
            no_doc_ojs = no_doc_ojs_elems[0] if no_doc_ojs_elems else ''
            iso_country = self._get_text(bib_doc_s, './ISO_COUNTRY')
            date_disp = self._get_text(bib_doc_s, './DATE_DISP')
            date_rec = self._get_text(bib_doc_s, './DATE_REC')

            # Parse dates
            publication_date = None
            if date_pub and len(date_pub) == 8:
                try:
                    publication_date = datetime.strptime(date_pub, '%Y%m%d').date()
                except ValueError:
                    logger.warning(f"Invalid publication date format: {date_pub}")

            dispatch_date = None
            if date_disp and len(date_disp) == 8:
                try:
                    dispatch_date = datetime.strptime(date_disp, '%Y%m%d').date()
                except ValueError:
                    logger.warning(f"Invalid dispatch date format: {date_disp}")

            # Extract deletion date from TECHNICAL_INFO
            deletion_date = None
            deletion_date_str = self._get_text(root, './/TECHNICAL_INFO/DELETION_DATE')
            if deletion_date_str and len(deletion_date_str) == 8:
                try:
                    deletion_date = datetime.strptime(deletion_date_str, '%Y%m%d').date()
                except ValueError:
                    pass

            # Build document ID from NO_DOC_OJS
            doc_id = f"ojs-{no_doc_ojs}" if no_doc_ojs else f"ojs-{xml_file.stem}"

            return {
                'doc_id': doc_id,
                'edition': no_oj or '1',
                'version': '1',
                'reception_id': no_doc_ojs,
                'deletion_date': deletion_date,
                'official_journal_ref': no_doc_ojs,
                'publication_date': publication_date,
                'dispatch_date': dispatch_date,
                'source_country': iso_country
            }

        except Exception as e:
            logger.error(f"Error extracting document info: {e}")
            return None

    def _extract_contracting_body(self, root) -> Optional[dict]:
        """Extract contracting body information."""
        try:
            # Find contracting authority in FD_CONTRACT_AWARD_SUM
            ca_profile = root.xpath('.//FD_CONTRACT_AWARD_SUM//CA_CE_CONCESSIONAIRE_PROFILE')[0]

            organisation = self._get_text(ca_profile, './/ORGANISATION')
            address = self._get_text(ca_profile, './/ADDRESS')
            town = self._get_text(ca_profile, './/TOWN')
            postal_code = self._get_text(ca_profile, './/POSTAL_CODE')
            country = ca_profile.xpath('.//COUNTRY/@VALUE')
            country_code = country[0] if country else None
            phone = self._get_text(ca_profile, './/PHONE')
            email = self._get_text(ca_profile, './/E_MAIL')
            fax = self._get_text(ca_profile, './/FAX')

            if not organisation:
                return None

            return {
                'official_name': organisation,
                'address': address or '',
                'town': town or '',
                'postal_code': postal_code or '',
                'country_code': country_code,
                'nuts_code': None,
                'contact_point': '',
                'phone': phone or '',
                'email': email or '',
                'fax': fax or '',
                'url_general': '',
                'url_buyer': '',
                'authority_type_code': '',
                'main_activity_code': ''
            }

        except Exception as e:
            logger.error(f"Error extracting contracting body: {e}")
            return None

    def _extract_contract_info(self, root) -> Optional[dict]:
        """Extract contract information."""
        try:
            bib_doc_s = root.xpath('.//BIB_DOC_S')[0]

            # Extract title from TI_DOC
            title_parts = bib_doc_s.xpath('.//TI_DOC/P/text()')
            title = title_parts[0] if title_parts else ''

            # Extract reference number (use direct child to avoid REF_NOTICE)
            no_doc_ojs_elems = bib_doc_s.xpath('./NO_DOC_OJS/text()')
            no_doc_ojs = no_doc_ojs_elems[0] if no_doc_ojs_elems else ''

            # Extract CPV code
            cpv_code = self._get_text(bib_doc_s, './/ORIGINAL_CPV')

            # Extract description from form
            description = self._get_text(root, './/DESCRIPTION_SUM/P')

            # Extract total value
            total_value = None
            total_currency = None
            value_elem = root.xpath('.//TOTAL_FINAL_VALUE//VALUE_COST/text()')
            if value_elem:
                total_value = self._parse_value(value_elem[0])
                currency_elem = root.xpath('.//TOTAL_FINAL_VALUE//COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE/@CURRENCY')
                total_currency = currency_elem[0] if currency_elem else None

            return {
                'title': title,
                'reference_number': no_doc_ojs or '',
                'short_description': description or '',
                'main_cpv_code': cpv_code or '',
                'contract_nature_code': '',
                'total_value': total_value,
                'total_value_currency': total_currency,
                'procedure_type_code': '',
                'award_criteria_code': '',
                'performance_nuts_code': None
            }

        except Exception as e:
            logger.error(f"Error extracting contract info: {e}")
            return None

    def _extract_awards(self, root) -> Optional[List[dict]]:
        """Extract award information."""
        awards = []

        try:
            # Find all AWARD_OF_CONTRACT_SUM elements
            award_elements = root.xpath('.//AWARD_OF_CONTRACT_SUM')

            if not award_elements:
                return None

            for award_elem in award_elements:
                # Extract contract number
                contract_number = self._get_text(award_elem, './/CONTRACT_NUMBER')

                # Extract award value
                awarded_value = None
                awarded_currency = None
                value_elem = award_elem.xpath('.//CONTRACT_VALUE_INFORMATION//VALUE_COST/text()')
                if value_elem:
                    awarded_value = self._parse_value(value_elem[0])
                    currency_elem = award_elem.xpath('.//CONTRACT_VALUE_INFORMATION//COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE/@CURRENCY')
                    awarded_currency = currency_elem[0] if currency_elem else None

                # Extract contractors
                contractors = []
                contractor_elems = award_elem.xpath('.//ECONOMIC_OPERATOR_NAME_ADDRESS')
                for contractor_elem in contractor_elems:
                    contact_data = contractor_elem.xpath('.//CONTACT_DATA_WITHOUT_RESPONSIBLE_NAME')[0] if contractor_elem.xpath('.//CONTACT_DATA_WITHOUT_RESPONSIBLE_NAME') else None
                    if contact_data is not None:
                        org_name = self._get_text(contact_data, './/ORGANISATION')
                        if org_name:
                            address = self._get_text(contact_data, './/ADDRESS')
                            town = self._get_text(contact_data, './/TOWN')
                            postal_code = self._get_text(contact_data, './/POSTAL_CODE')
                            country = contact_data.xpath('.//COUNTRY/@VALUE')
                            country_code = country[0] if country else None
                            email = self._get_text(contact_data, './/E_MAIL')
                            phone = self._get_text(contact_data, './/PHONE')
                            fax = self._get_text(contact_data, './/FAX')

                            contractors.append({
                                'official_name': org_name,
                                'address': address or '',
                                'town': town or '',
                                'postal_code': postal_code or '',
                                'country_code': country_code,
                                'nuts_code': None,
                                'phone': phone or '',
                                'email': email or '',
                                'fax': fax or '',
                                'url': '',
                                'is_sme': False
                            })

                # Get title from contract info (reuse from parent)
                title_parts = root.xpath('.//BIB_DOC_S//TI_DOC/P/text()')
                title = title_parts[0] if title_parts else ''

                awards.append({
                    'award_title': title,
                    'conclusion_date': None,
                    'contract_number': contract_number or '',
                    'tenders_received': None,
                    'tenders_received_sme': None,
                    'tenders_received_other_eu': None,
                    'tenders_received_non_eu': None,
                    'tenders_received_electronic': None,
                    'awarded_value': awarded_value,
                    'awarded_value_currency': awarded_currency,
                    'subcontracted_value': None,
                    'subcontracted_value_currency': None,
                    'subcontracting_description': '',
                    'contractors': contractors
                })

            return awards if awards else None

        except Exception as e:
            logger.error(f"Error extracting awards: {e}")
            return None

    def _get_text(self, element, xpath: str) -> str:
        """Safely extract text from XML element using XPath."""
        try:
            result = element.xpath(xpath + '/text()')
            return result[0].strip() if result and result[0] else ''
        except Exception:
            return ''

    def _parse_value(self, value_str: str) -> Optional[float]:
        """Parse monetary value from string."""
        try:
            # Remove spaces and replace comma with dot for decimal separator
            cleaned = value_str.strip().replace(' ', '').replace(',', '.')
            # Remove any remaining non-numeric characters except dots
            cleaned = re.sub(r'[^\d.]', '', cleaned)
            return float(cleaned) if cleaned else None
        except ValueError:
            logger.warning(f"Could not parse value: {value_str}")
            return None
