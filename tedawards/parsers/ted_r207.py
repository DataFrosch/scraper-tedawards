"""
TED R2.0.7 parser for pre-2014 formats.
Handles CONTRACT_AWARD forms instead of F03_2014.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
from lxml import etree

from .base import BaseParser
from ..schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)
from ..utils import FileDetector, DateParsingUtils

logger = logging.getLogger(__name__)


class TedR207Parser(BaseParser):
    """Parser for TED R2.0.7 format used before 2014."""

    def can_parse(self, xml_file: Path) -> bool:
        """Check if this file uses TED R2.0.7 format."""
        return FileDetector.is_ted_r207(xml_file)

    def get_format_name(self) -> str:
        """Return the format name for this parser."""
        return "TED R2.0.7"

    def parse_xml_file(self, xml_file: Path) -> Optional[TedParserResultModel]:
        """Parse a TED R2.0.7 XML file and extract award data."""
        try:
            tree = etree.parse(xml_file)
            root = tree.getroot()

            # Check if this document is in its original language to avoid duplicates
            lang_orig_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}LG_ORIG')
            original_lang = lang_orig_elem.text.strip() if lang_orig_elem is not None else None

            if not self._is_original_language_version(root, original_lang):
                logger.debug(f"Skipping {xml_file.name} - translation (original language: {original_lang})")
                return None

            # Extract all components in the expected format
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

            award_data = TedAwardDataModel(
                document=document_model,
                contracting_body=contracting_body_model,
                contract=contract_model,
                awards=award_models
            )

            return TedParserResultModel(awards=[award_data])

        except Exception as e:
            logger.error(f"Error parsing TED R2.0.7 file {xml_file}: {e}")
            return None

    def _extract_document_info(self, root, xml_file: Path) -> Optional[Dict]:
        """Extract document-level information."""
        try:
            # Generate document ID from filename
            doc_id = xml_file.stem.replace('_', '-')

            # Extract publication date
            pub_date_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}DATE_PUB')
            if pub_date_elem is None:
                logger.debug(f"No publication date found in {xml_file.name}")
                return None

            pub_date = DateParsingUtils.normalize_date_string(pub_date_elem.text)
            if not pub_date:
                logger.debug(f"Invalid publication date in {xml_file.name}")
                return None

            # Extract dispatch date
            dispatch_date_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}DS_DATE_DISPATCH')
            dispatch_date = None
            if dispatch_date_elem is not None:
                dispatch_date = DateParsingUtils.normalize_date_string(dispatch_date_elem.text)

            # Extract other document metadata
            reception_id_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}RECEPTION_ID')
            deletion_date_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}DELETION_DATE')
            oj_ref_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}NO_DOC_OJS')
            lang_orig_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}LG_ORIG')
            country_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}ISO_COUNTRY')

            # Extract form language list
            form_lang_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}FORM_LG_LIST')
            form_language = form_lang_elem.text.strip() if form_lang_elem is not None else None

            return {
                'doc_id': doc_id,
                'edition': root.get('EDITION'),
                'version': None,  # Legacy format doesn't have version info
                'reception_id': reception_id_elem.text if reception_id_elem is not None else None,
                'deletion_date': DateParsingUtils.normalize_date_string(deletion_date_elem.text) if deletion_date_elem is not None else None,
                'form_language': form_language,
                'official_journal_ref': oj_ref_elem.text if oj_ref_elem is not None else None,
                'publication_date': pub_date,
                'dispatch_date': dispatch_date,
                'original_language': lang_orig_elem.text.strip() if lang_orig_elem is not None else None,
                'source_country': country_elem.get('VALUE') if country_elem is not None else None
            }

        except Exception as e:
            logger.error(f"Error extracting document info: {e}")
            return None

    def _extract_contracting_body(self, root) -> Optional[Dict]:
        """Extract contracting body information from CONTACTING_AUTHORITY_INFORMATION."""
        try:
            # Find contracting authority in legacy format
            ca_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}CONTACTING_AUTHORITY_INFORMATION//{http://publications.europa.eu/TED_schema/Export}CA_CE_CONCESSIONAIRE_PROFILE')
            if ca_elem is None:
                return None

            name_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ORGANISATION')
            address_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ADDRESS')
            town_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TOWN')
            postal_code_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}POSTAL_CODE')
            country_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}COUNTRY')
            phone_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}PHONE')
            email_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}E_MAIL')
            fax_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}FAX')

            # Extract URL from INTERNET_ADDRESSES section
            url_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}INTERNET_ADDRESSES_CONTRACT_AWARD//{http://publications.europa.eu/TED_schema/Export}URL_GENERAL')

            # Extract authority type and activity codes
            authority_type_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}AA_AUTHORITY_TYPE')
            activity_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}MA_MAIN_ACTIVITIES')

            return {
                'official_name': name_elem.text if name_elem is not None else '',
                'address': address_elem.text if address_elem is not None else None,
                'town': town_elem.text if town_elem is not None else None,
                'postal_code': postal_code_elem.text if postal_code_elem is not None else None,
                'country_code': country_elem.get('VALUE') if country_elem is not None else None,
                'nuts_code': None,  # Legacy format may not have NUTS codes
                'contact_point': None,  # Not typically in legacy format
                'phone': phone_elem.text if phone_elem is not None else None,
                'email': email_elem.text if email_elem is not None else None,
                'fax': fax_elem.text if fax_elem is not None else None,
                'url_general': url_elem.text if url_elem is not None else None,
                'url_buyer': None,  # Not in legacy format
                'authority_type_code': authority_type_elem.get('CODE') if authority_type_elem is not None else None,
                'main_activity_code': activity_elem.get('CODE') if activity_elem is not None else None
            }

        except Exception as e:
            logger.error(f"Error extracting contracting body: {e}")
            return None

    def _extract_contract_info(self, root) -> Optional[Dict]:
        """Extract contract information from OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE."""
        try:
            # Find contract description section
            desc_section = root.find('.//{http://publications.europa.eu/TED_schema/Export}OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE//{http://publications.europa.eu/TED_schema/Export}DESCRIPTION_AWARD_NOTICE_INFORMATION')
            if desc_section is None:
                return None

            # Extract contract title
            title_elem = desc_section.find('.//{http://publications.europa.eu/TED_schema/Export}TITLE_CONTRACT//{http://publications.europa.eu/TED_schema/Export}P')
            title = title_elem.text if title_elem is not None else None

            # Extract short description
            desc_elem = desc_section.find('.//{http://publications.europa.eu/TED_schema/Export}SHORT_CONTRACT_DESCRIPTION//{http://publications.europa.eu/TED_schema/Export}P')
            description = desc_elem.text if desc_elem is not None else None

            # Extract main CPV code
            cpv_main_elem = desc_section.find('.//{http://publications.europa.eu/TED_schema/Export}CPV_MAIN//{http://publications.europa.eu/TED_schema/Export}CPV_CODE')
            cpv_code = cpv_main_elem.get('CODE') if cpv_main_elem is not None else None

            # Extract total value
            total_value_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}TOTAL_FINAL_VALUE//{http://publications.europa.eu/TED_schema/Export}VALUE_COST')
            estimated_value = None
            if total_value_elem is not None:
                try:
                    estimated_value = float(total_value_elem.get('FMTVAL', '0'))
                except (ValueError, TypeError) as e:
                    logger.error(f"Error parsing estimated value from FMTVAL: {e}")
                    raise

            # Extract contract type and location
            contract_type_elem = desc_section.find('.//{http://publications.europa.eu/TED_schema/Export}TYPE_CONTRACT')
            contract_type = contract_type_elem.get('VALUE') if contract_type_elem is not None else None

            # Extract NUTS code from location
            nuts_elem = desc_section.find('.//{http://publications.europa.eu/TED_schema/Export}NUTS')
            nuts_code = nuts_elem.get('CODE') if nuts_elem is not None else None

            # Extract total value currency from the total value element
            total_value_currency = 'EUR'  # Default for legacy format
            if total_value_elem is not None:
                currency_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE')
                if currency_elem is not None:
                    total_value_currency = currency_elem.get('CURRENCY', 'EUR')

            return {
                'title': title or '',
                'reference_number': None,  # Not typically in legacy format
                'short_description': description,
                'main_cpv_code': cpv_code,
                'contract_nature_code': contract_type,
                'total_value': estimated_value,
                'total_value_currency': total_value_currency,
                'procedure_type_code': None,  # Could extract from procedure info if needed
                'award_criteria_code': None,  # Could extract from award criteria if needed
                'performance_nuts_code': nuts_code
            }

        except Exception as e:
            logger.error(f"Error extracting contract info: {e}")
            return None

    def _extract_awards(self, root) -> List[Dict]:
        """Extract award information from AWARD_OF_CONTRACT sections."""
        awards = []

        try:
            # Find all award sections
            award_sections = root.findall('.//{http://publications.europa.eu/TED_schema/Export}AWARD_OF_CONTRACT')

            for award_section in award_sections:
                # Extract contract/lot number
                contract_num_elem = award_section.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_NUMBER')
                lot_num_elem = award_section.find('.//{http://publications.europa.eu/TED_schema/Export}LOT_NUMBER')

                # Extract contract title
                title_elem = award_section.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_TITLE//{http://publications.europa.eu/TED_schema/Export}P')
                title = title_elem.text if title_elem is not None else None

                # Extract award date
                award_date = None
                award_date_elem = award_section.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_AWARD_DATE')
                if award_date_elem is not None:
                    day_elem = award_date_elem.find('.//{http://publications.europa.eu/TED_schema/Export}DAY')
                    month_elem = award_date_elem.find('.//{http://publications.europa.eu/TED_schema/Export}MONTH')
                    year_elem = award_date_elem.find('.//{http://publications.europa.eu/TED_schema/Export}YEAR')

                    award_date = DateParsingUtils.parse_date_components(day_elem, month_elem, year_elem)

                # Extract number of offers received
                offers_received = None
                offers_elem = award_section.find('.//{http://publications.europa.eu/TED_schema/Export}OFFERS_RECEIVED_NUMBER')
                if offers_elem is not None and offers_elem.text is not None:
                    try:
                        offers_received = int(offers_elem.text.strip())
                    except (ValueError, TypeError) as e:
                        logger.error(f"Error parsing offers received: {e}")
                        raise

                # Extract contract value
                contract_value = None
                value_currency = None
                value_elem = award_section.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_VALUE_INFORMATION//{http://publications.europa.eu/TED_schema/Export}VALUE_COST')
                if value_elem is not None:
                    try:
                        contract_value = float(value_elem.get('FMTVAL', '0'))
                        # Extract currency from parent element
                        currency_elem = award_section.find('.//{http://publications.europa.eu/TED_schema/Export}COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE')
                        if currency_elem is not None:
                            value_currency = currency_elem.get('CURRENCY')
                    except (ValueError, TypeError) as e:
                        logger.error(f"Error parsing contract value: {e}")
                        raise

                # Extract contractor information
                contractors = []
                contractor_elem = award_section.find('.//{http://publications.europa.eu/TED_schema/Export}ECONOMIC_OPERATOR_NAME_ADDRESS//{http://publications.europa.eu/TED_schema/Export}CONTACT_DATA_WITHOUT_RESPONSIBLE_NAME')
                if contractor_elem is not None:
                    org_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ORGANISATION')
                    addr_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ADDRESS')
                    town_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TOWN')
                    postal_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}POSTAL_CODE')
                    country_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}COUNTRY')

                    contractor = {
                        'official_name': org_elem.text if org_elem is not None else '',
                        'address': addr_elem.text if addr_elem is not None else None,
                        'town': town_elem.text if town_elem is not None else None,
                        'postal_code': postal_elem.text if postal_elem is not None else None,
                        'country_code': country_elem.get('VALUE') if country_elem is not None else None,
                        'nuts_code': None,  # Legacy format typically doesn't have NUTS for contractors
                        'phone': None,  # Not available in legacy format
                        'email': None,  # Not available in legacy format
                        'fax': None,  # Not available in legacy format
                        'url': None,  # Not available in legacy format
                        'is_sme': False  # Not available in legacy format
                    }

                    if contractor['official_name']:  # Only add if we have a name
                        contractors.append(contractor)

                # Build award data in expected format
                award = {
                    'award_title': title,
                    'conclusion_date': award_date,  # Use award date as conclusion date for legacy format
                    'contract_number': contract_num_elem.text if contract_num_elem is not None else None,

                    # Tender statistics
                    'tenders_received': offers_received,
                    'tenders_received_sme': None,  # Not available in legacy format
                    'tenders_received_other_eu': None,
                    'tenders_received_non_eu': None,
                    'tenders_received_electronic': None,

                    # Award value
                    'awarded_value': contract_value,
                    'awarded_value_currency': value_currency or 'EUR',

                    # Subcontracting - not available in legacy format
                    'subcontracted_value': None,
                    'subcontracted_value_currency': None,
                    'subcontracting_description': None,

                    # Contractors
                    'contractors': contractors
                }

                awards.append(award)

        except Exception as e:
            logger.error(f"Error extracting awards: {e}")

        return awards

    def _is_original_language_version(self, root, original_lang: str) -> bool:
        """
        Check if this XML document represents the original language version.

        In TED R2.0.7 format, we need to check if the document is being processed
        in its original language to avoid duplicates.
        """
        if not original_lang:
            # Fail loud - original language is required for deduplication
            raise ValueError(f"No original language (LG_ORIG) found in document - cannot determine if this is original or translation")

        original_lang = original_lang.strip().upper()

        # Look for elements marked as CATEGORY="ORIGINAL"
        original_elements = root.xpath('//*[@CATEGORY="ORIGINAL"]')

        if not original_elements:
            # Fail loud - we must be able to identify the original language section
            raise ValueError(f"No element with CATEGORY='ORIGINAL' found - cannot determine original language version")

        # Check if any ORIGINAL element's language matches the document's original language
        for elem in original_elements:
            elem_lang = elem.get('LG', '').strip().upper()
            if elem_lang == original_lang:
                logger.debug(f"Processing document in original language: {original_lang}")
                return True

        logger.debug(f"Skipping translation - original language is {original_lang}, but no ORIGINAL section found for this language")
        return False

