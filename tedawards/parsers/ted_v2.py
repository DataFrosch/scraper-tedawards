"""
TED Version 2.0 parser - unified parser for all TED 2.0 variants.
Handles R2.0.7, R2.0.8, and R2.0.9 formats (2008-2023).
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
from ..utils import FileDetector, DateParsingUtils, XmlUtils

logger = logging.getLogger(__name__)


class TedV2Parser(BaseParser):
    """Unified parser for all TED 2.0 variants (R2.0.7, R2.0.8, R2.0.9)."""

    def can_parse(self, xml_file: Path) -> bool:
        """Check if this file uses any TED 2.0 format variant."""
        return FileDetector.is_ted_v2(xml_file)

    def get_format_name(self) -> str:
        """Return the format name for this parser."""
        return "TED 2.0"

    def parse_xml_file(self, xml_file: Path) -> Optional[TedParserResultModel]:
        """Parse a TED 2.0 XML file and extract award data."""
        try:
            tree = etree.parse(xml_file)
            root = tree.getroot()

            # Detect specific variant
            variant = self._detect_variant(root)
            logger.debug(f"Processing {xml_file.name} as {variant}")

            # Check if this document is in its original language to avoid duplicates
            original_lang = self._get_original_language(root)
            if not self._is_original_language_version(root, original_lang):
                logger.debug(f"Skipping {xml_file.name} - translation (original language: {original_lang})")
                return None

            # Extract all components using variant-aware methods
            document_info = self._extract_document_info(root, xml_file, variant)
            if not document_info:
                return None

            contracting_body = self._extract_contracting_body(root, variant)
            if not contracting_body:
                logger.debug(f"No contracting body found in {xml_file.name}")
                return None

            contract_info = self._extract_contract_info(root, variant)
            if not contract_info:
                logger.debug(f"No contract info found in {xml_file.name}")
                return None

            awards = self._extract_awards(root, variant)
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
            logger.error(f"Error parsing TED 2.0 file {xml_file}: {e}")
            return None

    def _detect_variant(self, root) -> str:
        """Detect which TED 2.0 variant this is based on XML structure."""
        # Check schema location for version
        schema_location = root.get('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation', '')

        if 'R2.0.9' in schema_location:
            return "R2.0.9"
        elif 'R2.0.8' in schema_location:
            return "R2.0.8"
        elif 'R2.0.7' in schema_location:
            return "R2.0.7"

        # Fall back to structural detection
        # R2.0.9 uses F03_2014 forms
        if root.find('.//{http://publications.europa.eu/TED_schema/Export}F03_2014') is not None:
            return "R2.0.9"
        # R2.0.7/R2.0.8 use CONTRACT_AWARD forms
        elif root.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_AWARD') is not None:
            return "R2.0.7/R2.0.8"

        return "Unknown"

    def _get_original_language(self, root) -> Optional[str]:
        """Get the original language of the document."""
        lang_orig_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}LG_ORIG')
        return lang_orig_elem.text.strip() if lang_orig_elem is not None else None

    def _is_original_language_version(self, root, original_lang: str) -> bool:
        """Check if this XML document represents the original language version."""
        if not original_lang:
            # Fail loud - original language is required for deduplication
            raise ValueError(f"No original language (LG_ORIG) found in document - cannot determine if this is original or translation")

        # Check the form language
        form_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_AWARD')
        if form_elem is None:
            form_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}F03_2014')

        if form_elem is not None:
            form_lang = form_elem.get('LG')
            form_category = form_elem.get('CATEGORY', '').upper()

            # Original documents should have CATEGORY="ORIGINAL" and matching language
            return (form_category == 'ORIGINAL' and
                    form_lang == original_lang)

        return False

    def _extract_document_info(self, root, xml_file: Path, variant: str) -> Optional[Dict]:
        """Extract document-level information."""
        try:
            # Generate document ID from filename
            doc_id = xml_file.stem.replace('_', '-')

            # Extract edition from root element
            edition = root.get('EDITION')
            if not edition:
                logger.debug(f"No edition found in {xml_file.name}")
                return None

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
            no_doc_oj_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}NO_DOC_OJS')
            country_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}ISO_COUNTRY')
            original_lang_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}LG_ORIG')

            return {
                'doc_id': doc_id,
                'edition': edition,
                'publication_date': pub_date,
                'dispatch_date': dispatch_date,
                'reception_id': reception_id_elem.text if reception_id_elem is not None else None,
                'official_journal_ref': no_doc_oj_elem.text if no_doc_oj_elem is not None else None,
                'source_country': country_elem.get('VALUE') if country_elem is not None else None,
                'original_language': original_lang_elem.text if original_lang_elem is not None else None,
                'form_language': original_lang_elem.text if original_lang_elem is not None else 'EN',
                'version': variant
            }

        except Exception as e:
            logger.error(f"Error extracting document info: {e}")
            return None

    def _extract_contracting_body(self, root, variant: str) -> Optional[Dict]:
        """Extract contracting body information based on variant."""
        try:
            if variant == "R2.0.9":
                return self._extract_contracting_body_r209(root)
            else:  # R2.0.7/R2.0.8
                return self._extract_contracting_body_r207(root)
        except Exception as e:
            logger.error(f"Error extracting contracting body: {e}")
            return None

    def _extract_contracting_body_r207(self, root) -> Optional[Dict]:
        """Extract contracting body for R2.0.7/R2.0.8 formats."""
        # Find contracting authority in R2.0.7/R2.0.8 format
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

        # Extract URL from various possible locations - also fix this path
        url_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}INTERNET_ADDRESSES_CONTRACT_AWARD//{http://publications.europa.eu/TED_schema/Export}URL_BUYER')

        # Extract authority type and activity codes from coded data section
        authority_type_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}AA_AUTHORITY_TYPE')
        activity_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}MA_MAIN_ACTIVITIES')

        return {
            'official_name': name_elem.text if name_elem is not None else '',
            'address': address_elem.text if address_elem is not None else None,
            'town': town_elem.text if town_elem is not None else None,
            'postal_code': postal_code_elem.text if postal_code_elem is not None else None,
            'country_code': country_elem.get('VALUE') if country_elem is not None else None,
            'nuts_code': None,  # May not be available in legacy format
            'contact_point': None,  # Not typically in legacy format
            'phone': phone_elem.text if phone_elem is not None else None,
            'email': email_elem.text if email_elem is not None else None,
            'fax': fax_elem.text if fax_elem is not None else None,
            'url_general': url_elem.text if url_elem is not None else None,
            'url_buyer': None,  # Not in legacy format
            'authority_type_code': authority_type_elem.get('CODE') if authority_type_elem is not None else None,
            'main_activity_code': activity_elem.get('CODE') if activity_elem is not None else None
        }

    def _extract_contracting_body_r209(self, root) -> Optional[Dict]:
        """Extract contracting body for R2.0.9 format."""
        # Find contracting authority in R2.0.9 format
        ca_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}F03_2014//{http://publications.europa.eu/TED_schema/Export}CONTRACTING_BODY')
        if ca_elem is None:
            return None

        # Extract basic info
        name_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}OFFICIALNAME')
        address_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ADDRESS')
        town_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TOWN')
        postal_code_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}POSTAL_CODE')
        country_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}COUNTRY')

        # Contact info
        contact_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CONTACT_POINT')
        phone_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}PHONE')
        email_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}E_MAIL')
        fax_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}FAX')

        # URLs
        url_general_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}URL_GENERAL')
        url_buyer_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}URL_BUYER')

        # Authority type and activity
        authority_type_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CA_TYPE')
        activity_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CA_ACTIVITY')

        return {
            'official_name': name_elem.text if name_elem is not None else '',
            'address': address_elem.text if address_elem is not None else None,
            'town': town_elem.text if town_elem is not None else None,
            'postal_code': postal_code_elem.text if postal_code_elem is not None else None,
            'country_code': country_elem.get('VALUE') if country_elem is not None else None,
            'nuts_code': None,  # Extract from NUTS if needed
            'contact_point': contact_elem.text if contact_elem is not None else None,
            'phone': phone_elem.text if phone_elem is not None else None,
            'email': email_elem.text if email_elem is not None else None,
            'fax': fax_elem.text if fax_elem is not None else None,
            'url_general': url_general_elem.text if url_general_elem is not None else None,
            'url_buyer': url_buyer_elem.text if url_buyer_elem is not None else None,
            'authority_type_code': authority_type_elem.get('VALUE') if authority_type_elem is not None else None,
            'main_activity_code': activity_elem.get('VALUE') if activity_elem is not None else None
        }

    def _extract_contract_info(self, root, variant: str) -> Optional[Dict]:
        """Extract contract information based on variant."""
        try:
            if variant == "R2.0.9":
                return self._extract_contract_info_r209(root)
            else:  # R2.0.7/R2.0.8
                return self._extract_contract_info_r207(root)
        except Exception as e:
            logger.error(f"Error extracting contract info: {e}")
            return None

    def _extract_contract_info_r207(self, root) -> Optional[Dict]:
        """Extract contract info for R2.0.7/R2.0.8 formats."""
        # Extract contract title and description
        title_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}TITLE_CONTRACT')
        description_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}SHORT_CONTRACT_DESCRIPTION')

        # Extract CPV codes
        cpv_main_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}CPV_MAIN//{http://publications.europa.eu/TED_schema/Export}CPV_CODE')
        cpv_additional_elems = root.findall('.//{http://publications.europa.eu/TED_schema/Export}CPV_ADDITIONAL//{http://publications.europa.eu/TED_schema/Export}CPV_CODE')

        # Extract contract nature and procedure type
        nature_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}NC_CONTRACT_NATURE')
        procedure_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}PR_PROC')

        return {
            'title': XmlUtils.extract_text(title_elem) if title_elem is not None else '',
            'description': XmlUtils.extract_text(description_elem) if description_elem is not None else None,
            'cpv_code_main': cpv_main_elem.get('CODE') if cpv_main_elem is not None else None,
            'cpv_codes_additional': [elem.get('CODE') for elem in cpv_additional_elems] if cpv_additional_elems else [],
            'contract_nature_code': nature_elem.get('CODE') if nature_elem is not None else None,
            'procedure_type_code': procedure_elem.get('CODE') if procedure_elem is not None else None,
            'eu_funding': False,  # Default, could be extracted if available
        }

    def _extract_contract_info_r209(self, root) -> Optional[Dict]:
        """Extract contract info for R2.0.9 format."""
        # Extract from F03_2014 form
        object_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}F03_2014//{http://publications.europa.eu/TED_schema/Export}OBJECT_CONTRACT')
        if object_elem is None:
            return None

        title_elem = object_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TITLE')
        description_elem = object_elem.find('.//{http://publications.europa.eu/TED_schema/Export}SHORT_DESCR')

        # Extract CPV codes
        cpv_main_elem = object_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CPV_MAIN//{http://publications.europa.eu/TED_schema/Export}CPV_CODE')
        cpv_additional_elems = object_elem.findall('.//{http://publications.europa.eu/TED_schema/Export}CPV_ADDITIONAL//{http://publications.europa.eu/TED_schema/Export}CPV_CODE')

        # Extract contract nature
        type_contract_elem = object_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TYPE_CONTRACT')

        return {
            'title': XmlUtils.extract_text(title_elem) if title_elem is not None else '',
            'description': XmlUtils.extract_text(description_elem) if description_elem is not None else None,
            'cpv_code_main': cpv_main_elem.get('CODE') if cpv_main_elem is not None else None,
            'cpv_codes_additional': [elem.get('CODE') for elem in cpv_additional_elems] if cpv_additional_elems else [],
            'contract_nature_code': type_contract_elem.get('CTYPE') if type_contract_elem is not None else None,
            'procedure_type_code': None,  # Extract from procedure section if needed
            'eu_funding': False,  # Default, could be extracted if available
        }

    def _extract_awards(self, root, variant: str) -> List[Dict]:
        """Extract award information based on variant."""
        try:
            if variant == "R2.0.9":
                return self._extract_awards_r209(root)
            else:  # R2.0.7/R2.0.8
                return self._extract_awards_r207(root)
        except Exception as e:
            logger.error(f"Error extracting awards: {e}")
            return []

    def _extract_awards_r207(self, root) -> List[Dict]:
        """Extract awards for R2.0.7/R2.0.8 formats."""
        awards = []

        # Find all award sections
        award_elems = root.findall('.//{http://publications.europa.eu/TED_schema/Export}AWARD_OF_CONTRACT')

        for award_elem in award_elems:
            # Extract basic award info
            contract_number_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_NUMBER')
            title_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_TITLE')
            award_date_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_AWARD_DATE')

            # Extract award value
            value_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_VALUE_INFORMATION//{http://publications.europa.eu/TED_schema/Export}VALUE_COST')
            currency_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_VALUE_INFORMATION//{http://publications.europa.eu/TED_schema/Export}COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE')

            # Extract number of offers
            offers_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}OFFERS_RECEIVED_NUMBER')

            # Extract contractors
            contractors = self._extract_contractors_r207(award_elem)

            award_data = {
                'contract_number': contract_number_elem.text if contract_number_elem is not None else None,
                'award_title': XmlUtils.extract_text(title_elem) if title_elem is not None else '',
                'conclusion_date': self._parse_award_date(award_date_elem) if award_date_elem is not None else None,
                'awarded_value': self._extract_value_amount(value_elem, currency_elem),
                'currency_code': currency_elem.get('CURRENCY') if currency_elem is not None else None,
                'tenders_received': int(offers_elem.text) if offers_elem is not None and offers_elem.text else None,
                'contractors': contractors
            }

            awards.append(award_data)

        return awards

    def _extract_awards_r209(self, root) -> List[Dict]:
        """Extract awards for R2.0.9 format."""
        awards = []

        # Find all award sections in F03_2014
        award_elems = root.findall('.//{http://publications.europa.eu/TED_schema/Export}F03_2014//{http://publications.europa.eu/TED_schema/Export}AWARD_CONTRACT')

        for award_elem in award_elems:
            # Extract basic award info
            contract_number_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CONTRACT_NO')
            title_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TITLE')

            # Extract award decision info
            award_decision_elem = award_elem.find('.//{http://publications.europa.eu/TED_schema/Export}AWARDED_CONTRACT')
            if award_decision_elem is None:
                continue

            award_date_elem = award_decision_elem.find('.//{http://publications.europa.eu/TED_schema/Export}DATE_CONCLUSION_CONTRACT')

            # Extract value
            value_elem = award_decision_elem.find('.//{http://publications.europa.eu/TED_schema/Export}VAL_TOTAL')

            # Extract number of offers
            offers_elem = award_decision_elem.find('.//{http://publications.europa.eu/TED_schema/Export}NB_TENDERS_RECEIVED')

            # Extract contractors
            contractors = self._extract_contractors_r209(award_decision_elem)

            award_data = {
                'contract_number': contract_number_elem.text if contract_number_elem is not None else None,
                'award_title': XmlUtils.extract_text(title_elem) if title_elem is not None else '',
                'conclusion_date': self._parse_award_date(award_date_elem) if award_date_elem is not None else None,
                'awarded_value': self._extract_value_amount_r209(value_elem),
                'currency_code': value_elem.get('CURRENCY') if value_elem is not None else None,
                'tenders_received': int(offers_elem.text) if offers_elem is not None and offers_elem.text else None,
                'contractors': contractors
            }

            awards.append(award_data)

        return awards

    def _extract_contractors_r207(self, award_elem) -> List[Dict]:
        """Extract contractor information for R2.0.7/R2.0.8."""
        contractors = []

        contractor_elems = award_elem.findall('.//{http://publications.europa.eu/TED_schema/Export}ECONOMIC_OPERATOR_NAME_ADDRESS')

        for contractor_elem in contractor_elems:
            contact_data_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}CONTACT_DATA_WITHOUT_RESPONSIBLE_NAME')
            if contact_data_elem is None:
                continue

            name_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ORGANISATION//{http://publications.europa.eu/TED_schema/Export}OFFICIALNAME')
            address_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ADDRESS')
            town_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TOWN')
            postal_code_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}POSTAL_CODE')
            country_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}COUNTRY')

            contractor_data = {
                'official_name': name_elem.text if name_elem is not None else '',
                'address': address_elem.text if address_elem is not None else None,
                'town': town_elem.text if town_elem is not None else None,
                'postal_code': postal_code_elem.text if postal_code_elem is not None else None,
                'country_code': country_elem.get('VALUE') if country_elem is not None else None,
                'nuts_code': None,  # May not be available in legacy format
            }

            contractors.append(contractor_data)

        return contractors

    def _extract_contractors_r209(self, award_elem) -> List[Dict]:
        """Extract contractor information for R2.0.9."""
        contractors = []

        contractor_elems = award_elem.findall('.//{http://publications.europa.eu/TED_schema/Export}CONTRACTOR')

        for contractor_elem in contractor_elems:
            name_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}OFFICIALNAME')
            address_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ADDRESS')
            town_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TOWN')
            postal_code_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}POSTAL_CODE')
            country_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}COUNTRY')
            nuts_elem = contractor_elem.find('.//{http://publications.europa.eu/TED_schema/Export}NUTS')

            contractor_data = {
                'official_name': name_elem.text if name_elem is not None else '',
                'address': address_elem.text if address_elem is not None else None,
                'town': town_elem.text if town_elem is not None else None,
                'postal_code': postal_code_elem.text if postal_code_elem is not None else None,
                'country_code': country_elem.get('VALUE') if country_elem is not None else None,
                'nuts_code': nuts_elem.get('CODE') if nuts_elem is not None else None,
            }

            contractors.append(contractor_data)

        return contractors

    def _parse_award_date(self, date_elem) -> Optional[str]:
        """Parse award date from various formats."""
        if date_elem is None:
            return None

        # Try to extract day/month/year components
        day_elem = date_elem.find('.//{http://publications.europa.eu/TED_schema/Export}DAY')
        month_elem = date_elem.find('.//{http://publications.europa.eu/TED_schema/Export}MONTH')
        year_elem = date_elem.find('.//{http://publications.europa.eu/TED_schema/Export}YEAR')

        if all(elem is not None for elem in [day_elem, month_elem, year_elem]):
            try:
                day = int(day_elem.text)
                month = int(month_elem.text)
                year = int(year_elem.text)
                return f"{year:04d}-{month:02d}-{day:02d}"
            except (ValueError, TypeError):
                pass

        # Fall back to direct text if available
        if hasattr(date_elem, 'text') and date_elem.text:
            return DateParsingUtils.normalize_date_string(date_elem.text)

        return None

    def _extract_value_amount(self, value_elem, currency_elem) -> Optional[float]:
        """Extract value amount for R2.0.7/R2.0.8."""
        if value_elem is None:
            return None

        try:
            # Try FMTVAL attribute first (numeric format)
            fmtval = value_elem.get('FMTVAL')
            if fmtval:
                return float(fmtval)

            # Fall back to text content, clean up formatting
            if value_elem.text:
                # Remove common formatting characters
                clean_text = value_elem.text.replace(' ', '').replace(',', '.').replace('\u00a0', '')
                # Extract numeric part
                import re
                match = re.search(r'[\d.]+', clean_text)
                if match:
                    return float(match.group())

        except (ValueError, TypeError) as e:
            logger.debug(f"Error parsing value amount: {e}")

        return None

    def _extract_value_amount_r209(self, value_elem) -> Optional[float]:
        """Extract value amount for R2.0.9."""
        if value_elem is None:
            return None

        try:
            # R2.0.9 typically uses direct numeric attributes or text
            if value_elem.text:
                return float(value_elem.text.replace(' ', '').replace(',', ''))
        except (ValueError, TypeError) as e:
            logger.debug(f"Error parsing R2.0.9 value amount: {e}")

        return None