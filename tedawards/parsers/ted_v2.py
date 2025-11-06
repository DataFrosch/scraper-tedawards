"""
TED Version 2.0 parser - unified parser for all TED 2.0 variants.
Handles R2.0.7, R2.0.8, and R2.0.9 formats (2008-2023).
"""

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional
from lxml import etree

from .base import BaseParser
from ..schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)

logger = logging.getLogger(__name__)


class TedV2Parser(BaseParser):
    """Unified parser for all TED 2.0 variants (R2.0.7, R2.0.8, R2.0.9)."""

    def can_parse(self, xml_file: Path) -> bool:
        """Check if this file uses any TED 2.0 format variant."""
        try:
            tree = etree.parse(xml_file)
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
            logger.debug(f"Error checking if {xml_file.name} is TED 2.0 format: {e}")
            return False

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
        # Use namespace-agnostic xpath since R2.0.9 uses different namespace
        lang_orig_elems = root.xpath('.//*[local-name()="LG_ORIG"]/text()')
        return lang_orig_elems[0].strip() if lang_orig_elems else None

    def _is_original_language_version(self, root, original_lang: str) -> bool:
        """Check if this XML document represents the original language version."""
        if not original_lang:
            # Fail loud - original language is required for deduplication
            raise ValueError(f"No original language (LG_ORIG) found in document - cannot determine if this is original or translation")

        # Check the form language - use namespace-agnostic xpath
        form_elems = root.xpath('.//*[local-name()="CONTRACT_AWARD" or local-name()="F03_2014"]')

        if form_elems:
            form_elem = form_elems[0]
            form_lang = form_elem.get('LG')
            form_category = form_elem.get('CATEGORY', '').upper()

            # Original documents should have CATEGORY="ORIGINAL" and matching language
            return (form_category == 'ORIGINAL' and
                    form_lang == original_lang)

        return False

    def _extract_document_info(self, root, xml_file: Path, variant: str) -> Optional[Dict]:
        """Extract document-level information."""
        try:
            # Extract document ID from DOC_ID attribute or filename
            doc_id = root.get('DOC_ID')
            if not doc_id:
                # Fallback to filename-based ID
                doc_id = xml_file.stem.replace('_', '-')

            # Extract edition from root element
            edition = root.get('EDITION')
            if not edition:
                logger.debug(f"No edition found in {xml_file.name}")
                return None

            # Extract publication date - use namespace-agnostic xpath
            pub_date_elems = root.xpath('.//*[local-name()="DATE_PUB"]')
            if not pub_date_elems:
                logger.debug(f"No publication date found in {xml_file.name}")
                return None

            # Parse ISO format date (YYYYMMDD from TED XML)
            try:
                pub_date = date.fromisoformat(pub_date_elems[0].text.strip())
            except (ValueError, AttributeError) as e:
                logger.error(f"Invalid publication date in {xml_file.name}: '{pub_date_elems[0].text}'. Error: {e}")
                raise

            # Extract dispatch date - use namespace-agnostic xpath
            dispatch_date_elems = root.xpath('.//*[local-name()="DS_DATE_DISPATCH"]')
            dispatch_date = None
            if dispatch_date_elems and dispatch_date_elems[0].text:
                try:
                    dispatch_date = date.fromisoformat(dispatch_date_elems[0].text.strip())
                except (ValueError, AttributeError) as e:
                    logger.error(f"Invalid dispatch date in {xml_file.name}: '{dispatch_date_elems[0].text}'. Error: {e}")
                    raise

            # Extract other document metadata - use namespace-agnostic xpath
            reception_id_elems = root.xpath('.//*[local-name()="RECEPTION_ID"]')
            no_doc_oj_elems = root.xpath('.//*[local-name()="NO_DOC_OJS"]')
            country_elems = root.xpath('.//*[local-name()="ISO_COUNTRY"]')
            original_lang_elems = root.xpath('.//*[local-name()="LG_ORIG"]')

            return {
                'doc_id': doc_id,
                'edition': edition,
                'publication_date': pub_date,
                'dispatch_date': dispatch_date,
                'reception_id': reception_id_elems[0].text if reception_id_elems else None,
                'official_journal_ref': no_doc_oj_elems[0].text if no_doc_oj_elems else None,
                'source_country': country_elems[0].get('VALUE') if country_elems else None,
                'original_language': original_lang_elems[0].text if original_lang_elems else None,
                'form_language': original_lang_elems[0].text if original_lang_elems else 'EN',
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
        # R2.0.7 uses CONTACTING_AUTHORITY_INFORMATION
        # R2.0.8 uses CONTRACTING_AUTHORITY_INFORMATION_CONTRACT_AWARD
        ca_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}CA_CE_CONCESSIONAIRE_PROFILE')
        if ca_elem is None:
            return None

        # Extract organization name - handle both R2.0.7 and R2.0.8 structures
        org_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ORGANISATION')
        official_name = ''
        if org_elem is not None:
            # R2.0.8: ORGANISATION > OFFICIALNAME
            officialname_elem = org_elem.find('.//{http://publications.europa.eu/TED_schema/Export}OFFICIALNAME')
            if officialname_elem is not None and officialname_elem.text:
                official_name = officialname_elem.text
            # R2.0.7: ORGANISATION directly contains text
            elif org_elem.text:
                official_name = org_elem.text

        address_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ADDRESS')
        town_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TOWN')
        postal_code_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}POSTAL_CODE')
        country_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}COUNTRY')
        phone_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}PHONE')
        email_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}E_MAIL')
        fax_elem = ca_elem.find('.//{http://publications.europa.eu/TED_schema/Export}FAX')

        # Extract URL from various possible locations
        url_general_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}URL_GENERAL')
        url_buyer_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}URL_BUYER')

        # Extract authority type and activity codes from coded data section
        authority_type_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}AA_AUTHORITY_TYPE')
        activity_elem = root.find('.//{http://publications.europa.eu/TED_schema/Export}MA_MAIN_ACTIVITIES')

        return {
            'official_name': official_name,
            'address': address_elem.text if address_elem is not None else None,
            'town': town_elem.text if town_elem is not None else None,
            'postal_code': postal_code_elem.text if postal_code_elem is not None else None,
            'country_code': country_elem.get('VALUE') if country_elem is not None else None,
            'nuts_code': None,  # May not be available in legacy format
            'contact_point': None,  # Not typically in legacy format
            'phone': phone_elem.text if phone_elem is not None else None,
            'email': email_elem.text if email_elem is not None else None,
            'fax': fax_elem.text if fax_elem is not None else None,
            'url_general': url_general_elem.text if url_general_elem is not None else None,
            'url_buyer': url_buyer_elem.text if url_buyer_elem is not None else None,
            'authority_type_code': authority_type_elem.get('CODE') if authority_type_elem is not None else None,
            'main_activity_code': activity_elem.get('CODE') if activity_elem is not None else None
        }

    def _extract_contracting_body_r209(self, root) -> Optional[Dict]:
        """Extract contracting body for R2.0.9 format."""
        # Find contracting authority in R2.0.9 format - use namespace-agnostic xpath
        ca_elems = root.xpath('.//*[local-name()="F03_2014"]//*[local-name()="CONTRACTING_BODY"]')
        if not ca_elems:
            return None

        ca_elem = ca_elems[0]

        # Extract basic info - use namespace-agnostic xpath
        name_elems = ca_elem.xpath('.//*[local-name()="OFFICIALNAME"]')
        address_elems = ca_elem.xpath('.//*[local-name()="ADDRESS"]')
        town_elems = ca_elem.xpath('.//*[local-name()="TOWN"]')
        postal_code_elems = ca_elem.xpath('.//*[local-name()="POSTAL_CODE"]')
        country_elems = ca_elem.xpath('.//*[local-name()="COUNTRY"]')

        # Contact info
        contact_elems = ca_elem.xpath('.//*[local-name()="CONTACT_POINT"]')
        phone_elems = ca_elem.xpath('.//*[local-name()="PHONE"]')
        email_elems = ca_elem.xpath('.//*[local-name()="E_MAIL"]')
        fax_elems = ca_elem.xpath('.//*[local-name()="FAX"]')

        # URLs
        url_general_elems = ca_elem.xpath('.//*[local-name()="URL_GENERAL"]')
        url_buyer_elems = ca_elem.xpath('.//*[local-name()="URL_BUYER"]')

        # Authority type and activity
        authority_type_elems = ca_elem.xpath('.//*[local-name()="CA_TYPE"]')
        activity_elems = ca_elem.xpath('.//*[local-name()="CA_ACTIVITY"]')

        return {
            'official_name': name_elems[0].text if name_elems and name_elems[0].text else '',
            'address': address_elems[0].text if address_elems and address_elems[0].text else None,
            'town': town_elems[0].text if town_elems and town_elems[0].text else None,
            'postal_code': postal_code_elems[0].text if postal_code_elems and postal_code_elems[0].text else None,
            'country_code': country_elems[0].get('VALUE') if country_elems else None,
            'nuts_code': None,  # Extract from NUTS if needed
            'contact_point': contact_elems[0].text if contact_elems and contact_elems[0].text else None,
            'phone': phone_elems[0].text if phone_elems and phone_elems[0].text else None,
            'email': email_elems[0].text if email_elems and email_elems[0].text else None,
            'fax': fax_elems[0].text if fax_elems and fax_elems[0].text else None,
            'url_general': url_general_elems[0].text if url_general_elems and url_general_elems[0].text else None,
            'url_buyer': url_buyer_elems[0].text if url_buyer_elems and url_buyer_elems[0].text else None,
            'authority_type_code': authority_type_elems[0].get('VALUE') if authority_type_elems else None,
            'main_activity_code': activity_elems[0].get('VALUE') if activity_elems else None
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
            'title': ''.join(title_elem.itertext()).strip() if title_elem is not None else '',
            'description': ''.join(description_elem.itertext()).strip() if description_elem is not None else None,
            'main_cpv_code': cpv_main_elem.get('CODE') if cpv_main_elem is not None else None,
            'cpv_codes_additional': [elem.get('CODE') for elem in cpv_additional_elems] if cpv_additional_elems else [],
            'contract_nature_code': nature_elem.get('CODE') if nature_elem is not None else None,
            'procedure_type_code': procedure_elem.get('CODE') if procedure_elem is not None else None,
            'eu_funding': False,  # Default, could be extracted if available
        }

    def _extract_contract_info_r209(self, root) -> Optional[Dict]:
        """Extract contract info for R2.0.9 format."""
        # Extract from F03_2014 form - use namespace-agnostic xpath
        object_elems = root.xpath('.//*[local-name()="F03_2014"]//*[local-name()="OBJECT_CONTRACT"]')
        if not object_elems:
            return None

        object_elem = object_elems[0]

        title_elems = object_elem.xpath('.//*[local-name()="TITLE"]')
        description_elems = object_elem.xpath('.//*[local-name()="SHORT_DESCR"]')

        # Extract CPV codes
        cpv_main_elems = object_elem.xpath('.//*[local-name()="CPV_MAIN"]//*[local-name()="CPV_CODE"]')
        cpv_additional_elems = object_elem.xpath('.//*[local-name()="CPV_ADDITIONAL"]//*[local-name()="CPV_CODE"]')

        # Extract contract nature
        type_contract_elems = object_elem.xpath('.//*[local-name()="TYPE_CONTRACT"]')

        return {
            'title': ''.join(title_elems[0].itertext()).strip() if title_elems else '',
            'description': ''.join(description_elems[0].itertext()).strip() if description_elems else None,
            'main_cpv_code': cpv_main_elems[0].get('CODE') if cpv_main_elems else None,
            'cpv_codes_additional': [elem.get('CODE') for elem in cpv_additional_elems] if cpv_additional_elems else [],
            'contract_nature_code': type_contract_elems[0].get('CTYPE') if type_contract_elems else None,
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
                'award_title': ''.join(title_elem.itertext()).strip() if title_elem is not None else '',
                'conclusion_date': self._parse_award_date(award_date_elem) if award_date_elem is not None else None,
                'awarded_value': self._extract_value_amount(value_elem, currency_elem),
                'awarded_value_currency': currency_elem.get('CURRENCY') if currency_elem is not None else None,
                'tenders_received': int(offers_elem.text) if offers_elem is not None and offers_elem.text else None,
                'contractors': contractors
            }

            awards.append(award_data)

        return awards

    def _extract_awards_r209(self, root) -> List[Dict]:
        """Extract awards for R2.0.9 format."""
        awards = []

        # Find all award sections in F03_2014 - use namespace-agnostic xpath
        award_elems = root.xpath('.//*[local-name()="F03_2014"]//*[local-name()="AWARD_CONTRACT"]')

        for award_elem in award_elems:
            # Extract basic award info
            contract_number_elems = award_elem.xpath('.//*[local-name()="CONTRACT_NO"]')
            title_elems = award_elem.xpath('.//*[local-name()="TITLE"]')

            # Extract award decision info
            award_decision_elems = award_elem.xpath('.//*[local-name()="AWARDED_CONTRACT"]')
            if not award_decision_elems:
                continue

            award_decision_elem = award_decision_elems[0]

            award_date_elems = award_decision_elem.xpath('.//*[local-name()="DATE_CONCLUSION_CONTRACT"]')

            # Extract value
            value_elems = award_decision_elem.xpath('.//*[local-name()="VAL_TOTAL"]')

            # Extract number of offers
            offers_elems = award_decision_elem.xpath('.//*[local-name()="NB_TENDERS_RECEIVED"]')

            # Extract contractors
            contractors = self._extract_contractors_r209(award_decision_elem)

            award_data = {
                'contract_number': contract_number_elems[0].text if contract_number_elems and contract_number_elems[0].text else None,
                'award_title': ''.join(title_elems[0].itertext()).strip() if title_elems else '',
                'conclusion_date': self._parse_award_date(award_date_elems[0]) if award_date_elems else None,
                'awarded_value': self._extract_value_amount_r209(value_elems[0]) if value_elems else None,
                'awarded_value_currency': value_elems[0].get('CURRENCY') if value_elems else None,
                'tenders_received': int(offers_elems[0].text) if offers_elems and offers_elems[0].text else None,
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

            # Extract organization name - handle both R2.0.7 and R2.0.8 structures
            org_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ORGANISATION')
            official_name = ''
            if org_elem is not None:
                # R2.0.8: ORGANISATION > OFFICIALNAME
                officialname_elem = org_elem.find('.//{http://publications.europa.eu/TED_schema/Export}OFFICIALNAME')
                if officialname_elem is not None and officialname_elem.text:
                    official_name = officialname_elem.text
                # R2.0.7: ORGANISATION directly contains text
                elif org_elem.text:
                    official_name = org_elem.text

            address_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}ADDRESS')
            town_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}TOWN')
            postal_code_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}POSTAL_CODE')
            country_elem = contact_data_elem.find('.//{http://publications.europa.eu/TED_schema/Export}COUNTRY')

            contractor_data = {
                'official_name': official_name,
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

        # Use namespace-agnostic xpath
        contractor_elems = award_elem.xpath('.//*[local-name()="CONTRACTOR"]')

        for contractor_elem in contractor_elems:
            name_elems = contractor_elem.xpath('.//*[local-name()="OFFICIALNAME"]')
            address_elems = contractor_elem.xpath('.//*[local-name()="ADDRESS"]')
            town_elems = contractor_elem.xpath('.//*[local-name()="TOWN"]')
            postal_code_elems = contractor_elem.xpath('.//*[local-name()="POSTAL_CODE"]')
            country_elems = contractor_elem.xpath('.//*[local-name()="COUNTRY"]')
            nuts_elems = contractor_elem.xpath('.//*[local-name()="NUTS"]')

            contractor_data = {
                'official_name': name_elems[0].text if name_elems and name_elems[0].text else '',
                'address': address_elems[0].text if address_elems and address_elems[0].text else None,
                'town': town_elems[0].text if town_elems and town_elems[0].text else None,
                'postal_code': postal_code_elems[0].text if postal_code_elems and postal_code_elems[0].text else None,
                'country_code': country_elems[0].get('VALUE') if country_elems else None,
                'nuts_code': nuts_elems[0].get('CODE') if nuts_elems else None,
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
            try:
                return date.fromisoformat(date_elem.text.strip())
            except (ValueError, AttributeError) as e:
                logger.error(f"Invalid date: '{date_elem.text}'. Error: {e}")
                raise

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