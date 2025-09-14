import logging
from pathlib import Path
from typing import Dict, List, Optional
from lxml import etree
from datetime import datetime

from .base import BaseParser
from ..schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel,
    normalize_date_string
)

logger = logging.getLogger(__name__)

class TedXmlParser(BaseParser):
    """Parse TED R2.0.9 XML files and extract award notice data."""

    def can_parse(self, xml_file: Path) -> bool:
        """Check if this is a TED R2.0.9 format file."""
        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read(1000)  # Read first 1KB
            return 'TED_EXPORT' in content and 'ted/R2.0.9' in content
        except Exception:
            return False

    def get_format_name(self) -> str:
        """Return format name."""
        return "TED R2.0.9"

    def parse_xml_file(self, xml_path: Path) -> Optional[TedParserResultModel]:
        """Parse a single TED XML file and return structured data."""
        try:
            tree = etree.parse(xml_path)
            root = tree.getroot()

            # Check if this is an award notice (document type 7)
            doc_type = root.xpath('//*[local-name()="TD_DOCUMENT_TYPE"]/@CODE')
            if not doc_type or doc_type[0] != '7':
                logger.debug(f"Skipping {xml_path.name} - not an award notice")
                return None

            award_data = self._extract_award_data(root)
            if award_data:
                return TedParserResultModel(awards=[award_data])
            return None

        except Exception as e:
            logger.error(f"Error parsing {xml_path}: {e}")
            return None

    def _extract_award_data(self, root) -> Optional[TedAwardDataModel]:
        """Extract structured data from award notice XML."""
        try:
            # Document metadata
            document = self._extract_document_info(root)

            # Contracting body
            contracting_body = self._extract_contracting_body(root)

            # Contract details
            contract = self._extract_contract_info(root)

            # Award information
            awards = self._extract_award_info(root)

            return TedAwardDataModel(
                document=document,
                contracting_body=contracting_body,
                contract=contract,
                awards=awards
            )
        except Exception as e:
            logger.error(f"Error creating award data model: {e}")
            return None

    def _extract_document_info(self, root) -> DocumentModel:
        """Extract document metadata."""
        return DocumentModel(
            doc_id=root.get('DOC_ID') or '',
            edition=root.get('EDITION'),
            version=root.get('VERSION'),
            reception_id=self._get_text(root, '//*[local-name()="RECEPTION_ID"]'),
            deletion_date=normalize_date_string(self._get_text(root, '//*[local-name()="DELETION_DATE"]')),
            form_language=self._get_text(root, '//*[local-name()="FORM_LG_LIST"]', '').strip(),
            official_journal_ref=self._get_text(root, '//*[local-name()="NO_DOC_OJS"]'),
            publication_date=normalize_date_string(self._get_text(root, '//*[local-name()="DATE_PUB"]')),
            dispatch_date=normalize_date_string(self._get_text(root, '//*[local-name()="DS_DATE_DISPATCH"]')),
            original_language=self._get_text(root, '//*[local-name()="LG_ORIG"]'),
            source_country=self._get_attr(root, '//*[local-name()="ISO_COUNTRY"]', 'VALUE')
        )

    def _extract_contracting_body(self, root) -> ContractingBodyModel:
        """Extract contracting body information."""
        cb_xpath = '//*[local-name()="ADDRESS_CONTRACTING_BODY"]'
        return ContractingBodyModel(
            official_name=self._get_text(root, f'{cb_xpath}//*[local-name()="OFFICIALNAME"]') or '',
            address=self._get_text(root, f'{cb_xpath}//*[local-name()="ADDRESS"]'),
            town=self._get_text(root, f'{cb_xpath}//*[local-name()="TOWN"]'),
            postal_code=self._get_text(root, f'{cb_xpath}//*[local-name()="POSTAL_CODE"]'),
            country_code=self._get_attr(root, f'{cb_xpath}//*[local-name()="COUNTRY"]', 'VALUE'),
            nuts_code=self._get_attr(root, f'{cb_xpath}//*[local-name()="NUTS"]', 'CODE'),
            contact_point=self._get_text(root, f'{cb_xpath}//*[local-name()="CONTACT_POINT"]'),
            phone=self._get_text(root, f'{cb_xpath}//*[local-name()="PHONE"]'),
            email=self._get_text(root, f'{cb_xpath}//*[local-name()="E_MAIL"]'),
            fax=self._get_text(root, f'{cb_xpath}//*[local-name()="FAX"]'),
            url_general=self._get_text(root, f'{cb_xpath}//*[local-name()="URL_GENERAL"]'),
            url_buyer=self._get_text(root, f'{cb_xpath}//*[local-name()="URL_BUYER"]'),
            authority_type_code=self._get_attr(root, '//*[local-name()="AA_AUTHORITY_TYPE"]', 'CODE'),
            main_activity_code=self._get_attr(root, '//*[local-name()="MA_MAIN_ACTIVITIES"]', 'CODE')
        )

    def _extract_contract_info(self, root) -> ContractModel:
        """Extract contract information."""
        return ContractModel(
            title=self._get_text(root, '//*[local-name()="OBJECT_CONTRACT"]//*[local-name()="TITLE"]//*[local-name()="P"]') or '',
            reference_number=self._get_text(root, '//*[local-name()="REFERENCE_NUMBER"]'),
            short_description=self._get_multiline_text(root, '//*[local-name()="OBJECT_CONTRACT"]//*[local-name()="SHORT_DESCR"]//*[local-name()="P"]'),
            main_cpv_code=self._get_attr(root, '//*[local-name()="CPV_MAIN"]//*[local-name()="CPV_CODE"]', 'CODE'),
            contract_nature_code=self._get_attr(root, '//*[local-name()="NC_CONTRACT_NATURE"]', 'CODE'),
            total_value=self._get_decimal(root, '//*[local-name()="VAL_TOTAL"]'),
            total_value_currency=self._get_attr(root, '//*[local-name()="VAL_TOTAL"]', 'CURRENCY'),
            procedure_type_code=self._get_attr(root, '//*[local-name()="PR_PROC"]', 'CODE'),
            award_criteria_code=self._get_attr(root, '//*[local-name()="AC_AWARD_CRIT"]', 'CODE'),
            performance_nuts_code=self._get_attr(root, '//*[local-name()="PERFORMANCE_NUTS"]', 'CODE')
        )

    def _extract_award_info(self, root) -> List[AwardModel]:
        """Extract award information."""
        awards = []

        for award_elem in root.xpath('//*[local-name()="AWARD_CONTRACT"]'):
            contractors = self._extract_contractors(award_elem)

            award_data = AwardModel(
                award_title=self._get_text(award_elem, './/*[local-name()="TITLE"]//*[local-name()="P"]'),
                conclusion_date=normalize_date_string(self._get_text(award_elem, './/*[local-name()="DATE_CONCLUSION_CONTRACT"]')),
                contract_number=self._get_text(award_elem, './/*[local-name()="CONTRACT_NUMBER"]'),

                # Tender statistics
                tenders_received=self._get_int(award_elem, './/*[local-name()="NB_TENDERS_RECEIVED"]'),
                tenders_received_sme=self._get_int(award_elem, './/*[local-name()="NB_TENDERS_RECEIVED_SME"]'),
                tenders_received_other_eu=self._get_int(award_elem, './/*[local-name()="NB_TENDERS_RECEIVED_OTHER_EU"]'),
                tenders_received_non_eu=self._get_int(award_elem, './/*[local-name()="NB_TENDERS_RECEIVED_NON_EU"]'),
                tenders_received_electronic=self._get_int(award_elem, './/*[local-name()="NB_TENDERS_RECEIVED_EMEANS"]'),

                # Award value
                awarded_value=self._get_decimal(award_elem, './/*[local-name()="VAL_TOTAL"]'),
                awarded_value_currency=self._get_attr(award_elem, './/*[local-name()="VAL_TOTAL"]', 'CURRENCY'),

                # Subcontracting
                subcontracted_value=self._get_decimal(award_elem, './/*[local-name()="VAL_SUBCONTRACTING"]'),
                subcontracted_value_currency=self._get_attr(award_elem, './/*[local-name()="VAL_SUBCONTRACTING"]', 'CURRENCY'),
                subcontracting_description=self._get_text(award_elem, './/*[local-name()="INFO_ADD_SUBCONTRACTING"]//*[local-name()="P"]'),

                # Contractors
                contractors=contractors
            )
            awards.append(award_data)

        return awards

    def _extract_contractors(self, award_elem) -> List[ContractorModel]:
        """Extract contractor information from award element."""
        contractors = []

        for contractor_elem in award_elem.xpath('.//*[local-name()="CONTRACTOR"]'):
            addr_elems = contractor_elem.xpath('.//*[local-name()="ADDRESS_CONTRACTOR"]')
            if addr_elems:
                addr_elem = addr_elems[0]
                official_name = self._get_text(addr_elem, './/*[local-name()="OFFICIALNAME"]')

                if official_name:  # Only create contractor if we have a name
                    contractor_data = ContractorModel(
                        official_name=official_name,
                        address=self._get_text(addr_elem, './/*[local-name()="ADDRESS"]'),
                        town=self._get_text(addr_elem, './/*[local-name()="TOWN"]'),
                        postal_code=self._get_text(addr_elem, './/*[local-name()="POSTAL_CODE"]'),
                        country_code=self._get_attr(addr_elem, './/*[local-name()="COUNTRY"]', 'VALUE'),
                        nuts_code=self._get_attr(addr_elem, './/*[local-name()="NUTS"]', 'CODE'),
                        phone=self._get_text(addr_elem, './/*[local-name()="PHONE"]'),
                        email=self._get_text(addr_elem, './/*[local-name()="E_MAIL"]'),
                        fax=self._get_text(addr_elem, './/*[local-name()="FAX"]'),
                        url=self._get_text(addr_elem, './/*[local-name()="URL"]'),
                        is_sme=len(contractor_elem.xpath('.//*[local-name()="SME"]')) > 0
                    )
                    contractors.append(contractor_data)

        # If no contractors found, return empty list

        return contractors

    def _get_text(self, elem, xpath, default=''):
        """Get text content from xpath."""
        result = elem.xpath(xpath)
        return result[0].text if result and result[0].text else default

    def _get_attr(self, elem, xpath, attr, default=''):
        """Get attribute value from xpath."""
        result = elem.xpath(xpath)
        return result[0].get(attr, default) if result else default

    def _get_multiline_text(self, elem, xpath):
        """Get concatenated text from multiple P elements."""
        results = elem.xpath(xpath)
        return ' '.join(p.text for p in results if p.text).strip()

    def _get_int(self, elem, xpath, default=None):
        """Get integer value from xpath."""
        text = self._get_text(elem, xpath)
        return int(text) if text and text.isdigit() else default

    def _get_decimal(self, elem, xpath, default=None):
        """Get decimal value from xpath."""
        text = self._get_text(elem, xpath)
        try:
            return float(text) if text else default
        except ValueError:
            return default

