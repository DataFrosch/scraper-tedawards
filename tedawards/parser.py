import logging
from pathlib import Path
from typing import Dict, List, Optional
from lxml import etree
from datetime import datetime

logger = logging.getLogger(__name__)

class TedXmlParser:
    """Parse TED XML files and extract award notice data."""

    NAMESPACES = {
        'ted': 'http://publications.europa.eu/resource/schema/ted/R2.0.9/publication',
        'n2021': 'http://publications.europa.eu/resource/schema/ted/2021/nuts'
    }

    def parse_xml_file(self, xml_path: Path) -> Optional[Dict]:
        """Parse a single TED XML file and return structured data."""
        try:
            tree = etree.parse(xml_path)
            root = tree.getroot()

            # Check if this is an award notice (document type 7)
            doc_type = root.xpath('//TD_DOCUMENT_TYPE/@CODE')
            if not doc_type or doc_type[0] != '7':
                logger.debug(f"Skipping {xml_path.name} - not an award notice")
                return None

            return self._extract_award_data(root)

        except Exception as e:
            logger.error(f"Error parsing {xml_path}: {e}")
            return None

    def _extract_award_data(self, root) -> Dict:
        """Extract structured data from award notice XML."""
        data = {}

        # Document metadata
        data['document'] = self._extract_document_info(root)

        # Contracting body
        data['contracting_body'] = self._extract_contracting_body(root)

        # Contract details
        data['contract'] = self._extract_contract_info(root)

        # Award information
        data['awards'] = self._extract_award_info(root)

        return data

    def _extract_document_info(self, root) -> Dict:
        """Extract document metadata."""
        return {
            'doc_id': root.get('DOC_ID'),
            'edition': root.get('EDITION'),
            'version': root.get('VERSION'),
            'reception_id': self._get_text(root, './/RECEPTION_ID'),
            'deletion_date': self._parse_date(self._get_text(root, './/DELETION_DATE')),
            'form_language': self._get_text(root, './/FORM_LG_LIST', '').strip(),
            'official_journal_ref': self._get_text(root, './/NO_DOC_OJS'),
            'publication_date': self._parse_date(self._get_text(root, './/DATE_PUB')),
            'dispatch_date': self._parse_date(self._get_text(root, './/DS_DATE_DISPATCH')),
            'original_language': self._get_text(root, './/LG_ORIG'),
            'source_country': self._get_attr(root, './/ISO_COUNTRY', 'VALUE')
        }

    def _extract_contracting_body(self, root) -> Dict:
        """Extract contracting body information."""
        cb_xpath = './/CONTRACTING_BODY/ADDRESS_CONTRACTING_BODY'
        return {
            'official_name': self._get_text(root, f'{cb_xpath}/OFFICIALNAME'),
            'address': self._get_text(root, f'{cb_xpath}/ADDRESS'),
            'town': self._get_text(root, f'{cb_xpath}/TOWN'),
            'postal_code': self._get_text(root, f'{cb_xpath}/POSTAL_CODE'),
            'country_code': self._get_attr(root, f'{cb_xpath}/COUNTRY', 'VALUE'),
            'nuts_code': self._get_attr(root, f'{cb_xpath}/n2021:NUTS', 'CODE'),
            'contact_point': self._get_text(root, f'{cb_xpath}/CONTACT_POINT'),
            'phone': self._get_text(root, f'{cb_xpath}/PHONE'),
            'email': self._get_text(root, f'{cb_xpath}/E_MAIL'),
            'fax': self._get_text(root, f'{cb_xpath}/FAX'),
            'url_general': self._get_text(root, f'{cb_xpath}/URL_GENERAL'),
            'url_buyer': self._get_text(root, f'{cb_xpath}/URL_BUYER'),
            'authority_type_code': self._get_attr(root, './/AA_AUTHORITY_TYPE', 'CODE'),
            'main_activity_code': self._get_attr(root, './/MA_MAIN_ACTIVITIES', 'CODE')
        }

    def _extract_contract_info(self, root) -> Dict:
        """Extract contract information."""
        return {
            'title': self._get_text(root, './/OBJECT_CONTRACT/TITLE/P'),
            'reference_number': self._get_text(root, './/REFERENCE_NUMBER'),
            'short_description': self._get_multiline_text(root, './/OBJECT_CONTRACT/SHORT_DESCR/P'),
            'main_cpv_code': self._get_attr(root, './/CPV_MAIN/CPV_CODE', 'CODE'),
            'contract_nature_code': self._get_attr(root, './/NC_CONTRACT_NATURE', 'CODE'),
            'total_value': self._get_decimal(root, './/VAL_TOTAL'),
            'total_value_currency': self._get_attr(root, './/VAL_TOTAL', 'CURRENCY'),
            'procedure_type_code': self._get_attr(root, './/PR_PROC', 'CODE'),
            'award_criteria_code': self._get_attr(root, './/AC_AWARD_CRIT', 'CODE'),
            'performance_nuts_code': self._get_attr(root, './/n2021:PERFORMANCE_NUTS', 'CODE')
        }

    def _extract_award_info(self, root) -> List[Dict]:
        """Extract award information."""
        awards = []

        for award_elem in root.xpath('.//AWARD_CONTRACT'):
            award_data = {
                'award_title': self._get_text(award_elem, './/TITLE/P'),
                'conclusion_date': self._parse_date(self._get_text(award_elem, './/DATE_CONCLUSION_CONTRACT')),
                'contract_number': self._get_text(award_elem, './/CONTRACT_NUMBER'),

                # Tender statistics
                'tenders_received': self._get_int(award_elem, './/NB_TENDERS_RECEIVED'),
                'tenders_received_sme': self._get_int(award_elem, './/NB_TENDERS_RECEIVED_SME'),
                'tenders_received_other_eu': self._get_int(award_elem, './/NB_TENDERS_RECEIVED_OTHER_EU'),
                'tenders_received_non_eu': self._get_int(award_elem, './/NB_TENDERS_RECEIVED_NON_EU'),
                'tenders_received_electronic': self._get_int(award_elem, './/NB_TENDERS_RECEIVED_EMEANS'),

                # Award value
                'awarded_value': self._get_decimal(award_elem, './/VAL_TOTAL'),
                'awarded_value_currency': self._get_attr(award_elem, './/VAL_TOTAL', 'CURRENCY'),

                # Subcontracting
                'subcontracted_value': self._get_decimal(award_elem, './/VAL_SUBCONTRACTING'),
                'subcontracted_value_currency': self._get_attr(award_elem, './/VAL_SUBCONTRACTING', 'CURRENCY'),
                'subcontracting_description': self._get_text(award_elem, './/INFO_ADD_SUBCONTRACTING/P'),

                # Contractors
                'contractors': self._extract_contractors(award_elem)
            }
            awards.append(award_data)

        return awards

    def _extract_contractors(self, award_elem) -> List[Dict]:
        """Extract contractor information from award element."""
        contractors = []

        for contractor_elem in award_elem.xpath('.//CONTRACTOR'):
            addr_elem = contractor_elem.find('.//ADDRESS_CONTRACTOR')
            if addr_elem is not None:
                contractor_data = {
                    'official_name': self._get_text(addr_elem, './/OFFICIALNAME'),
                    'address': self._get_text(addr_elem, './/ADDRESS'),
                    'town': self._get_text(addr_elem, './/TOWN'),
                    'postal_code': self._get_text(addr_elem, './/POSTAL_CODE'),
                    'country_code': self._get_attr(addr_elem, './/COUNTRY', 'VALUE'),
                    'nuts_code': self._get_attr(addr_elem, './/n2021:NUTS', 'CODE'),
                    'phone': self._get_text(addr_elem, './/PHONE'),
                    'email': self._get_text(addr_elem, './/E_MAIL'),
                    'fax': self._get_text(addr_elem, './/FAX'),
                    'url': self._get_text(addr_elem, './/URL'),
                    'is_sme': contractor_elem.find('.//SME') is not None
                }
                contractors.append(contractor_data)

        return contractors

    def _get_text(self, elem, xpath, default=''):
        """Get text content from xpath."""
        result = elem.xpath(xpath, namespaces=self.NAMESPACES)
        return result[0].text if result and result[0].text else default

    def _get_attr(self, elem, xpath, attr, default=''):
        """Get attribute value from xpath."""
        result = elem.xpath(xpath, namespaces=self.NAMESPACES)
        return result[0].get(attr, default) if result else default

    def _get_multiline_text(self, elem, xpath):
        """Get concatenated text from multiple P elements."""
        results = elem.xpath(xpath, namespaces=self.NAMESPACES)
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

    def _parse_date(self, date_str):
        """Parse date string to date object."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            return None