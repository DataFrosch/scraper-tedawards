import logging
from pathlib import Path
from typing import Dict, List, Optional
from lxml import etree
from datetime import datetime

from .base import BaseParser

logger = logging.getLogger(__name__)

class EFormsUBLParser(BaseParser):
    """Parse eForms UBL ContractAwardNotice XML files and extract award notice data."""

    def can_parse(self, xml_file: Path) -> bool:
        """Check if this is an eForms UBL ContractAwardNotice format file."""
        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read(1000)  # Read first 1KB
            return ('ContractAwardNotice' in content and
                    'urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2' in content)
        except Exception:
            return False

    def get_format_name(self) -> str:
        """Return format name."""
        return "eForms UBL ContractAwardNotice"

    def parse_xml_file(self, xml_path: Path) -> Optional[Dict]:
        """Parse an eForms UBL XML file and return structured data."""
        try:
            tree = etree.parse(xml_path)
            root = tree.getroot()

            # Define namespaces used in eForms
            namespaces = {
                'can': 'urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2',
                'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
                'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
                'efac': 'http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1',
                'efbc': 'http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1',
                'efext': 'http://data.europa.eu/p27/eforms-ubl-extensions/1',
                'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2'
            }

            # Extract basic document information
            doc_data = self._extract_document_data(root, namespaces, xml_path)
            if not doc_data:
                return None

            # Extract contracting body
            contracting_body = self._extract_contracting_body(root, namespaces)
            if not contracting_body:
                return None

            # Extract contract information
            contract = self._extract_contract(root, namespaces)
            if not contract:
                return None

            # Extract awards
            awards = self._extract_awards(root, namespaces)
            if not awards:
                return None

            return {
                'document': doc_data,
                'contracting_body': contracting_body,
                'contract': contract,
                'awards': awards
            }

        except Exception as e:
            logger.error(f"Error parsing eForms UBL file {xml_path}: {e}")
            return None

    def _extract_document_data(self, root, ns, xml_file: Path) -> Optional[Dict]:
        """Extract document metadata from eForms UBL."""
        try:
            # Extract document ID from filename (more reliable than internal IDs)
            doc_id = xml_file.stem
            # Handle both _2024 and _2025 patterns
            if '_2024' in doc_id:
                doc_id = doc_id.replace('_2024', '-2024')
            elif '_2025' in doc_id:
                doc_id = doc_id.replace('_2025', '-2025')
            else:
                # Generic pattern for future years
                import re
                doc_id = re.sub(r'_(\d{4})$', r'-\1', doc_id)

            # Extract publication date from various possible locations
            pub_date_str = (
                self._get_text(root, './/efac:Publication/efbc:PublicationDate', ns) or
                self._get_text(root, './/cbc:IssueDate', ns) or
                self._get_text(root, './/efac:SettledContract/cbc:IssueDate', ns) or
                self._get_text(root, './/cac:ContractAwardNotice/cbc:IssueDate', ns)
            )

            # Parse the publication date
            from datetime import date
            pub_date = self._parse_date(pub_date_str) if pub_date_str else None

            # If no publication date found, this is an error - don't fallback to current date
            if pub_date is None:
                logger.error(f"No publication date found in eForms document {xml_file}")
                return None

            # Extract language - look for most common language in the document
            language_elements = root.xpath('.//*[@languageID]', namespaces=ns)
            languages = [elem.get('languageID') for elem in language_elements if elem.get('languageID')]
            language = max(set(languages), key=languages.count) if languages else 'EN'

            # Extract sender country
            countries = root.xpath('.//cac:Country/cbc:IdentificationCode/text()', namespaces=ns)
            country = countries[0] if countries else ''

            # Create official journal reference
            year = pub_date.year if hasattr(pub_date, 'year') else 2024
            # Extract day of year for journal numbering
            day_of_year = pub_date.timetuple().tm_yday if hasattr(pub_date, 'timetuple') else 1
            official_ref = f"{year}/S {day_of_year:03d}-{doc_id}"

            return {
                'doc_id': doc_id,
                'edition': f"{year}{day_of_year:03d}",
                'version': 'eForms-UBL',
                'reception_id': '',
                'deletion_date': None,
                'form_language': language,
                'official_journal_ref': official_ref,
                'publication_date': pub_date,
                'dispatch_date': pub_date,
                'original_language': language,
                'source_country': country
            }
        except Exception as e:
            logger.error(f"Error extracting document data: {e}")
            return None

    def _extract_contracting_body(self, root, ns) -> Optional[Dict]:
        """Extract contracting body information from eForms UBL."""
        try:
            # Find the contracting party organization ID from the main document structure
            contracting_party_id = self._get_text(root, './/cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID', ns)

            if not contracting_party_id:
                # Fallback to first organization if no contracting party specified
                orgs = root.xpath('.//efac:Organizations/efac:Organization', namespaces=ns)
                if orgs:
                    contracting_body = orgs[0].find('.//efac:Company', ns)
                else:
                    return None
            else:
                # Find the organization with the matching ID
                contracting_body = None
                orgs = root.xpath('.//efac:Organizations/efac:Organization', namespaces=ns)
                for org in orgs:
                    company = org.find('.//efac:Company', ns)
                    if company is not None:
                        org_id = self._get_text(company, './/cac:PartyIdentification/cbc:ID', ns)
                        if org_id == contracting_party_id:
                            contracting_body = company
                            break

            if contracting_body is None:
                return None

            return {
                'official_name': self._get_text(contracting_body, './/cac:PartyName/cbc:Name', ns) or '',
                'national_id': self._get_text(contracting_body, './/cac:PartyLegalEntity/cbc:CompanyID', ns),
                'address': self._get_text(contracting_body, './/cac:PostalAddress/cbc:StreetName', ns),
                'town': self._get_text(contracting_body, './/cac:PostalAddress/cbc:CityName', ns),
                'postal_code': self._get_text(contracting_body, './/cac:PostalAddress/cbc:PostalZone', ns),
                'country_code': self._get_text(contracting_body, './/cac:PostalAddress/cac:Country/cbc:IdentificationCode', ns),
                'nuts_code': '',  # TODO: Extract NUTS if available
                'contact_point': '',
                'phone': self._get_text(contracting_body, './/cac:Contact/cbc:Telephone', ns),
                'email': self._get_text(contracting_body, './/cac:Contact/cbc:ElectronicMail', ns),
                'fax': '',
                'url_general': self._get_text(contracting_body, './/cbc:WebsiteURI', ns),
                'url_buyer': '',
                'authority_type_code': '',
                'main_activity_code': ''
            }
        except Exception as e:
            logger.error(f"Error extracting contracting body: {e}")
            return None

    def _extract_contract(self, root, ns) -> Optional[Dict]:
        """Extract contract information from eForms UBL."""
        try:
            # Get contract title from settled contract
            title = self._get_text(root, './/efac:SettledContract/cbc:Title', ns) or ''

            # Get contract reference
            ref_number = self._get_text(root, './/efac:SettledContract/efac:ContractReference/cbc:ID', ns)

            # Get total value
            total_amount = root.xpath('.//efac:NoticeResult/cbc:TotalAmount', namespaces=ns)
            total_value = None
            total_currency = ''
            if total_amount:
                total_value = self._get_decimal_from_text(total_amount[0].text)
                total_currency = total_amount[0].get('currencyID', '')

            # Extract main CPV code
            main_cpv = self._get_text(root, './/cac:ProcurementProject/cac:MainCommodityClassification/cbc:ItemClassificationCode', ns)

            # Extract contract nature
            contract_nature_code = self._get_text(root, './/cac:ProcurementProject/cbc:ProcurementTypeCode', ns)

            # Extract procedure type
            procedure_type_code = self._get_text(root, './/cac:TenderingProcess/cbc:ProcedureCode', ns)

            # Extract performance NUTS code
            nuts_code = self._get_text(root, './/cac:ProcurementProject/cac:RealizedLocation/cac:Address/cbc:CountrySubentityCode', ns)

            return {
                'title': title,
                'reference_number': ref_number,
                'short_description': title,
                'main_cpv_code': main_cpv or '',
                'contract_nature_code': contract_nature_code or '',
                'contract_nature': contract_nature_code or '',
                'total_value': total_value,
                'total_value_currency': total_currency,
                'estimated_value': None,
                'estimated_value_currency': '',
                'has_lots': False,
                'lot_count': 1,
                'procedure_type_code': procedure_type_code or '',
                'procedure_type': procedure_type_code or '',
                'award_criteria_code': '',
                'award_criteria': '',
                'is_eu_funded': False,
                'performance_nuts_code': nuts_code or ''
            }
        except Exception as e:
            logger.error(f"Error extracting contract: {e}")
            return None

    def _extract_awards(self, root, ns) -> List[Dict]:
        """Extract award information from eForms UBL."""
        try:
            awards = []

            # Get lot results (awards)
            lot_results = root.xpath('.//efac:LotResult', namespaces=ns)

            for lot_result in lot_results:
                # Get conclusion date
                conclusion_date = self._get_text(root, './/efac:SettledContract/cbc:IssueDate', ns)
                conclusion_date_parsed = self._parse_date(conclusion_date) if conclusion_date else None

                # Get tender information
                tender_amount = root.xpath('.//efac:LotTender/cac:LegalMonetaryTotal/cbc:PayableAmount', namespaces=ns)
                awarded_value = None
                awarded_currency = ''
                if tender_amount:
                    awarded_value = self._get_decimal_from_text(tender_amount[0].text)
                    awarded_currency = tender_amount[0].get('currencyID', '')

                # Extract contractors
                contractors = self._extract_contractors(root, ns)

                award = {
                    'contract_number': self._get_text(root, './/efac:SettledContract/efac:ContractReference/cbc:ID', ns),
                    'award_title': self._get_text(root, './/efac:SettledContract/cbc:Title', ns) or '',
                    'conclusion_date': conclusion_date_parsed,
                    'is_awarded': True,
                    'unsuccessful_reason': '',
                    'tenders_received': 1,  # TODO: Extract actual tender count
                    'tenders_received_sme': None,
                    'tenders_received_other_eu': None,
                    'tenders_received_non_eu': None,
                    'tenders_received_electronic': 1,
                    'awarded_value': awarded_value,
                    'awarded_value_currency': awarded_currency,
                    'awarded_value_eur': None,  # TODO: Convert to EUR if needed
                    'is_subcontracted': False,  # TODO: Extract subcontracting info
                    'subcontracted_value': None,
                    'subcontracted_value_currency': '',
                    'subcontracting_description': '',
                    'contractors': contractors
                }
                awards.append(award)

            return awards if awards else [self._create_default_award(root, ns)]

        except Exception as e:
            logger.error(f"Error extracting awards: {e}")
            return []

    def _extract_contractors(self, root, ns) -> List[Dict]:
        """Extract contractor information from eForms UBL."""
        try:
            contractors = []

            # Find winning tenderer organization IDs from tender results
            winning_org_ids = set()
            tenderer_parties = root.xpath('.//efac:TenderingParty', namespaces=ns)
            for party in tenderer_parties:
                tenderer_ids = party.xpath('.//efac:Tenderer/cbc:ID/text()', namespaces=ns)
                winning_org_ids.update(tenderer_ids)

            # Find contractor organizations by matching winning organization IDs
            orgs = root.xpath('.//efac:Organizations/efac:Organization', namespaces=ns)

            for org in orgs:
                company = org.find('.//efac:Company', ns)
                if company is not None:
                    org_id = self._get_text(company, './/cac:PartyIdentification/cbc:ID', ns)

                    # Only include organizations that are winning tenderers
                    if org_id in winning_org_ids:
                        contractor = {
                            'official_name': self._get_text(company, './/cac:PartyName/cbc:Name', ns) or '',
                            'national_id': self._get_text(company, './/cac:PartyLegalEntity/cbc:CompanyID', ns),
                            'address': self._get_text(company, './/cac:PostalAddress/cbc:StreetName', ns),
                            'town': self._get_text(company, './/cac:PostalAddress/cbc:CityName', ns),
                            'postal_code': self._get_text(company, './/cac:PostalAddress/cbc:PostalZone', ns),
                            'country_code': self._get_text(company, './/cac:PostalAddress/cac:Country/cbc:IdentificationCode', ns),
                            'nuts_code': '',
                            'phone': self._get_text(company, './/cac:Contact/cbc:Telephone', ns),
                            'email': self._get_text(company, './/cac:Contact/cbc:ElectronicMail', ns),
                            'fax': '',
                            'url': self._get_text(company, './/cbc:WebsiteURI', ns),
                            'is_sme': False
                        }
                        contractors.append(contractor)

            # If no contractors found, create a default one
            if not contractors:
                contractors.append({
                    'official_name': 'Unknown Contractor',
                    'national_id': '',
                    'address': '',
                    'town': '',
                    'postal_code': '',
                    'country_code': '',
                    'nuts_code': '',
                    'phone': '',
                    'email': '',
                    'fax': '',
                    'url': '',
                    'is_sme': False
                })

            return contractors
        except Exception as e:
            logger.error(f"Error extracting contractors: {e}")
            return []

    def _create_default_award(self, root, ns) -> Dict:
        """Create a default award when no lot results are found."""
        return {
            'contract_number': '',
            'award_title': self._get_text(root, './/efac:SettledContract/cbc:Title', ns) or '',
            'conclusion_date': None,
            'is_awarded': True,
            'unsuccessful_reason': '',
            'tenders_received': 1,
            'tenders_received_sme': None,
            'tenders_received_other_eu': None,
            'tenders_received_non_eu': None,
            'tenders_received_electronic': 1,
            'awarded_value': None,
            'awarded_value_currency': '',
            'awarded_value_eur': None,
            'is_subcontracted': False,
            'subcontracted_value': None,
            'subcontracted_value_currency': '',
            'subcontracting_description': '',
            'contractors': self._extract_contractors(root, ns)
        }

    def _get_text(self, elem, xpath, ns=None, default=''):
        """Get text content from xpath."""
        try:
            result = elem.xpath(xpath, namespaces=ns) if ns else elem.xpath(xpath)
            return result[0].text if result and result[0].text else default
        except Exception:
            return default

    def _get_attr(self, elem, xpath, attr, ns=None, default=''):
        """Get attribute value from xpath."""
        try:
            result = elem.xpath(xpath, namespaces=ns) if ns else elem.xpath(xpath)
            return result[0].get(attr, default) if result else default
        except Exception:
            return default

    def _get_decimal_from_text(self, text, default=None):
        """Convert text to decimal."""
        if not text:
            return default
        try:
            return float(text)
        except (ValueError, TypeError):
            return default

    def _parse_date(self, date_str):
        """Parse date string to date object."""
        if not date_str:
            return None

        try:
            # Clean the date string - remove timezone info and time
            clean_date = date_str.split('+')[0].split('-')[0:3]  # Keep only YYYY-MM-DD parts
            if len(clean_date) == 3:
                clean_date = '-'.join(clean_date)
            else:
                clean_date = date_str.split('+')[0].split('T')[0].split('Z')[0]

            # Try different formats
            formats = ['%Y-%m-%d', '%Y%m%d']
            for fmt in formats:
                try:
                    return datetime.strptime(clean_date, fmt).date()
                except ValueError:
                    continue

        except Exception as e:
            logger.debug(f"Date parsing error for '{date_str}': {e}")

        return None