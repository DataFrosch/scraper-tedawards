import logging
from pathlib import Path
from typing import Dict, List, Optional
from lxml import etree
from datetime import datetime

from .base import BaseParser
from ..schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)
from ..utils import XmlUtils, FileDetector, DateParsingUtils

logger = logging.getLogger(__name__)

class EFormsUBLParser(BaseParser):
    """Parse eForms UBL ContractAwardNotice XML files and extract award notice data."""

    def can_parse(self, xml_file: Path) -> bool:
        """Check if this is an eForms UBL ContractAwardNotice format file."""
        return FileDetector.is_eforms_ubl(xml_file)

    def get_format_name(self) -> str:
        """Return format name."""
        return "eForms UBL ContractAwardNotice"

    def parse_xml_file(self, xml_path: Path) -> Optional[TedParserResultModel]:
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

            # Convert to Pydantic models
            document_model = DocumentModel(**doc_data)
            contracting_body_model = ContractingBodyModel(**contracting_body)
            contract_model = ContractModel(**contract)
            award_models = [AwardModel(**award) for award in awards]

            award_data = TedAwardDataModel(
                document=document_model,
                contracting_body=contracting_body_model,
                contract=contract_model,
                awards=award_models
            )

            return TedParserResultModel(awards=[award_data])

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
                XmlUtils.get_text_with_namespace(root, './/efac:Publication/efbc:PublicationDate', ns) or
                XmlUtils.get_text_with_namespace(root, './/cbc:IssueDate', ns) or
                XmlUtils.get_text_with_namespace(root, './/efac:SettledContract/cbc:IssueDate', ns) or
                XmlUtils.get_text_with_namespace(root, './/cac:ContractAwardNotice/cbc:IssueDate', ns)
            )

            # Parse the publication date
            pub_date = DateParsingUtils.normalize_date_string(pub_date_str) if pub_date_str else None

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
            contracting_party_id = XmlUtils.get_text_with_namespace(root, './/cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID', ns)

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
                        org_id = XmlUtils.get_text_with_namespace(company, './/cac:PartyIdentification/cbc:ID', ns)
                        if org_id == contracting_party_id:
                            contracting_body = company
                            break

            if contracting_body is None:
                return None

            return {
                'official_name': XmlUtils.get_text_with_namespace(contracting_body, './/cac:PartyName/cbc:Name', ns) or '',
                'address': XmlUtils.get_text_with_namespace(contracting_body, './/cac:PostalAddress/cbc:StreetName', ns),
                'town': XmlUtils.get_text_with_namespace(contracting_body, './/cac:PostalAddress/cbc:CityName', ns),
                'postal_code': XmlUtils.get_text_with_namespace(contracting_body, './/cac:PostalAddress/cbc:PostalZone', ns),
                'country_code': XmlUtils.get_text_with_namespace(contracting_body, './/cac:PostalAddress/cac:Country/cbc:IdentificationCode', ns),
                'nuts_code': '',  # TODO: Extract NUTS if available
                'contact_point': '',
                'phone': XmlUtils.get_text_with_namespace(contracting_body, './/cac:Contact/cbc:Telephone', ns),
                'email': XmlUtils.get_text_with_namespace(contracting_body, './/cac:Contact/cbc:ElectronicMail', ns),
                'fax': '',
                'url_general': XmlUtils.get_text_with_namespace(contracting_body, './/cbc:WebsiteURI', ns),
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
            title = XmlUtils.get_text_with_namespace(root, './/efac:SettledContract/cbc:Title', ns) or ''

            # Get contract reference
            ref_number = XmlUtils.get_text_with_namespace(root, './/efac:SettledContract/efac:ContractReference/cbc:ID', ns)

            # Get total value
            total_amount = root.xpath('.//efac:NoticeResult/cbc:TotalAmount', namespaces=ns)
            total_value = None
            total_currency = ''
            if total_amount:
                total_value = XmlUtils.get_decimal_from_text(total_amount[0].text)
                total_currency = total_amount[0].get('currencyID', '')

            # Extract main CPV code
            main_cpv = XmlUtils.get_text_with_namespace(root, './/cac:ProcurementProject/cac:MainCommodityClassification/cbc:ItemClassificationCode', ns)

            # Extract contract nature
            contract_nature_code = XmlUtils.get_text_with_namespace(root, './/cac:ProcurementProject/cbc:ProcurementTypeCode', ns)

            # Extract procedure type
            procedure_type_code = XmlUtils.get_text_with_namespace(root, './/cac:TenderingProcess/cbc:ProcedureCode', ns)

            # Extract performance NUTS code
            nuts_code = XmlUtils.get_text_with_namespace(root, './/cac:ProcurementProject/cac:RealizedLocation/cac:Address/cbc:CountrySubentityCode', ns)

            return {
                'title': title or '',
                'reference_number': ref_number,
                'short_description': title,
                'main_cpv_code': main_cpv,
                'contract_nature_code': contract_nature_code,
                'total_value': total_value,
                'total_value_currency': total_currency,
                'procedure_type_code': procedure_type_code,
                'award_criteria_code': None,
                'performance_nuts_code': nuts_code
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
                conclusion_date = XmlUtils.get_text_with_namespace(root, './/efac:SettledContract/cbc:IssueDate', ns)
                conclusion_date_parsed = DateParsingUtils.normalize_date_string(conclusion_date) if conclusion_date else None

                # Get tender information
                tender_amount = root.xpath('.//efac:LotTender/cac:LegalMonetaryTotal/cbc:PayableAmount', namespaces=ns)
                awarded_value = None
                awarded_currency = ''
                if tender_amount:
                    awarded_value = XmlUtils.get_decimal_from_text(tender_amount[0].text)
                    awarded_currency = tender_amount[0].get('currencyID', '')

                # Extract contractors
                contractors = self._extract_contractors(root, ns)

                award = {
                    'award_title': XmlUtils.get_text_with_namespace(root, './/efac:SettledContract/cbc:Title', ns),
                    'conclusion_date': conclusion_date_parsed,
                    'contract_number': XmlUtils.get_text_with_namespace(root, './/efac:SettledContract/efac:ContractReference/cbc:ID', ns),
                    'tenders_received': None,  # Extract from XML if available
                    'tenders_received_sme': None,
                    'tenders_received_other_eu': None,
                    'tenders_received_non_eu': None,
                    'tenders_received_electronic': None,
                    'awarded_value': awarded_value,
                    'awarded_value_currency': awarded_currency,
                    'subcontracted_value': None,
                    'subcontracted_value_currency': None,
                    'subcontracting_description': None,
                    'contractors': contractors
                }
                awards.append(award)

            return awards

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
                    org_id = XmlUtils.get_text_with_namespace(company, './/cac:PartyIdentification/cbc:ID', ns)

                    # Only include organizations that are winning tenderers
                    if org_id in winning_org_ids:
                        official_name = XmlUtils.get_text_with_namespace(company, './/cac:PartyName/cbc:Name', ns)
                        if official_name:  # Only add if we have a name
                            contractor = {
                                'official_name': official_name,
                                'address': XmlUtils.get_text_with_namespace(company, './/cac:PostalAddress/cbc:StreetName', ns),
                                'town': XmlUtils.get_text_with_namespace(company, './/cac:PostalAddress/cbc:CityName', ns),
                                'postal_code': XmlUtils.get_text_with_namespace(company, './/cac:PostalAddress/cbc:PostalZone', ns),
                                'country_code': XmlUtils.get_text_with_namespace(company, './/cac:PostalAddress/cac:Country/cbc:IdentificationCode', ns),
                                'nuts_code': None,
                                'phone': XmlUtils.get_text_with_namespace(company, './/cac:Contact/cbc:Telephone', ns),
                                'email': XmlUtils.get_text_with_namespace(company, './/cac:Contact/cbc:ElectronicMail', ns),
                                'fax': None,
                                'url': XmlUtils.get_text_with_namespace(company, './/cbc:WebsiteURI', ns),
                                'is_sme': False
                            }
                            contractors.append(contractor)

            # If no contractors found, return empty list (will be handled by AwardModel)
            return contractors
        except Exception as e:
            logger.error(f"Error extracting contractors: {e}")
            return []



