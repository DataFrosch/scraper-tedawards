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

class EFormsUBLParser(BaseParser):
    """Parse eForms UBL ContractAwardNotice XML files and extract award notice data."""

    def can_parse(self, xml_file: Path) -> bool:
        """Check if this is an eForms UBL ContractAwardNotice format file."""
        try:
            with open(xml_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(1000)  # Read first 1KB
            return ('ContractAwardNotice' in content and
                    'urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2' in content)
        except Exception as e:
            logger.debug(f"Error reading file {xml_file.name} for eForms UBL detection: {e}")
            return False

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

            # Check if this is the original language version to avoid processing translations
            if not self._is_original_language_version(root, namespaces):
                logger.debug(f"Skipping {xml_path.name} - not original language version")
                return None

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
            pub_date_elem = (
                root.xpath('.//efac:Publication/efbc:PublicationDate', namespaces=ns) or
                root.xpath('.//cbc:IssueDate', namespaces=ns) or
                root.xpath('.//efac:SettledContract/cbc:IssueDate', namespaces=ns) or
                root.xpath('.//cac:ContractAwardNotice/cbc:IssueDate', namespaces=ns)
            )

            # Parse ISO format date (YYYY-MM-DD), strip timezone if present
            if not pub_date_elem or not pub_date_elem[0].text:
                logger.error(f"No publication date found in eForms document {xml_file}")
                return None

            try:
                # eForms dates: "2024-01-04" or "2024-01-04Z" or "2024-01-04+01:00"
                # Use fromisoformat but strip timezone suffix if present
                raw_date = pub_date_elem[0].text.strip()
                # fromisoformat doesn't handle timezones in date strings, so strip them
                date_only = raw_date.split('Z')[0].split('+')[0].split('T')[0]
                pub_date = date.fromisoformat(date_only)
            except (ValueError, AttributeError, IndexError) as e:
                logger.error(f"Invalid publication date format in {xml_file}: {pub_date_elem[0].text}. Expected ISO date format. Error: {e}")
                raise

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
            contracting_party_id_elem = root.xpath('.//cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID', namespaces=ns)
            contracting_party_id = contracting_party_id_elem[0].text if contracting_party_id_elem and contracting_party_id_elem[0].text else None

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
                        org_id_elem = company.xpath('.//cac:PartyIdentification/cbc:ID', namespaces=ns)
                        org_id = org_id_elem[0].text if org_id_elem and org_id_elem[0].text else None
                        if org_id == contracting_party_id:
                            contracting_body = company
                            break

            if contracting_body is None:
                return None

            # Extract all fields with explicit xpath calls
            name_elem = contracting_body.xpath('.//cac:PartyName/cbc:Name', namespaces=ns)
            address_elem = contracting_body.xpath('.//cac:PostalAddress/cbc:StreetName', namespaces=ns)
            town_elem = contracting_body.xpath('.//cac:PostalAddress/cbc:CityName', namespaces=ns)
            postal_elem = contracting_body.xpath('.//cac:PostalAddress/cbc:PostalZone', namespaces=ns)
            country_elem = contracting_body.xpath('.//cac:PostalAddress/cac:Country/cbc:IdentificationCode', namespaces=ns)
            phone_elem = contracting_body.xpath('.//cac:Contact/cbc:Telephone', namespaces=ns)
            email_elem = contracting_body.xpath('.//cac:Contact/cbc:ElectronicMail', namespaces=ns)
            url_elem = contracting_body.xpath('.//cbc:WebsiteURI', namespaces=ns)

            return {
                'official_name': name_elem[0].text if (name_elem and name_elem[0].text) else '',
                'address': address_elem[0].text if (address_elem and address_elem[0].text) else None,
                'town': town_elem[0].text if (town_elem and town_elem[0].text) else None,
                'postal_code': postal_elem[0].text if (postal_elem and postal_elem[0].text) else None,
                'country_code': country_elem[0].text if (country_elem and country_elem[0].text) else None,
                'nuts_code': None,  # TODO: Extract NUTS if available
                'contact_point': '',
                'phone': phone_elem[0].text if (phone_elem and phone_elem[0].text) else None,
                'email': email_elem[0].text if (email_elem and email_elem[0].text) else None,
                'fax': '',
                'url_general': url_elem[0].text if (url_elem and url_elem[0].text) else None,
                'url_buyer': '',
                'authority_type_code': '',
                'main_activity_code': ''
            }
        except Exception as e:
            logger.error(f"Error extracting contracting body: {e}")
            raise

    def _extract_contract(self, root, ns) -> Optional[Dict]:
        """Extract contract information from eForms UBL."""
        try:
            # Get contract title from settled contract
            title_elem = root.xpath('.//efac:SettledContract/cbc:Title', namespaces=ns)
            title = title_elem[0].text if (title_elem and title_elem[0].text) else ''

            # Get contract reference
            ref_elem = root.xpath('.//efac:SettledContract/efac:ContractReference/cbc:ID', namespaces=ns)
            ref_number = ref_elem[0].text if (ref_elem and ref_elem[0].text) else None

            # Get total value
            total_amount = root.xpath('.//efac:NoticeResult/cbc:TotalAmount', namespaces=ns)
            total_value = None
            total_currency = ''
            if total_amount and total_amount[0].text:
                try:
                    total_value = float(total_amount[0].text)
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid total amount value: {total_amount[0].text}. Error: {e}")
                    raise
                total_currency = total_amount[0].get('currencyID', '')

            # Extract main CPV code
            cpv_elem = root.xpath('.//cac:ProcurementProject/cac:MainCommodityClassification/cbc:ItemClassificationCode', namespaces=ns)
            main_cpv = cpv_elem[0].text if (cpv_elem and cpv_elem[0].text) else None

            # Extract contract nature
            nature_elem = root.xpath('.//cac:ProcurementProject/cbc:ProcurementTypeCode', namespaces=ns)
            contract_nature_code = nature_elem[0].text if (nature_elem and nature_elem[0].text) else None

            # Extract procedure type
            proc_elem = root.xpath('.//cac:TenderingProcess/cbc:ProcedureCode', namespaces=ns)
            procedure_type_code = proc_elem[0].text if (proc_elem and proc_elem[0].text) else None

            # Extract performance NUTS code
            nuts_elem = root.xpath('.//cac:ProcurementProject/cac:RealizedLocation/cac:Address/cbc:CountrySubentityCode', namespaces=ns)
            nuts_code = nuts_elem[0].text if (nuts_elem and nuts_elem[0].text) else None

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
            raise

    def _extract_awards(self, root, ns) -> List[Dict]:
        """Extract award information from eForms UBL."""
        try:
            awards = []

            # Get lot results (awards)
            lot_results = root.xpath('.//efac:LotResult', namespaces=ns)

            for lot_result in lot_results:
                # Get conclusion date
                conclusion_date_elem = root.xpath('.//efac:SettledContract/cbc:IssueDate', namespaces=ns)
                conclusion_date_parsed = None
                if conclusion_date_elem and conclusion_date_elem[0].text:
                    try:
                        raw_date = conclusion_date_elem[0].text.strip()
                        date_only = raw_date.split('Z')[0].split('+')[0].split('T')[0]
                        conclusion_date_parsed = date.fromisoformat(date_only)
                    except (ValueError, AttributeError, IndexError) as e:
                        logger.error(f"Invalid conclusion date format: {conclusion_date_elem[0].text}. Expected ISO date format. Error: {e}")
                        raise

                # Get tender information
                tender_amount = root.xpath('.//efac:LotTender/cac:LegalMonetaryTotal/cbc:PayableAmount', namespaces=ns)
                awarded_value = None
                awarded_currency = ''
                if tender_amount and tender_amount[0].text:
                    try:
                        awarded_value = float(tender_amount[0].text)
                    except (ValueError, TypeError) as e:
                        logger.error(f"Invalid awarded value: {tender_amount[0].text}. Error: {e}")
                        raise
                    awarded_currency = tender_amount[0].get('currencyID', '')

                # Extract contractors
                contractors = self._extract_contractors(root, ns)

                # Get award title
                award_title_elem = root.xpath('.//efac:SettledContract/cbc:Title', namespaces=ns)
                award_title = award_title_elem[0].text if (award_title_elem and award_title_elem[0].text) else None

                # Get contract number
                contract_num_elem = root.xpath('.//efac:SettledContract/efac:ContractReference/cbc:ID', namespaces=ns)
                contract_number = contract_num_elem[0].text if (contract_num_elem and contract_num_elem[0].text) else None

                award = {
                    'award_title': award_title,
                    'conclusion_date': conclusion_date_parsed,
                    'contract_number': contract_number,
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
                    org_id_elem = company.xpath('.//cac:PartyIdentification/cbc:ID', namespaces=ns)
                    org_id = org_id_elem[0].text if (org_id_elem and org_id_elem[0].text) else None

                    # Only include organizations that are winning tenderers
                    if org_id in winning_org_ids:
                        name_elem = company.xpath('.//cac:PartyName/cbc:Name', namespaces=ns)
                        official_name = name_elem[0].text if (name_elem and name_elem[0].text) else None

                        if official_name:  # Only add if we have a name
                            address_elem = company.xpath('.//cac:PostalAddress/cbc:StreetName', namespaces=ns)
                            town_elem = company.xpath('.//cac:PostalAddress/cbc:CityName', namespaces=ns)
                            postal_elem = company.xpath('.//cac:PostalAddress/cbc:PostalZone', namespaces=ns)
                            country_elem = company.xpath('.//cac:PostalAddress/cac:Country/cbc:IdentificationCode', namespaces=ns)
                            phone_elem = company.xpath('.//cac:Contact/cbc:Telephone', namespaces=ns)
                            email_elem = company.xpath('.//cac:Contact/cbc:ElectronicMail', namespaces=ns)
                            url_elem = company.xpath('.//cbc:WebsiteURI', namespaces=ns)

                            contractor = {
                                'official_name': official_name,
                                'address': address_elem[0].text if (address_elem and address_elem[0].text) else None,
                                'town': town_elem[0].text if (town_elem and town_elem[0].text) else None,
                                'postal_code': postal_elem[0].text if (postal_elem and postal_elem[0].text) else None,
                                'country_code': country_elem[0].text if (country_elem and country_elem[0].text) else None,
                                'nuts_code': None,
                                'phone': phone_elem[0].text if (phone_elem and phone_elem[0].text) else None,
                                'email': email_elem[0].text if (email_elem and email_elem[0].text) else None,
                                'fax': None,
                                'url': url_elem[0].text if (url_elem and url_elem[0].text) else None,
                                'is_sme': False
                            }
                            contractors.append(contractor)

            # If no contractors found, return empty list (will be handled by AwardModel)
            return contractors
        except Exception as e:
            logger.error(f"Error extracting contractors: {e}")
            return []

    def _is_original_language_version(self, root, ns) -> bool:
        """
        Check if this eForms UBL document is in its original language.

        eForms documents can have translations, so we need to filter to only process
        the original language version to avoid duplicates.

        For eForms UBL, the original language is typically indicated by:
        1. The document's primary language matching the original language of the notice
        2. Looking for explicit language markers in the metadata
        """
        try:
            # Extract the most common language used in the document
            language_elements = root.xpath('.//*[@languageID]', namespaces=ns)
            languages = [elem.get('languageID') for elem in language_elements if elem.get('languageID')]

            if not languages:
                # Fail loud - language information is required for deduplication
                raise ValueError("No language markers (languageID attributes) found in eForms document - cannot determine original language version")

            # Get the most frequent language
            primary_lang = max(set(languages), key=languages.count)

            # For eForms, check if there's an explicit original language marker
            # Look for language declarations or original language indicators
            orig_lang_elements = root.xpath('.//efbc:OriginalLanguage/text()', namespaces=ns)
            if orig_lang_elements:
                original_language = orig_lang_elements[0].upper()
                return primary_lang.upper() == original_language.upper()

            # Fallback: Check publication metadata for language indicators
            # In eForms, if this document is published in multiple languages,
            # the original usually has specific markers
            publication_elements = root.xpath('.//efac:Publication', namespaces=ns)
            for pub in publication_elements:
                lang_attr = pub.get('languageID', '').upper()
                if lang_attr and lang_attr == primary_lang.upper():
                    # Check if this publication is marked as original
                    pub_type_elem = pub.xpath('.//efbc:PublicationType', namespaces=ns)
                    pub_type = pub_type_elem[0].text if (pub_type_elem and pub_type_elem[0].text) else None
                    if pub_type and 'original' in pub_type.lower():
                        return True

            # If no explicit indicators found, assume this is the original
            # (better to process potentially duplicate than miss original)
            logger.debug(f"No explicit original language markers found in eForms document, processing as original (primary language: {primary_lang})")
            return True

        except Exception as e:
            logger.error(f"Error checking original language version: {e}")
            # In case of error, process the document (fail-open approach)
            return True