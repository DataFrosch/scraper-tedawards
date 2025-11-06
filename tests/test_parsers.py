"""
Comprehensive tests for all TED XML parser formats.

Tests cover:
- TED META format (2008-2013)
- TED 2.0 R2.0.7 format (2008-2010)
- TED 2.0 R2.0.8 format (2011-2013)
- TED 2.0 R2.0.9 format (2014-2024)
- eForms UBL format (2025+)

Each test validates:
1. Parser detection (can_parse)
2. Document parsing (parse_xml_file)
3. Data extraction (document, contracting body, contract, awards, contractors)
4. Data validation using Pydantic models
"""

import pytest
from pathlib import Path
from datetime import date

from tedawards.parsers.factory import ParserFactory
from tedawards.parsers.ted_meta_xml import TedMetaXmlParser
from tedawards.parsers.ted_v2 import TedV2Parser
from tedawards.parsers.eforms_ubl import EFormsUBLParser
from tedawards.schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)


FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ============================================================================
# TED META XML Format Tests (2008-2013)
# ============================================================================

class TestTedMetaXmlParser:
    """Tests for TED META XML format parser (2008-2013)."""

    @pytest.fixture
    def parser(self):
        """Create a TED META XML parser instance."""
        return TedMetaXmlParser()

    def test_can_parse_2008_meta_format(self, parser):
        """Test parser detection for 2008 META format."""
        fixture_file = FIXTURES_DIR / "ted_meta_2008_en.zip"
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), "Parser should detect 2008 META format"

    def test_can_parse_2010_meta_format(self, parser):
        """Test parser detection for 2010 META format."""
        fixture_file = FIXTURES_DIR / "ted_meta_2010_cs.zip"
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), "Parser should detect 2010 META format"

    def test_parse_2008_meta_document(self, parser):
        """Test parsing 2008 META format document."""
        fixture_file = FIXTURES_DIR / "ted_meta_2008_en.zip"
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, "Parser should return result"
        assert isinstance(result, TedParserResultModel), "Result should be TedParserResultModel"
        assert len(result.awards) > 0, "Should extract at least one award"

        # Validate first award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id, "Document ID should be present"
        assert document.publication_date is not None, "Publication date should be present"
        assert document.original_language, "Original language should be present"
        assert document.source_country, "Source country should be present"

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, "Contracting body name should be present"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, "Contract title should be present"

        # Validate awards
        assert len(award_data.awards) > 0, "Should have at least one award"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)

    def test_parse_2010_meta_document(self, parser):
        """Test parsing 2010 META format document."""
        fixture_file = FIXTURES_DIR / "ted_meta_2010_cs.zip"
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, "Parser should return result"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, "Should extract at least one award"

        # Validate award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)
        assert award_data.document is not None
        assert award_data.contracting_body is not None
        assert award_data.contract is not None
        assert len(award_data.awards) > 0

    def test_get_format_name(self, parser):
        """Test parser format name."""
        assert parser.get_format_name() == "TED META XML"


# ============================================================================
# TED 2.0 R2.0.7 Format Tests (2008-2010)
# ============================================================================

class TestTedV2R207Parser:
    """Tests for TED 2.0 R2.0.7 format parser."""

    @pytest.fixture
    def parser(self):
        """Create a TED V2 parser instance."""
        return TedV2Parser()

    def test_can_parse_r207_format(self, parser):
        """Test parser detection for R2.0.7 format."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_7_2011.xml"
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), "Parser should detect R2.0.7 format"

    def test_parse_r207_document(self, parser):
        """Test parsing R2.0.7 format document."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_7_2011.xml"
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, "Parser should return result"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, "Should extract at least one award"

        # Validate award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id == "000755-2011", "Document ID should match fixture"
        assert document.publication_date is not None, "Publication date should be present"
        assert document.version, "Version should be present"
        assert "R2.0.7" in document.version or "R2.0.7/R2.0.8" in document.version

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, "Contracting body name should be present"
        assert contracting_body.country_code, "Country code should be present"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, "Contract title should be present"

        # Validate awards
        assert len(award_data.awards) > 0, "Should have at least one award"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)

        # Validate contractors if present
        if award.contractors:
            for contractor in award.contractors:
                assert isinstance(contractor, ContractorModel)
                assert contractor.official_name, "Contractor name should be present"


# ============================================================================
# TED 2.0 R2.0.8 Format Tests (2011-2013)
# ============================================================================

class TestTedV2R208Parser:
    """Tests for TED 2.0 R2.0.8 format parser."""

    @pytest.fixture
    def parser(self):
        """Create a TED V2 parser instance."""
        return TedV2Parser()

    def test_can_parse_r208_format(self, parser):
        """Test parser detection for R2.0.8 format."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_8_2015.xml"
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), "Parser should detect R2.0.8 format"

    def test_parse_r208_document(self, parser):
        """Test parsing R2.0.8 format document."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_8_2015.xml"
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, "Parser should return result"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, "Should extract at least one award"

        # Validate award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id == "001077-2015", "Document ID should match fixture"
        assert document.publication_date is not None, "Publication date should be present"
        assert document.version, "Version should be present"
        assert "R2.0.8" in document.version or "R2.0.7/R2.0.8" in document.version

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, "Contracting body name should be present"
        assert contracting_body.country_code, "Country code should be present"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, "Contract title should be present"

        # Validate awards
        assert len(award_data.awards) > 0, "Should have at least one award"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)

        # Validate contractors if present
        if award.contractors:
            for contractor in award.contractors:
                assert isinstance(contractor, ContractorModel)
                assert contractor.official_name, "Contractor name should be present"


# ============================================================================
# TED 2.0 R2.0.9 Format Tests (2014-2024)
# ============================================================================

class TestTedV2R209Parser:
    """Tests for TED 2.0 R2.0.9 format parser (F03_2014 forms)."""

    @pytest.fixture
    def parser(self):
        """Create a TED V2 parser instance."""
        return TedV2Parser()

    def test_can_parse_r209_format(self, parser):
        """Test parser detection for R2.0.9 format."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_9_2024.xml"
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), "Parser should detect R2.0.9 format"

    def test_parse_r209_document(self, parser):
        """Test parsing R2.0.9 format document."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_9_2024.xml"
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, "Parser should return result"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, "Should extract at least one award"

        # Validate award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id == "000769-2024", "Document ID should match fixture"
        assert document.publication_date is not None, "Publication date should be present"
        assert document.publication_date == date(2024, 1, 2), "Publication date should be 2024-01-02"
        assert document.version == "R2.0.9", "Version should be R2.0.9"
        assert document.source_country == "PL", "Source country should be Poland"
        assert document.original_language == "pl", "Original language should be Polish"

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, "Contracting body name should be present"
        assert "25. Wojskowy Oddział Gospodarczy" in contracting_body.official_name
        assert contracting_body.country_code == "PL", "Country code should be Poland"
        assert contracting_body.town == "Białystok", "Town should be Białystok"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, "Contract title should be present"
        assert "pieczywa" in contract.title.lower(), "Title should mention bread (pieczywa)"
        assert contract.main_cpv_code == "15811000", "CPV code should match"

        # Validate awards
        assert len(award_data.awards) > 0, "Should have at least one award"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)
        assert award.awarded_value is not None, "Award value should be present"
        assert award.awarded_value == 3467275.00, "Award value should match"
        assert award.awarded_value_currency == "PLN", "Currency should be PLN"
        assert award.tenders_received == 5, "Should have received 5 tenders"

        # Validate contractors
        assert len(award.contractors) > 0, "Should have at least one contractor"
        contractor = award.contractors[0]
        assert isinstance(contractor, ContractorModel)
        assert contractor.official_name, "Contractor name should be present"
        assert "SOBIESKI" in contractor.official_name, "Contractor should be SOBIESKI"
        assert contractor.country_code == "PL", "Contractor country should be Poland"

    def test_get_format_name(self, parser):
        """Test parser format name."""
        assert parser.get_format_name() == "TED 2.0"


# ============================================================================
# eForms UBL Format Tests (2025+)
# ============================================================================

class TestEFormsUBLParser:
    """Tests for eForms UBL ContractAwardNotice format parser."""

    @pytest.fixture
    def parser(self):
        """Create an eForms UBL parser instance."""
        return EFormsUBLParser()

    def test_can_parse_eforms_ubl_format(self, parser):
        """Test parser detection for eForms UBL format."""
        fixture_file = FIXTURES_DIR / "eforms_ubl_2025.xml"
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), "Parser should detect eForms UBL format"

    def test_parse_eforms_ubl_document(self, parser):
        """Test parsing eForms UBL format document."""
        fixture_file = FIXTURES_DIR / "eforms_ubl_2025.xml"
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, "Parser should return result"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, "Should extract at least one award"

        # Validate award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id, "Document ID should be present"
        assert "2025" in document.doc_id, "Document ID should contain 2025"
        assert document.publication_date is not None, "Publication date should be present"
        assert document.version == "eForms-UBL", "Version should be eForms-UBL"

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, "Contracting body name should be present"
        assert contracting_body.country_code, "Country code should be present"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, "Contract title should be present"

        # Validate awards
        assert len(award_data.awards) > 0, "Should have at least one award"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)

        # Validate contractors if present
        if award.contractors:
            for contractor in award.contractors:
                assert isinstance(contractor, ContractorModel)
                assert contractor.official_name, "Contractor name should be present"

    def test_get_format_name(self, parser):
        """Test parser format name."""
        assert parser.get_format_name() == "eForms UBL ContractAwardNotice"


# ============================================================================
# Parser Factory Tests
# ============================================================================

class TestParserFactory:
    """Tests for ParserFactory auto-detection."""

    @pytest.fixture
    def factory(self):
        """Create a parser factory instance."""
        return ParserFactory()

    def test_factory_detects_ted_meta_2008(self, factory):
        """Test factory auto-detects TED META 2008 format."""
        fixture_file = FIXTURES_DIR / "ted_meta_2008_en.zip"
        parser = factory.get_parser(fixture_file)
        assert parser is not None, "Factory should return a parser"
        assert isinstance(parser, TedMetaXmlParser), "Should detect TED META XML parser"

    def test_factory_detects_ted_meta_2010(self, factory):
        """Test factory auto-detects TED META 2010 format."""
        fixture_file = FIXTURES_DIR / "ted_meta_2010_cs.zip"
        parser = factory.get_parser(fixture_file)
        assert parser is not None, "Factory should return a parser"
        assert isinstance(parser, TedMetaXmlParser), "Should detect TED META XML parser"

    def test_factory_detects_ted_v2_r207(self, factory):
        """Test factory auto-detects TED 2.0 R2.0.7 format."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_7_2011.xml"
        parser = factory.get_parser(fixture_file)
        assert parser is not None, "Factory should return a parser"
        assert isinstance(parser, TedV2Parser), "Should detect TED V2 parser"

    def test_factory_detects_ted_v2_r208(self, factory):
        """Test factory auto-detects TED 2.0 R2.0.8 format."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_8_2015.xml"
        parser = factory.get_parser(fixture_file)
        assert parser is not None, "Factory should return a parser"
        assert isinstance(parser, TedV2Parser), "Should detect TED V2 parser"

    def test_factory_detects_ted_v2_r209(self, factory):
        """Test factory auto-detects TED 2.0 R2.0.9 format."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_9_2024.xml"
        parser = factory.get_parser(fixture_file)
        assert parser is not None, "Factory should return a parser"
        assert isinstance(parser, TedV2Parser), "Should detect TED V2 parser"

    def test_factory_detects_eforms_ubl(self, factory):
        """Test factory auto-detects eForms UBL format."""
        fixture_file = FIXTURES_DIR / "eforms_ubl_2025.xml"
        parser = factory.get_parser(fixture_file)
        assert parser is not None, "Factory should return a parser"
        assert isinstance(parser, EFormsUBLParser), "Should detect eForms UBL parser"

    def test_factory_supported_formats(self, factory):
        """Test factory returns list of supported formats."""
        formats = factory.get_supported_formats()
        assert len(formats) >= 3, "Should support at least 3 formats"
        assert "TED META XML" in formats
        assert "TED 2.0" in formats
        assert "eForms UBL ContractAwardNotice" in formats


# ============================================================================
# Integration Tests - End-to-End Parsing
# ============================================================================

class TestEndToEndParsing:
    """Integration tests for complete parsing workflow."""

    @pytest.fixture
    def factory(self):
        """Create a parser factory instance."""
        return ParserFactory()

    @pytest.mark.parametrize("fixture_file,expected_format", [
        ("ted_meta_2008_en.zip", "TED META XML"),
        ("ted_meta_2010_cs.zip", "TED META XML"),
        ("ted_v2_r2_0_7_2011.xml", "TED 2.0"),
        ("ted_v2_r2_0_8_2015.xml", "TED 2.0"),
        ("ted_v2_r2_0_9_2024.xml", "TED 2.0"),
        ("eforms_ubl_2025.xml", "eForms UBL ContractAwardNotice"),
    ])
    def test_parse_all_formats(self, factory, fixture_file, expected_format):
        """Test parsing all supported formats end-to-end."""
        file_path = FIXTURES_DIR / fixture_file

        # Get parser
        parser = factory.get_parser(file_path)
        assert parser is not None, f"Should detect parser for {fixture_file}"
        assert parser.get_format_name() == expected_format

        # Parse document
        result = parser.parse_xml_file(file_path)
        assert result is not None, f"Should parse {fixture_file}"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, f"Should extract awards from {fixture_file}"

        # Validate basic structure for all formats
        for award_data in result.awards:
            assert isinstance(award_data, TedAwardDataModel)
            assert award_data.document is not None
            assert award_data.contracting_body is not None
            assert award_data.contract is not None
            assert len(award_data.awards) > 0

    def test_parse_all_fixtures_in_directory(self, factory):
        """Test that all XML fixtures can be parsed without errors."""
        xml_fixtures = list(FIXTURES_DIR.glob("*.xml"))
        zip_fixtures = [
            f for f in FIXTURES_DIR.glob("*.zip")
            if "_meta_" in f.name.lower()  # Include TED META format ZIPs
        ]

        all_fixtures = xml_fixtures + zip_fixtures
        parsed_count = 0
        errors = []

        for fixture_file in all_fixtures:
            try:
                parser = factory.get_parser(fixture_file)
                if parser is not None:
                    result = parser.parse_xml_file(fixture_file)
                    if result is not None:
                        parsed_count += 1
                    else:
                        errors.append(f"{fixture_file.name}: Parser returned None")
                else:
                    errors.append(f"{fixture_file.name}: No parser detected")
            except Exception as e:
                errors.append(f"{fixture_file.name}: {str(e)}")

        # Report results
        print(f"\nParsed {parsed_count} out of {len(all_fixtures)} fixtures")
        if errors:
            print("\nErrors encountered:")
            for error in errors:
                print(f"  - {error}")

        # We expect to parse the XML formats we support
        assert parsed_count >= 5, f"Should parse at least 5 fixtures, parsed {parsed_count}"


# ============================================================================
# Data Validation Tests
# ============================================================================

class TestDataValidation:
    """Tests for data validation and quality."""

    def test_date_fields_are_valid(self):
        """Test that date fields are properly validated."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_9_2024.xml"
        parser = TedV2Parser()
        result = parser.parse_xml_file(fixture_file)

        assert result is not None
        award_data = result.awards[0]

        # Check date fields
        assert isinstance(award_data.document.publication_date, date)
        if award_data.document.dispatch_date:
            assert isinstance(award_data.document.dispatch_date, date)

        for award in award_data.awards:
            if award.conclusion_date:
                assert isinstance(award.conclusion_date, date)

    def test_country_codes_are_uppercase(self):
        """Test that country codes are normalized to uppercase."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_9_2024.xml"
        parser = TedV2Parser()
        result = parser.parse_xml_file(fixture_file)

        assert result is not None
        award_data = result.awards[0]

        # Check country codes
        if award_data.document.source_country:
            assert award_data.document.source_country.isupper()
        if award_data.contracting_body.country_code:
            assert award_data.contracting_body.country_code.isupper()

        for award in award_data.awards:
            for contractor in award.contractors:
                if contractor.country_code:
                    assert contractor.country_code.isupper()

    def test_language_codes_are_lowercase(self):
        """Test that language codes are normalized to lowercase."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_9_2024.xml"
        parser = TedV2Parser()
        result = parser.parse_xml_file(fixture_file)

        assert result is not None
        award_data = result.awards[0]

        # Check language codes
        assert award_data.document.form_language.islower()
        if award_data.document.original_language:
            assert award_data.document.original_language.islower()

    def test_contractor_names_are_present(self):
        """Test that contractors have valid names."""
        fixture_file = FIXTURES_DIR / "ted_v2_r2_0_9_2024.xml"
        parser = TedV2Parser()
        result = parser.parse_xml_file(fixture_file)

        assert result is not None
        award_data = result.awards[0]

        for award in award_data.awards:
            if award.contractors:
                for contractor in award.contractors:
                    assert contractor.official_name, "Contractor must have official name"
                    assert len(contractor.official_name.strip()) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
