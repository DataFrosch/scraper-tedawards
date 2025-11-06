"""
Tests for TED 2.0 XML format parser.

The unified TED 2.0 parser handles multiple format variants:
- R2.0.7 (2011-2013): XML with CONTRACT_AWARD forms, early structure
- R2.0.8 (2014-2015): XML with CONTRACT_AWARD forms, enhanced structure
- R2.0.9 (2014-2024): XML with F03_2014 forms, modern structure

These tests validate:
1. Parser detection (can_parse)
2. Document parsing (parse_xml_file)
3. Data extraction (document, contracting body, contract, awards, contractors)
4. Data validation using Pydantic models
"""

import pytest
from pathlib import Path
from datetime import date

from tedawards.parsers.ted_v2 import TedV2Parser
from tedawards.schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

# List of TED 2.0 R2.0.7 fixtures (2011-2013)
TED_V2_R207_FIXTURES = [
    "ted_v2_r2_0_7_2011.xml",
]

# List of TED 2.0 R2.0.8 fixtures (2014-2015)
TED_V2_R208_FIXTURES = [
    "ted_v2_r2_0_8_2015.xml",
]

# List of TED 2.0 R2.0.9 fixtures (2014-2024)
TED_V2_R209_FIXTURES = [
    "ted_v2_r2_0_9_2024.xml",
]


class TestTedV2R207Parser:
    """Tests for TED 2.0 R2.0.7 format parser."""

    @pytest.fixture
    def parser(self):
        """Create a TED V2 parser instance."""
        return TedV2Parser()

    @pytest.mark.parametrize("fixture_name", TED_V2_R207_FIXTURES)
    def test_can_parse_r207_format(self, parser, fixture_name):
        """Test parser detection for R2.0.7 format."""
        fixture_file = FIXTURES_DIR / fixture_name
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), f"Parser should detect R2.0.7 format for {fixture_name}"

    @pytest.mark.parametrize("fixture_name", TED_V2_R207_FIXTURES)
    def test_parse_r207_document(self, parser, fixture_name):
        """Test parsing R2.0.7 format document."""
        fixture_file = FIXTURES_DIR / fixture_name
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, f"Parser should return result for {fixture_name}"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, f"Should extract at least one award from {fixture_name}"

        # Validate award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id, f"Document ID should be present in {fixture_name}"
        assert document.publication_date is not None, f"Publication date should be present in {fixture_name}"
        assert document.version, f"Version should be present in {fixture_name}"
        assert "R2.0.7" in document.version or "R2.0.7/R2.0.8" in document.version

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, f"Contracting body name should be present in {fixture_name}"
        assert contracting_body.country_code, f"Country code should be present in {fixture_name}"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, f"Contract title should be present in {fixture_name}"

        # Validate awards
        assert len(award_data.awards) > 0, f"Should have at least one award in {fixture_name}"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)

        # Validate contractors if present
        if award.contractors:
            for contractor in award.contractors:
                assert isinstance(contractor, ContractorModel)
                assert contractor.official_name, f"Contractor name should be present in {fixture_name}"


class TestTedV2R208Parser:
    """Tests for TED 2.0 R2.0.8 format parser."""

    @pytest.fixture
    def parser(self):
        """Create a TED V2 parser instance."""
        return TedV2Parser()

    @pytest.mark.parametrize("fixture_name", TED_V2_R208_FIXTURES)
    def test_can_parse_r208_format(self, parser, fixture_name):
        """Test parser detection for R2.0.8 format."""
        fixture_file = FIXTURES_DIR / fixture_name
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), f"Parser should detect R2.0.8 format for {fixture_name}"

    @pytest.mark.parametrize("fixture_name", TED_V2_R208_FIXTURES)
    def test_parse_r208_document(self, parser, fixture_name):
        """Test parsing R2.0.8 format document."""
        fixture_file = FIXTURES_DIR / fixture_name
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, f"Parser should return result for {fixture_name}"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, f"Should extract at least one award from {fixture_name}"

        # Validate award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id, f"Document ID should be present in {fixture_name}"
        assert document.publication_date is not None, f"Publication date should be present in {fixture_name}"
        assert document.version, f"Version should be present in {fixture_name}"
        assert "R2.0.8" in document.version or "R2.0.7/R2.0.8" in document.version

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, f"Contracting body name should be present in {fixture_name}"
        assert contracting_body.country_code, f"Country code should be present in {fixture_name}"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, f"Contract title should be present in {fixture_name}"

        # Validate awards
        assert len(award_data.awards) > 0, f"Should have at least one award in {fixture_name}"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)

        # Validate contractors if present
        if award.contractors:
            for contractor in award.contractors:
                assert isinstance(contractor, ContractorModel)
                assert contractor.official_name, f"Contractor name should be present in {fixture_name}"


class TestTedV2R209Parser:
    """Tests for TED 2.0 R2.0.9 format parser (F03_2014 forms)."""

    @pytest.fixture
    def parser(self):
        """Create a TED V2 parser instance."""
        return TedV2Parser()

    @pytest.mark.parametrize("fixture_name", TED_V2_R209_FIXTURES)
    def test_can_parse_r209_format(self, parser, fixture_name):
        """Test parser detection for R2.0.9 format."""
        fixture_file = FIXTURES_DIR / fixture_name
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), f"Parser should detect R2.0.9 format for {fixture_name}"

    def test_parse_r209_document_detailed(self, parser):
        """Test parsing R2.0.9 format document with detailed validation (2024 fixture only)."""
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
        assert document.doc_id == "002670-2024", "Document ID should match fixture"
        assert document.publication_date is not None, "Publication date should be present"
        assert document.publication_date == date(2024, 1, 3), "Publication date should be 2024-01-03"
        assert document.version == "R2.0.9", "Version should be R2.0.9"
        assert document.source_country == "AT", "Source country should be Austria"

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, "Contracting body name should be present"
        assert "Medizinische Universität Innsbruck" in contracting_body.official_name
        assert contracting_body.country_code == "AT", "Country code should be Austria"
        assert contracting_body.town == "Innsbruck", "Town should be Innsbruck"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, "Contract title should be present"
        assert "Pipettierroboter" in contract.title, "Title should mention Pipettierroboter"
        assert contract.main_cpv_code == "38430000", "CPV code should match"

        # Validate awards
        assert len(award_data.awards) > 0, "Should have at least one award"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)
        assert award.awarded_value is not None, "Award value should be present"
        assert award.awarded_value == 388481.50, "Award value should match"
        assert award.awarded_value_currency == "EUR", "Currency should be EUR"
        assert award.tenders_received == 1, "Should have received 1 tender"

        # Validate contractors
        assert len(award.contractors) > 0, "Should have at least one contractor"
        contractor = award.contractors[0]
        assert isinstance(contractor, ContractorModel)
        assert contractor.official_name, "Contractor name should be present"
        assert "Hamilton Germany" in contractor.official_name, "Contractor should be Hamilton Germany"
        assert contractor.country_code == "DE", "Contractor country should be Germany"

    @pytest.mark.parametrize("fixture_name", TED_V2_R209_FIXTURES)
    def test_parse_r209_document(self, parser, fixture_name):
        """Test parsing R2.0.9 format document (all fixtures)."""
        fixture_file = FIXTURES_DIR / fixture_name
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, f"Parser should return result for {fixture_name}"
        assert isinstance(result, TedParserResultModel)
        assert len(result.awards) > 0, f"Should extract at least one award from {fixture_name}"

        # Validate award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id, f"Document ID should be present in {fixture_name}"
        assert document.publication_date is not None, f"Publication date should be present in {fixture_name}"
        assert document.version == "R2.0.9", f"Version should be R2.0.9 in {fixture_name}"
        assert document.source_country, f"Source country should be present in {fixture_name}"

        # Validate contracting body
        contracting_body = award_data.contracting_body
        assert isinstance(contracting_body, ContractingBodyModel)
        assert contracting_body.official_name, f"Contracting body name should be present in {fixture_name}"

        # Validate contract
        contract = award_data.contract
        assert isinstance(contract, ContractModel)
        assert contract.title, f"Contract title should be present in {fixture_name}"

        # Validate awards
        assert len(award_data.awards) > 0, f"Should have at least one award in {fixture_name}"
        award = award_data.awards[0]
        assert isinstance(award, AwardModel)

    def test_get_format_name(self, parser):
        """Test parser format name."""
        assert parser.get_format_name() == "TED 2.0"


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
