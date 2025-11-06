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
