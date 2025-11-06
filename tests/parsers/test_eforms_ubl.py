"""
Tests for eForms UBL ContractAwardNotice format parser (2025+).

The eForms UBL format is the new EU standard for TED notices starting in 2025.
These tests validate:
1. Parser detection (can_parse)
2. Document parsing (parse_xml_file)
3. Data extraction (document, contracting body, contract, awards, contractors)
4. Data validation using Pydantic models
"""

import pytest
from pathlib import Path

from tedawards.parsers.eforms_ubl import EFormsUBLParser
from tedawards.schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
