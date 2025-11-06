"""
Tests for TED META XML format parser (2008-2013).

The TED META XML format was used from 2008-2013 for TED notices.
These tests validate:
1. Parser detection (can_parse)
2. Document parsing (parse_xml_file)
3. Data extraction (document, contracting body, contract, awards, contractors)
4. Data validation using Pydantic models
"""

import pytest
from pathlib import Path

from tedawards.parsers.ted_meta_xml import TedMetaXmlParser
from tedawards.schema import (
    TedParserResultModel, TedAwardDataModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel
)


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
