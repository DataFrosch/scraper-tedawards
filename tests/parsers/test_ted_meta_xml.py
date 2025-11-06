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

# List of TED META XML fixtures to test
TED_META_FIXTURES = [
    "ted_meta_2008_en.zip",
    "ted_meta_2009_de.zip",
    "ted_meta_2009_pl.zip",
    "ted_meta_2010_cs.zip",
]


class TestTedMetaXmlParser:
    """Tests for TED META XML format parser (2008-2013)."""

    @pytest.fixture
    def parser(self):
        """Create a TED META XML parser instance."""
        return TedMetaXmlParser()

    @pytest.mark.parametrize("fixture_name", TED_META_FIXTURES)
    def test_can_parse_meta_format(self, parser, fixture_name):
        """Test parser detection for TED META XML format."""
        fixture_file = FIXTURES_DIR / fixture_name
        assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"
        assert parser.can_parse(fixture_file), f"Parser should detect META format for {fixture_name}"

    @pytest.mark.parametrize("fixture_name", TED_META_FIXTURES)
    def test_parse_meta_document(self, parser, fixture_name):
        """Test parsing TED META XML format document."""
        fixture_file = FIXTURES_DIR / fixture_name
        result = parser.parse_xml_file(fixture_file)

        # Validate result structure
        assert result is not None, f"Parser should return result for {fixture_name}"
        assert isinstance(result, TedParserResultModel), "Result should be TedParserResultModel"
        assert len(result.awards) > 0, f"Should extract at least one award from {fixture_name}"

        # Validate first award data
        award_data = result.awards[0]
        assert isinstance(award_data, TedAwardDataModel)

        # Validate document
        document = award_data.document
        assert isinstance(document, DocumentModel)
        assert document.doc_id, f"Document ID should be present in {fixture_name}"
        assert document.publication_date is not None, f"Publication date should be present in {fixture_name}"
        assert document.original_language, f"Original language should be present in {fixture_name}"
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
        assert parser.get_format_name() == "TED META XML"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
