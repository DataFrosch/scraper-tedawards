"""
Tests for ParserFactory auto-detection.

The ParserFactory automatically detects and selects the appropriate parser
based on the file format. These tests validate:
1. Correct parser detection for all supported formats
2. Priority order (TED META XML → TED 2.0 → eForms UBL)
3. Support for both .xml files and .ZIP archives
"""

import pytest
from pathlib import Path

from tedawards.parsers.factory import ParserFactory
from tedawards.parsers.ted_meta_xml import TedMetaXmlParser
from tedawards.parsers.ted_v2 import TedV2Parser
from tedawards.parsers.eforms_ubl import EFormsUBLParser


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

# Import fixture lists from other test modules
TED_META_FIXTURES = [
    "ted_meta_2008_en.zip",
    "ted_meta_2009_en.zip",
    "ted_meta_2010_en.zip",
]

TED_V2_R207_FIXTURES = [
    "ted_v2_r2_0_7_2011.xml",
]

TED_V2_R208_FIXTURES = [
    "ted_v2_r2_0_8_2015.xml",
]

TED_V2_R209_FIXTURES = [
    "ted_v2_r2_0_9_2024.xml",
]

EFORMS_UBL_FIXTURES = [
    "eforms_ubl_2025.xml",
    "eforms_ubl_2025_alt.xml",
]


class TestParserFactory:
    """Tests for ParserFactory auto-detection."""

    @pytest.fixture
    def factory(self):
        """Create a parser factory instance."""
        return ParserFactory()

    @pytest.mark.parametrize("fixture_name", TED_META_FIXTURES)
    def test_factory_detects_ted_meta(self, factory, fixture_name):
        """Test factory auto-detects TED META XML format."""
        fixture_file = FIXTURES_DIR / fixture_name
        parser = factory.get_parser(fixture_file)
        assert parser is not None, f"Factory should return a parser for {fixture_name}"
        assert isinstance(parser, TedMetaXmlParser), f"Should detect TED META XML parser for {fixture_name}"

    @pytest.mark.parametrize("fixture_name", TED_V2_R207_FIXTURES)
    def test_factory_detects_ted_v2_r207(self, factory, fixture_name):
        """Test factory auto-detects TED 2.0 R2.0.7 format."""
        fixture_file = FIXTURES_DIR / fixture_name
        parser = factory.get_parser(fixture_file)
        assert parser is not None, f"Factory should return a parser for {fixture_name}"
        assert isinstance(parser, TedV2Parser), f"Should detect TED V2 parser for {fixture_name}"

    @pytest.mark.parametrize("fixture_name", TED_V2_R208_FIXTURES)
    def test_factory_detects_ted_v2_r208(self, factory, fixture_name):
        """Test factory auto-detects TED 2.0 R2.0.8 format."""
        fixture_file = FIXTURES_DIR / fixture_name
        parser = factory.get_parser(fixture_file)
        assert parser is not None, f"Factory should return a parser for {fixture_name}"
        assert isinstance(parser, TedV2Parser), f"Should detect TED V2 parser for {fixture_name}"

    @pytest.mark.parametrize("fixture_name", TED_V2_R209_FIXTURES)
    def test_factory_detects_ted_v2_r209(self, factory, fixture_name):
        """Test factory auto-detects TED 2.0 R2.0.9 format."""
        fixture_file = FIXTURES_DIR / fixture_name
        parser = factory.get_parser(fixture_file)
        assert parser is not None, f"Factory should return a parser for {fixture_name}"
        assert isinstance(parser, TedV2Parser), f"Should detect TED V2 parser for {fixture_name}"

    @pytest.mark.parametrize("fixture_name", EFORMS_UBL_FIXTURES)
    def test_factory_detects_eforms_ubl(self, factory, fixture_name):
        """Test factory auto-detects eForms UBL format."""
        fixture_file = FIXTURES_DIR / fixture_name
        parser = factory.get_parser(fixture_file)
        assert parser is not None, f"Factory should return a parser for {fixture_name}"
        assert isinstance(parser, EFormsUBLParser), f"Should detect eForms UBL parser for {fixture_name}"

    def test_factory_supported_formats(self, factory):
        """Test factory returns list of supported formats."""
        formats = factory.get_supported_formats()
        assert len(formats) >= 3, "Should support at least 3 formats"
        assert "TED META XML" in formats
        assert "TED 2.0" in formats
        assert "eForms UBL ContractAwardNotice" in formats


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
