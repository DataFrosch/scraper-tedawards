"""Tests for TED INTERNAL_OJS R2.0.5 parser."""

import pytest
from pathlib import Path
from tedawards.parsers.ted_internal_ojs import TedInternalOjsParser


@pytest.fixture
def parser():
    """Create parser instance."""
    return TedInternalOjsParser()


@pytest.fixture
def sample_file():
    """Get path to sample INTERNAL_OJS file."""
    return Path(__file__).parent.parent / "fixtures" / "ted_internal_ojs_r2_0_5_2008.en"


def test_can_parse_internal_ojs_format(parser, sample_file):
    """Test that parser correctly identifies INTERNAL_OJS format."""
    assert parser.can_parse(sample_file) is True


def test_cannot_parse_non_en_files(parser, tmp_path):
    """Test that parser rejects non-.en files."""
    # Create a temporary .de file
    de_file = tmp_path / "test.de"
    de_file.write_text('<?xml version="1.0"?><INTERNAL_OJS><BIB_DOC_S><NAT_NOTICE>7</NAT_NOTICE></BIB_DOC_S><CONTRACT_AWARD_SUM/></INTERNAL_OJS>')

    assert parser.can_parse(de_file) is False


def test_cannot_parse_non_award_notice(parser, tmp_path):
    """Test that parser rejects non-award notices."""
    # Create a file with NAT_NOTICE != 7
    non_award = tmp_path / "test.en"
    non_award.write_text('<?xml version="1.0"?><INTERNAL_OJS><BIB_DOC_S><NAT_NOTICE>2</NAT_NOTICE></BIB_DOC_S><CONTRACT_AWARD_SUM/></INTERNAL_OJS>')

    assert parser.can_parse(non_award) is False


def test_get_format_name(parser):
    """Test format name is correct."""
    assert parser.get_format_name() == "TED INTERNAL_OJS R2.0.5"


def test_parse_xml_file(parser, sample_file):
    """Test parsing of INTERNAL_OJS file."""
    result = parser.parse_xml_file(sample_file)

    assert result is not None
    assert len(result.awards) == 1

    award_data = result.awards[0]

    # Check document info
    assert award_data.document.doc_id == "ojs-2008/S 85-114495"
    assert award_data.document.reception_id == "2008/S 85-114495"
    assert award_data.document.source_country == "LT"
    assert str(award_data.document.publication_date) == "2008-05-02"
    assert str(award_data.document.dispatch_date) == "2008-04-28"
    assert str(award_data.document.deletion_date) == "2008-08-06"

    # Check contracting body
    assert award_data.contracting_body.official_name == "Kauno kolegija"
    assert award_data.contracting_body.town == "Kaunas"
    assert award_data.contracting_body.postal_code == "50468"
    assert award_data.contracting_body.country_code == "LT"
    assert award_data.contracting_body.email == "lina.kuraitiene@fc.kauko.lt"
    assert award_data.contracting_body.phone == "(370-37) 35 23 24"

    # Check contract info
    assert award_data.contract.title == "LT-Kaunas: software"
    assert award_data.contract.reference_number == "2008/S 85-114495"
    assert award_data.contract.main_cpv_code == "30240000"
    assert award_data.contract.short_description == "Software."
    assert award_data.contract.total_value == 16425.6
    assert award_data.contract.total_value_currency == "LTL"

    # Check awards
    assert len(award_data.awards) == 1
    award = award_data.awards[0]
    assert award.contract_number == "1"
    assert award.awarded_value == 16425.6
    assert award.awarded_value_currency == "LTL"

    # Check contractors
    assert len(award.contractors) == 1
    contractor = award.contractors[0]
    assert contractor.official_name == 'UAB "Innovation Computer Group"'
    assert contractor.town == "Kaunas"
    assert contractor.postal_code == "50122"
    assert contractor.country_code == "LT"
    assert contractor.email == "kaunas.verslas@icg.lt"
    assert contractor.phone == "(370-37) 32 80 29"


def test_parse_value_formatting(parser):
    """Test value parsing with different formats."""
    # Test space-separated thousands
    assert parser._parse_value("16 425,6") == 16425.6

    # Test plain number
    assert parser._parse_value("16425.6") == 16425.6

    # Test comma as decimal separator
    assert parser._parse_value("16425,60") == 16425.60

    # Test invalid value
    assert parser._parse_value("invalid") is None

    # Test empty value
    assert parser._parse_value("") is None
