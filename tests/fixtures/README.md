# TED Awards Test Fixtures

This directory contains test fixtures representing different TED document formats for testing the parsers. Each fixture is a real contract award notice from TED archives.

## Available Fixtures

### TED ISO Legacy Format (1995-2007) - **MISSING PARSER**
These are ZIP archives containing structured field-based text data in original TED format.

- **`ted_iso_legacy_1999_en.zip`** - English ISO format from 1999-01-02
- **`ted_iso_legacy_2003_en.zip`** - English ISO format from 2003-01-02
- **`ted_iso_legacy_2006_utf8_lt.zip`** - Lithuanian UTF8 format from 2006-01-04 (same structure as ISO)

**Parser**: ❌ **MISSING** - `TedIsoLegacyParser` needed
**File Patterns**:
- `*_ISO_ORG.zip` or `*_ISO_ORG.ZIP` (1995-2007)
- `*_UTF8_ORG.zip` or `*_UTF8_ORG.ZIP` (2006-2007) - **Same format, different encoding**

**Contains**: Structured field-based text data with codes like `TD:`, `ND:`, `AU:`, `TX:`
**Format Example**:
```
TI: SE-Gothenburg: safety installations
PD: 20030102
ND: 59-2003
TD: 7 - Contract award
NC: 1 - Public works contract
AU: TRAFIKKONTORET
```

### TED META Format (2008-2013)
These are ZIP archives containing structured text data in META XML format.

- **`ted_meta_2008_en.zip`** - English META format from 2008-01-03
- **`ted_meta_2010_cs.zip`** - Czech META format from 2010-01-02

**Parser**: `TedMetaXmlParser`
**File Pattern**: `*_meta_org.zip`
**Contains**: XML structured text data in ZIP archives

### TED 2.0 XML Formats (2008-2023)
These are XML files using the TED_EXPORT schema with different variants.

- **`ted_v2_r2_0_7_2011.xml`** - TED R2.0.7 format from 2011-01-04
  - Schema: `R2.0.7.S03.E01`
  - Forms: `CONTRACT_AWARD`
  - Document ID: `000755-2011`

- **`ted_v2_r2_0_8_2015.xml`** - TED R2.0.8 format from 2015-01-02
  - Schema: `R2.0.8.S02.E01`
  - Forms: `CONTRACT_AWARD`
  - Document ID: `001077-2015`

- **`ted_v2_r2_0_9_2024.xml`** - TED R2.0.9 format from 2024-01-02
  - Schema: `R2.0.9.S05.E01`
  - Forms: `F03_2014`
  - Document ID: `000769-2024`

**Parser**: `TedV2Parser` (unified parser for all variants)
**File Pattern**: `*.xml`
**Root Element**: `TED_EXPORT` with namespace `http://publications.europa.eu/TED_schema/Export`

### eForms UBL ContractAwardNotice (2025+)
These are XML files using the new EU eForms UBL standard.

- **`eforms_ubl_2025.xml`** - eForms UBL format from 2025-01-02
  - Schema: UBL ContractAwardNotice-2
  - Root: `ContractAwardNotice`
  - Document ID: `00000372_2025`

**Parser**: `EFormsUBLParser`
**File Pattern**: `*.xml`
**Root Element**: `ContractAwardNotice` with namespace `urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2`

## Missing Formats

Based on the analysis of downloaded data from 1994-2025, the following formats were identified but **do not have parsers yet**:

### 1. TED ISO Legacy Format (1995-2007)
- **Years**: 1995-2007
- **File Patterns**:
  - `*_ISO_ORG.zip` or `*_ISO_ORG.ZIP` (1995-2007)
  - `*_UTF8_ORG.zip` or `*_UTF8_ORG.ZIP` (2006-2007) - **Same format**
- **Status**: ❌ **MISSING PARSER** - Contains valuable contract award data
- **Format**: Structured field-based text with codes like `TD:`, `ND:`, `AU:`, `TX:`
- **Value**: Contains 12+ years of contract awards not accessible by current parsers
- **Priority**: **HIGH** - Large data gap covering early EU procurement
- **Note**: UTF8 and ISO variants are identical structure, just different languages/encoding

### 2. eForms UBL Coverage Note
- **Status**: ✅ **Found in 2025 data** - eForms transition occurred in 2025
- **Coverage**: 2024 still uses TED 2.0 R2.0.9, 2025+ uses eForms UBL
- **Parser**: Already implemented and working

## Parser Coverage

| Format | Years | Parser Available | Status | Priority |
|--------|-------|------------------|---------|----------|
| TED ISO Legacy | 1995-2007 | ❌ **MISSING** | Downloaded, not parsed | **HIGH** |
| TED Text (META) | 2008-2013 | ✅ Yes | `TedMetaXmlParser` | Complete |
| TED 2.0 R2.0.7 | 2008-2010 | ✅ Yes | `TedV2Parser` | Complete |
| TED 2.0 R2.0.8 | 2011-2013 | ✅ Yes | `TedV2Parser` | Complete |
| TED 2.0 R2.0.9 | 2014-2024 | ✅ Yes | `TedV2Parser` | Complete |
| eForms UBL | 2025+ | ✅ Yes | `EFormsUBLParser` | Complete |

## Usage in Tests

```python
# Example usage in pytest
import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"

def test_ted_iso_legacy_parser():
    fixture_file = FIXTURES_DIR / "ted_iso_legacy_2003_en.zip"
    # Test TedIsoLegacyParser (MISSING - needs implementation)

def test_ted_meta_xml_parser():
    fixture_file = FIXTURES_DIR / "ted_meta_2008_en.zip"
    # Test TedMetaXmlParser with this fixture

def test_ted_v2_r207_parser():
    fixture_file = FIXTURES_DIR / "ted_v2_r2_0_7_2011.xml"
    # Test TedV2Parser with R2.0.7 format

def test_ted_v2_r208_parser():
    fixture_file = FIXTURES_DIR / "ted_v2_r2_0_8_2015.xml"
    # Test TedV2Parser with R2.0.8 format

def test_ted_v2_r209_parser():
    fixture_file = FIXTURES_DIR / "ted_v2_r2_0_9_2024.xml"
    # Test TedV2Parser with R2.0.9 format

def test_eforms_ubl_parser():
    fixture_file = FIXTURES_DIR / "eforms_ubl_2025.xml"
    # Test EFormsUBLParser with eForms UBL format
```

## Data Collection Details

The fixtures were created by downloading TED archives for January 1st of each year from 1994-2025 using:

```bash
LOG_LEVEL=INFO uv run tedawards scrape --date YYYY-01-01
```

Files were selected based on:
1. Being actual contract award notices (document type 7)
2. Being in original language (not translations)
3. Representative examples of each format variant
4. Successful parsing by existing parsers

Total data downloaded: ~32 years of daily archives
Successfully parsed years: 2008-2024 (TED text and TED 2.0 formats)
**Major Gap**: 1995-2007 (12+ years) requires new TED ISO Legacy parser

## Summary

**✅ Complete Coverage:** 2008-2025 (18 years) - All major formats supported
**❌ Missing Coverage:** 1995-2007 (12+ years) - TED ISO Legacy format needs parser
**🎉 eForms Found:** 2025 data contains actual eForms UBL format - transition complete!