# TED Awards Scraper - Claude Context

## Project Overview

TED Awards scraper for analyzing EU procurement contract awards from **2008 onwards**. Processes XML-formatted TED data, focusing **only on award notices** (document type 7 - "Contract award notice").

## Tech Stack & Requirements

- **Development**: uv for dependency management
- **Database**: SQLAlchemy ORM
- **Python**: >=3.12 with lxml, requests, sqlalchemy, pydantic, click

## Key Architecture Decisions

1. **Award-only focus**: Filter XML parsing to only process contract award notices
2. **Environment configuration**: All DB settings via env vars (.env for dev)
3. **Year-based scraping**: Scrape by year, iterating through sequential OJ issue numbers (not calendar dates)
4. **Database deduplication**: Use INSERT ... ON CONFLICT DO NOTHING for documents and contractors to handle duplicates
5. **No fallbacks or defaults**: Only extract data directly from XML files - no defaults, no fallbacks, no default records. Missing data should be None in Python and NULL in database. If we cannot extract required data, skip the record entirely rather than creating defaults
6. **Fail-loud error handling**: Errors should always bubble up and cause loud failures. Never silently ignore errors or continue processing with partial data. Use proper exception handling but let errors propagate to calling code for proper error reporting and debugging. This includes:
   - **Never assume defaults**: If required data is missing (like original language for deduplication), raise an exception rather than assuming a default value
   - **Never gracefully degrade**: If data integrity cannot be guaranteed, fail immediately rather than producing potentially incorrect results
   - **Always validate critical assumptions**: If business logic depends on certain data being present, validate it exists and fail if it doesn't
7. **Explicit data extraction**: Use built-in Python and standard library methods - no custom utility wrappers. Every assumption about data format must be explicit and testable:
   - **Prefer standard library**: Use built-in methods over custom implementations (e.g., Python's date parsing, lxml's text extraction)
   - **Explicit errors**: When parsing fails, error messages must show the actual data value that failed, not just generic messages
   - **Data quality first**: Code should reveal data quality issues, not paper over them with fallbacks

## Data Source Details

- **URL Pattern**: `https://ted.europa.eu/packages/daily/{yyyynnnnn}` where `nnnnn` is the Official Journal (OJ S) issue number (e.g., 202400001 = issue 1 of 2024)
- **Package Numbering**: Sequential issue numbers, NOT calendar days. Issues increment by 1 but skip weekends/holidays (e.g., 2008 ends at issue 253, not 366)
- **File Format**: `.tar.gz` archives containing XML documents
- **Coverage**: XML data from **January 2008 onwards** (earlier data uses non-XML formats not supported)
- **Rate Limits**: 3 concurrent downloads, 700 requests/min, 600 downloads per 6min/IP
- **Scraping Strategy**: Try sequential issue numbers starting from 1, stopping after 10 consecutive 404s (typical year has ~250 issues)

### Supported XML Formats

The scraper supports multiple TED XML document formats:

1. **TED META XML (2008-2010)**

   - **Format**: ZIP files containing structured XML text data
   - **Content**: Multiple ZIP files per language (meta_org.zip variants)
   - **Parser**: `TedMetaXmlParser` - handles early XML format with structured fields
   - **First available**: 2008-01-03
   - **Coverage**: 2008-2010 (overlaps with TED INTERNAL_OJS and early TED 2.0 formats)
   - **Language handling**: Daily archives contain **separate ZIP files for each language** (`EN_*.zip`, `DE_*.zip`, `FR_*.zip`, etc.)
     - Each ZIP contains documents in that specific language only
     - Parser only processes English files (`EN_*` files)
     - Language filter: `doc.get('lg', '').upper() == 'EN'`
     - Documents have `lg="en"` attribute (lowercase in META XML format)

2. **TED INTERNAL_OJS R2.0.5 (2008)**

   - **Format**: Individual XML files with language-specific extensions (`.en`, `.de`, `.fr`, etc.)
   - **Root element**: `<INTERNAL_OJS>` wrapper
   - **Award forms**: `<CONTRACT_AWARD_SUM>` with `<FD_CONTRACT_AWARD_SUM>` content
   - **Parser**: `TedInternalOjsParser` - handles INTERNAL_OJS wrapper format
   - **First available**: 2008 (specific dates vary)
   - **Coverage**: 2008 only (transitional format between META XML and TED 2.0)
   - **File structure**: Directories containing `{doc_id}_{year}.{lang}` files (e.g., `114495_2008.en`)
   - **Language handling**: Parser only processes `.en` files (English language)
     - Each document is a separate file per language
     - Award identification via `<NAT_NOTICE>7</NAT_NOTICE>` in `BIB_DOC_S` section
   - **Document ID pattern**: `ojs-{NO_DOC_OJS}` (e.g., `ojs-2008/S 85-114495`)

3. **TED 2.0 XML (2011-2024)** - **Unified Parser**

   - **Variants**:
     - **R2.0.7 (2011-2013)**: XML with CONTRACT_AWARD forms, early structure
     - **R2.0.8 (2014-2015)**: XML with CONTRACT_AWARD forms, enhanced structure
     - **R2.0.9 (2014-2024)**: XML with F03_2014 forms, modern structure
   - **Format**: XML with TED_EXPORT namespace
   - **File naming**: `{6-8digits}_{year}.xml` (e.g., 000248_2012.xml)
   - **Parser**: `TedV2Parser` - unified parser handling all TED 2.0 variants with automatic format detection
   - **First available**: 2011-01-04
   - **Language handling**: Daily archives contain **one XML file per document** in its **original language**
     - Each document includes:
       - `FORM_SECTION` with form in original language (e.g., `<F03_2014 LG="DE">`)
       - `TRANSLATION_SECTION` with **English labels** and translations for all EU languages
       - `CODED_DATA_SECTION` with English descriptions for CPV codes, NUTS, etc.
     - **Parser processes ALL documents regardless of original language**
     - Extracts English labels from `TRANSLATION_SECTION` when available
     - Example: A German procurement (`LG="DE"`) includes `<ML_TI_DOC LG="EN">` with English title
     - **CRITICAL**: Do NOT filter by language - would lose 95%+ of documents

4. **eForms UBL ContractAwardNotice (2025+)**
   - **Format**: UBL-based XML with ContractAwardNotice schema
   - **Namespace**: `urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2`
   - **Parser**: `EFormsUBLParser` - handles new EU eForms standard
   - **Language handling**: Similar to TED 2.0 - one file per document with language markers in `languageID` attributes
     - **Parser processes ALL documents regardless of language**

## Database Architecture

Database setup handled directly in `scraper.py`:

- Engine and session factory created at module level from environment variables
- `get_session()` context manager for transaction management with automatic commit/rollback
- Schema automatically created on scraper initialization

SQLAlchemy models in `models.py`:

- `ted_documents` - Main document metadata (PK: doc_id)
- `contracting_bodies` - Purchasing organizations
- `contracts` - Procurement items
- `lots` - Contract subdivisions
- `awards` - Award decisions
- `contractors` - Winning companies (unique constraint on official_name + country_code)
- Reference tables for CPV, NUTS, countries, etc.

Deduplication handled via unique constraints and `INSERT ... ON CONFLICT DO NOTHING` (works with both SQLite and PostgreSQL).

## Format Detection & Parser Selection

The `ParserFactory` automatically detects and selects the appropriate parser:

- **Priority Order**: TedMetaXmlParser → TedInternalOjsParser → TedV2Parser → EFormsUBLParser
- **Detection**: Each parser has a `can_parse()` method to identify compatible formats
- **File Types**: Handles `.xml` files, `.en` files, and `.ZIP` archives
- **TED 2.0 Auto-Detection**: The unified TedV2Parser automatically detects R2.0.7, R2.0.8, or R2.0.9 variants

### Archive Structure

- **TED 2.0 (2011+)**: `.tar.gz` containing individual `.xml` files with TED_EXPORT namespace
- **TED INTERNAL_OJS (2008)**: `.tar.gz` containing directories with language-specific files (`.en`, `.de`, etc.)
- **TED META XML (2008-2010)**: `.tar.gz` containing ZIP files (`*_meta_org.zip`) with structured XML data

## Key XML Data Structures

### TED 2.0 R2.0.9 (F03_2014 Award Notice)

- `TED_EXPORT/CODED_DATA_SECTION` - Document metadata
- `TED_EXPORT/FORM_SECTION/F03_2014` - Award notice data
  - `CONTRACTING_BODY` - Buyer info
  - `OBJECT_CONTRACT` - Contract details
  - `AWARD_CONTRACT` - Winner and value info

### TED META XML Format

- ZIP-based XML format with structured fields
- `TD: 7 - Contract award` identifies award notices
- `ND` field provides universal document identifier across languages

## Development Commands

```bash
# Scrape a full year (tries all issues 1-300, stops after 10 consecutive 404s)
uv run tedawards scrape --year 2024

# Scrape specific issue range within a year
uv run tedawards scrape --year 2008 --start-issue 1 --max-issue 50

# Backfill multiple years
uv run tedawards backfill --start-year 2008 --end-year 2024

# Scrape a specific package by number
uv run tedawards package --package 200800001
```

## Code Organization

- `scraper.py` - Main scraper with database setup and session management
- `models.py` - SQLAlchemy ORM models with schema definitions
- `schema.py` - Pydantic models for data validation
- `parsers/` - Format-specific XML parsers
- `main.py` - CLI interface

## Environment Variables

- `DB_PATH` - Path to SQLite database file (default: `./tedawards.db`)
- `TED_DATA_DIR` - Local storage for downloaded archives (default: `./data`)
- `LOG_LEVEL` - Logging configuration (default: `INFO`)
