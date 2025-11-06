# TED Awards Scraper - Claude Context

## Project Overview
TED Awards scraper for analyzing EU procurement contract awards. Focus is **only on award notices** (document type 7 - "Contract award notice").

## Tech Stack & Requirements
- **Development**: uv for dependency management
- **Database**: SQLite with SQLAlchemy ORM (easy PostgreSQL migration later)
- **Python**: >=3.12 with lxml, requests, sqlalchemy, pydantic, click

## Key Architecture Decisions
1. **Award-only focus**: Filter XML parsing to only process contract award notices
2. **Environment configuration**: All DB settings via env vars (.env for dev)
3. **Daily incremental**: Fetch daily archives we don't have yet
4. **Modular design**: Archive downloader, XML parser, DB handler, scheduler
5. **No fallbacks or defaults**: Only extract data directly from XML files - no defaults, no fallbacks, no default records. Missing data should be None in Python and NULL in database. If we cannot extract required data, skip the record entirely rather than creating defaults
6. **Fail-loud error handling**: Errors should always bubble up and cause loud failures. Never silently ignore errors or continue processing with partial data. Use proper exception handling but let errors propagate to calling code for proper error reporting and debugging. This includes:
   - **Never assume defaults**: If required data is missing (like original language for deduplication), raise an exception rather than assuming a default value
   - **Never gracefully degrade**: If data integrity cannot be guaranteed, fail immediately rather than producing potentially incorrect results
   - **Always validate critical assumptions**: If business logic depends on certain data being present, validate it exists and fail if it doesn't

## Data Source Details
- **URL Pattern**: `https://ted.europa.eu/packages/daily/{yyyynnnnn}` (e.g., 202400001)
- **File Format**: `.tar.gz` archives containing various formats by year
- **Rate Limits**: 3 concurrent downloads, 700 requests/min, 600 downloads per 6min/IP

### Supported File Formats
The scraper supports multiple TED document formats across different time periods:

1. **TED Text Format (2007 and earlier)**
   - Format: ZIP files containing structured text files with field-based format
   - Content: Multiple ZIP files per language (meta and utf8 variants)
   - Parser: `TedMetaXmlParser` - handles legacy text format with structured fields

2. **TED 2.0 (2008-2023)** - **Unified Parser**
   - **R2.0.7 (2008-2010)**: XML with CONTRACT_AWARD forms, early structure
   - **R2.0.8 (2011-2013)**: XML with CONTRACT_AWARD forms, enhanced structure
   - **R2.0.9 (2014-2023)**: XML with F03_2014 forms, modern structure
   - Format: XML with TED_EXPORT namespace
   - File naming: `{6-8digits}_{year}.xml` (e.g., 000248_2012.xml)
   - Parser: `TedV2Parser` - unified parser handling all TED 2.0 variants with automatic format detection

3. **eForms UBL ContractAwardNotice (2024+)**
   - Format: UBL-based XML with ContractAwardNotice schema
   - Namespace: `urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2`
   - Parser: `EFormsUBLParser` - handles new EU eForms standard

## Database Schema
Comprehensive SQLAlchemy models in `models.py` with tables:
- `ted_documents` - Main document metadata
- `contracting_bodies` - Purchasing organizations
- `contracts` - Procurement items
- `lots` - Contract subdivisions
- `awards` - Award decisions
- `contractors` - Winning companies
- Plus reference tables for CPV, NUTS, countries, etc.

Schema is automatically created by SQLAlchemy on first run.

## Format Detection & Parser Selection
The `ParserFactory` automatically detects and selects the appropriate parser:
- **Priority Order**: TedMetaXmlParser → TedV2Parser → EFormsUBLParser
- **Detection**: Each parser has a `can_parse()` method to identify compatible formats
- **File Types**: Handles both `.xml` files and `.ZIP` archives containing text data
- **TED 2.0 Auto-Detection**: The unified TedV2Parser automatically detects R2.0.7, R2.0.8, or R2.0.9 variants

### Archive Structure
- **Modern (2014+)**: `.tar.gz` containing individual XML files
- **Legacy Text (2007-2008)**: `.tar.gz` containing ZIP files with structured text data

## Key Data Structures by Format

### TED R2.0.9 (F03_2014 Award Notice)
- `TED_EXPORT/CODED_DATA_SECTION` - Document metadata
- `TED_EXPORT/FORM_SECTION/F03_2014` - Award notice data
  - `CONTRACTING_BODY` - Buyer info
  - `OBJECT_CONTRACT` - Contract details
  - `AWARD_CONTRACT` - Winner and value info

### TED Text Format (Legacy)
- Field-based format with codes like `TD:`, `PD:`, `AU:`, `TI:`, `TX:`
- `TD: 7 - Contract award` identifies award notices
- `ND` field provides universal document identifier across languages

## Development Commands
- `uv run tedawards scrape --date 2024-01-01`
- `uv run tedawards backfill --start-date 2024-01-01 --end-date 2024-01-31`

## Schema Management
- SQLAlchemy automatically creates schema on first run
- Database is a single SQLite file for easy portability
- Schema is defined in `models.py` using SQLAlchemy's declarative base

## Environment Variables
- `DB_PATH` - Path to SQLite database file (default: `./data/tedawards.db`)
- `TED_DATA_DIR` - Local storage for downloaded archives (default: `./data`)
- `LOG_LEVEL` - Logging configuration (default: `INFO`)