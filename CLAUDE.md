# TED Awards Scraper - Claude Context

## Project Overview
TED Awards scraper for analyzing EU procurement contract awards. Focus is **only on award notices** (document type 7 - "Contract award notice").

## Tech Stack & Requirements
- **Development**: uv for dependency management, docker-compose for database
- **Production**: Docker container
- **Database**: PostgreSQL with environment-based configuration
- **Python**: >=3.12 with lxml, requests, psycopg2, pydantic, click

## Key Architecture Decisions
1. **Award-only focus**: Filter XML parsing to only process contract award notices
2. **Environment configuration**: All DB settings via env vars (.env for dev)
3. **Daily incremental**: Fetch daily archives we don't have yet
4. **Modular design**: Archive downloader, XML parser, DB handler, scheduler
5. **No fallbacks or defaults**: Only extract data directly from XML files - no defaults, no fallbacks, no default records. Missing data should be None in Python and NULL in database. If we cannot extract required data, skip the record entirely rather than creating defaults
6. **Fail-loud error handling**: Errors should always bubble up and cause loud failures. Never silently ignore errors or continue processing with partial data. Use proper exception handling but let errors propagate to calling code for proper error reporting and debugging

## Data Source Details
- **URL Pattern**: `https://ted.europa.eu/packages/daily/{yyyynnnnn}` (e.g., 202400001)
- **File Format**: `.tar.gz` archives containing various formats by year
- **Rate Limits**: 3 concurrent downloads, 700 requests/min, 600 downloads per 6min/IP

### Supported File Formats
The scraper supports multiple TED document formats across different time periods:

1. **TED Text Format (2007 and earlier)**
   - Format: ZIP files containing structured text files with field-based format
   - Content: Multiple ZIP files per language (meta and utf8 variants)
   - Parser: `TedTextParser` - handles legacy text format with structured fields

2. **TED R2.0.7 (2008-2013)**
   - Format: XML with TED_EXPORT namespace
   - Schema: Uses CONTRACT_AWARD forms instead of F03_2014
   - Parser: `TedR207Parser` - handles pre-2014 XML formats

3. **TED R2.0.9 (2014-2023)**
   - Format: XML with TED_EXPORT namespace and F03_2014 forms
   - File naming: `{6-8digits}_{year}.xml` (e.g., 000001_2024.xml)
   - Structure: `TED_EXPORT/CODED_DATA_SECTION` + `TED_EXPORT/FORM_SECTION/F03_2014`
   - Parser: `TedXmlParser` - main parser for modern XML format

4. **eForms UBL ContractAwardNotice (2024+)**
   - Format: UBL-based XML with ContractAwardNotice schema
   - Namespace: `urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2`
   - Parser: `EFormsUBLParser` - handles new EU eForms standard

## Database Schema
Comprehensive schema in `schema.sql` with tables:
- `ted_documents` - Main document metadata
- `contracting_bodies` - Purchasing organizations
- `contracts` - Procurement items
- `lots` - Contract subdivisions
- `awards` - Award decisions
- `contractors` - Winning companies
- Plus reference tables for CPV, NUTS, countries, etc.

## Format Detection & Parser Selection
The `ParserFactory` automatically detects and selects the appropriate parser:
- **Priority Order**: TedTextParser → TedXmlParser → EFormsUBLParser → TedR207Parser
- **Detection**: Each parser has a `can_parse()` method to identify compatible formats
- **File Types**: Handles both `.xml` files and `.ZIP` archives containing text data

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
- `docker-compose up -d` (database)

## Schema Management
- Scraper automatically creates schema on first run if database is empty
- No need for manual schema setup - just point at any PostgreSQL database

## Environment Variables
- `DATABASE_URL` or individual `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `TED_DATA_DIR` - Local storage for downloaded archives
- `LOG_LEVEL` - Logging configuration