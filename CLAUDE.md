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

## Data Source Details
- **URL Pattern**: `https://ted.europa.eu/packages/daily/{yyyynnnnn}` (e.g., 202400001)
- **File Format**: `.tar.gz` archives
- **XML Format**: TED R2.0.9 schema with F03_2014 form sections
- **Rate Limits**: 3 concurrent downloads, 700 requests/min, 600 downloads per 6min/IP
- **File Naming**: `{6-8digits}_{year}.xml` (e.g., 000001_2024.xml)

## Database Schema
Comprehensive schema in `schema.sql` with tables:
- `ted_documents` - Main document metadata
- `contracting_bodies` - Purchasing organizations
- `contracts` - Procurement items
- `lots` - Contract subdivisions
- `awards` - Award decisions
- `contractors` - Winning companies
- Plus reference tables for CPV, NUTS, countries, etc.

## Key XML Structure (F03_2014 Award Notice)
- `TED_EXPORT/CODED_DATA_SECTION` - Document metadata
- `TED_EXPORT/FORM_SECTION/F03_2014` - Award notice data
  - `CONTRACTING_BODY` - Buyer info
  - `OBJECT_CONTRACT` - Contract details
  - `AWARD_CONTRACT` - Winner and value info

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