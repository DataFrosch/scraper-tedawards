# TED Awards Scraper

A Python scraper for EU procurement contract award notices from [TED Europa](https://ted.europa.eu/). Processes XML-formatted TED data from **2008 onwards**.

## Features

- Scrapes TED award notice packages by year from **January 2008 onwards** (document type 7 only)
- Supports multiple XML formats:
  - TED META XML (2008-2010) - Early XML format in ZIP archives
  - TED INTERNAL_OJS R2.0.5 (2008) - Transitional INTERNAL_OJS wrapper format
  - TED 2.0 R2.0.7-R2.0.9 (2011-2024) - Standard TED XML formats
  - eForms UBL (2025+) - New EU eForms standard
- SQLite database with comprehensive procurement schema (PostgreSQL also supported)
- Processes TED packages by Official Journal issue number (not calendar dates)
- Smart stopping logic: automatically detects end of year (stops after 10 consecutive 404s)
- Automatic schema creation and reference data management
- Handles duplicate data gracefully with database-level deduplication

## Quick Start

1. **Setup environment**:
   ```bash
   # Install dependencies
   uv sync
   ```

2. **Scrape data**:
   ```bash
   # Scrape a full year (automatically finds all available packages)
   uv run tedawards scrape --year 2024

   # Scrape specific issue range within a year
   uv run tedawards scrape --year 2008 --start-issue 1 --max-issue 50

   # Backfill multiple years
   uv run tedawards backfill --start-year 2008 --end-year 2024

   # Scrape a specific package by number
   uv run tedawards package --package 200800001
   ```

3. **Query data**:
   ```bash
   # SQLite database is created at ./tedawards.db by default
   sqlite3 tedawards.db
   ```

## Configuration

Set environment variables in `.env`:
```env
DB_PATH=./tedawards.db          # SQLite database path (default: ./tedawards.db)
TED_DATA_DIR=./data              # Directory for downloaded packages (default: ./data)
LOG_LEVEL=INFO                   # Logging level (default: INFO)
```

## Database Schema

Key tables:
- `ted_documents` - Award notice metadata
- `contracting_bodies` - Organizations issuing contracts
- `contracts` - Procurement contracts
- `awards` - Award decisions and statistics
- `contractors` - Winning companies
- `award_contractors` - Award-contractor relationships

## Architecture

- **Parsers**: Automatically detects and processes multiple XML formats
  - `TedMetaXmlParser` - TED META XML format (2008-2010)
  - `TedInternalOjsParser` - TED INTERNAL_OJS R2.0.5 format (2008)
  - `TedV2Parser` - TED 2.0 R2.0.7/R2.0.8/R2.0.9 formats (2011-2024)
  - `EFormsUBLParser` - eForms UBL ContractAwardNotice (2025+)
- **Database**: SQLite with comprehensive procurement schema (PostgreSQL also supported)
- **Scraper**: Downloads and processes TED packages by Official Journal issue number (sequential, not calendar-based)
- **CLI**: Year-based commands for scraping and backfilling

## Package Numbering

TED packages use **Official Journal (OJ S) issue numbers**, not calendar dates:
- Format: `{year}{issue_number:05d}` (e.g., `200800001` = issue 1 of 2008)
- Issues are sequential but skip weekends/holidays
- Typical year has ~250 issues (not 365 days)
- Scraper automatically handles gaps by stopping after 10 consecutive 404s

## Data Coverage

- **Time Range**: January 2008 to present (17+ years of XML-formatted data)
- **Data Quality**:
  - ✅ 93.3% of awards include conclusion dates
  - ✅ All key procurement data extracted accurately
  - ✅ Handles multiple XML formats and variations
  - ✅ Consistent processing across different archive dates
- **Format Support**: All major TED XML formats (META XML, INTERNAL_OJS R2.0.5, R2.0.7-R2.0.9, eForms UBL)