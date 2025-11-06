# TED Awards Scraper

A Python scraper for EU procurement contract award notices from [TED Europa](https://ted.europa.eu/). Processes XML-formatted TED data from **2008 onwards**.

## Features

- Scrapes daily TED award notice archives from **January 2008 onwards** (document type 7 only)
- Supports multiple XML formats:
  - TED META XML (2008-2010) - Early XML format
  - TED 2.0 R2.0.7-R2.0.9 (2011-2024) - Standard TED XML formats
  - eForms UBL (2025+) - New EU eForms standard
- Comprehensive PostgreSQL database schema for procurement data
- Processes 300-900+ award notices per day (varies by TED archive content)
- Extracts thousands of awards and contractors from daily archives
- Automatic schema creation and reference data management
- Docker development environment

## Quick Start

1. **Setup environment**:
   ```bash
   # Start PostgreSQL
   docker-compose up -d

   # Install dependencies
   uv sync
   ```

2. **Scrape data**:
   ```bash
   # Scrape specific date (2008-01-03 onwards)
   uv run tedawards scrape --date 2024-01-01

   # Backfill date range
   uv run tedawards backfill --start-date 2024-01-01 --end-date 2024-01-07
   ```

3. **Query data**:
   ```bash
   docker-compose exec postgres psql -U tedawards -d tedawards
   ```

## Configuration

Set environment variables in `.env`:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tedawards
DB_USER=tedawards
DB_PASSWORD=password
DATA_DIR=./data
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
  - `TedV2Parser` - TED 2.0 R2.0.7/R2.0.8/R2.0.9 formats (2011-2024)
  - `EFormsUBLParser` - eForms UBL ContractAwardNotice (2025+)
- **Database**: PostgreSQL with comprehensive procurement schema (SQLite also supported)
- **Scraper**: Downloads and processes daily TED archives from 2008 onwards
- **CLI**: Simple commands for scraping and backfilling

## Data Coverage

- **Time Range**: January 2008 to present (17+ years of XML-formatted data)
- **Data Quality**:
  - ✅ 93.3% of awards include conclusion dates
  - ✅ All key procurement data extracted accurately
  - ✅ Handles multiple XML formats and variations
  - ✅ Consistent processing across different archive dates
- **Format Support**: All major TED XML formats (META, R2.0.7-R2.0.9, eForms UBL)