# TED Awards Scraper

A Python scraper for EU procurement contract award notices from [TED Europa](https://ted.europa.eu/).

## Features

- Scrapes daily TED award notice archives (document type 7 only)
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
   # Scrape specific date
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

- **Parser**: Extracts data from TED XML R2.0.9 format
- **Database**: PostgreSQL with comprehensive procurement schema
- **Scraper**: Downloads and processes daily TED archives
- **CLI**: Simple commands for scraping and backfilling

## Data Quality

- ✅ 93.3% of awards include conclusion dates
- ✅ All key procurement data extracted accurately
- ✅ Handles multiple date formats and XML variations
- ✅ Consistent processing across different archive dates
- ✅ Filters TED R2.0.9 format (ignores newer eForms format)