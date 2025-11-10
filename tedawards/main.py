import click
import logging
import os
from datetime import datetime
from .scraper import scrape_year, scrape_year_range, scrape_package

logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@click.group()
def cli():
    """TED Awards scraper for EU procurement contract awards."""
    pass

@cli.command()
@click.option('--year', type=int, required=True,
              help='Year to scrape (e.g., 2024)')
@click.option('--start-issue', type=int, default=None,
              help='Starting OJ issue number (default: auto-resume from last downloaded issue + 1, or 1 if none)')
@click.option('--max-issue', type=int, default=300,
              help='Maximum issue number to try (default: 300)')
@click.option('--force-reimport', is_flag=True,
              help='Reimport all data from already-downloaded archives (starts from issue 1)')
def scrape(year, start_issue, max_issue, force_reimport):
    """Scrape TED awards for a specific year.

    By default, skips already-downloaded packages and resumes from the next issue.
    Use --force-reimport to process all downloaded archives again (e.g., to rebuild database).
    """
    scrape_year(year, start_issue, max_issue, force_reimport=force_reimport)

@cli.command()
@click.option('--start-year', type=int, required=True,
              help='Start year for backfill (e.g., 2008)')
@click.option('--end-year', type=int, default=datetime.now().year,
              help='End year for backfill (default: current year)')
@click.option('--force-reimport', is_flag=True,
              help='Reimport all data from already-downloaded archives')
def backfill(start_year, end_year, force_reimport):
    """Backfill TED awards for a range of years.

    By default, skips already-downloaded packages and resumes each year from the next issue.
    Use --force-reimport to process all downloaded archives again (e.g., to rebuild database).
    """
    scrape_year_range(start_year, end_year, force_reimport=force_reimport)

if __name__ == '__main__':
    cli()