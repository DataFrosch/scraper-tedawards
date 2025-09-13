import click
import logging
from datetime import datetime, date
from .config import config
from .scraper import TedScraper

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@click.group()
def cli():
    """TED Awards scraper for EU procurement contract awards."""
    pass

@cli.command()
@click.option('--date', type=click.DateTime(['%Y-%m-%d']), default=str(date.today()),
              help='Date to scrape (YYYY-MM-DD), defaults to today')
def scrape(date):
    """Scrape TED awards for a specific date."""
    scraper = TedScraper()
    scraper.scrape_date(date.date())

@cli.command()
@click.option('--start-date', type=click.DateTime(['%Y-%m-%d']), required=True,
              help='Start date for backfill (YYYY-MM-DD)')
@click.option('--end-date', type=click.DateTime(['%Y-%m-%d']), default=str(date.today()),
              help='End date for backfill (YYYY-MM-DD), defaults to today')
def backfill(start_date, end_date):
    """Backfill TED awards for a date range."""
    scraper = TedScraper()
    scraper.backfill_range(start_date.date(), end_date.date())

if __name__ == '__main__':
    cli()