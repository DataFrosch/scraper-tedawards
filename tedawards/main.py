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
@click.option('--start-issue', type=int, default=1,
              help='Starting OJ issue number (default: 1)')
@click.option('--max-issue', type=int, default=300,
              help='Maximum issue number to try (default: 300)')
def scrape(year, start_issue, max_issue):
    """Scrape TED awards for a specific year."""
    scrape_year(year, start_issue, max_issue)

@cli.command()
@click.option('--start-year', type=int, required=True,
              help='Start year for backfill (e.g., 2008)')
@click.option('--end-year', type=int, default=datetime.now().year,
              help='End year for backfill (default: current year)')
def backfill(start_year, end_year):
    """Backfill TED awards for a range of years."""
    scrape_year_range(start_year, end_year)

@cli.command()
@click.option('--package', type=int, required=True,
              help='Package number to scrape (e.g., 202400001)')
def package(package):
    """Scrape a specific TED package by number."""
    count = scrape_package(package)
    if count > 0:
        click.echo(f"Successfully processed {count} award notices")
    else:
        click.echo("No award notices found or package not available")

if __name__ == '__main__':
    cli()