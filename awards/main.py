import click
import logging
import os
from datetime import datetime
from .db import refresh_materialized_view
from .rates import update_rates

from .portals import PORTALS

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def _resolve_portals(portal_arg: str | None) -> list:
    """Resolve --portal argument to list of portal objects."""
    if portal_arg is None:
        return list(PORTALS.values())
    names = [n.strip() for n in portal_arg.split(",")]
    result = []
    for name in names:
        if name not in PORTALS:
            raise click.BadParameter(
                f"Unknown portal '{name}'. Available: {', '.join(PORTALS)}"
            )
        result.append(PORTALS[name])
    return result


@click.group()
def cli():
    """Procurement awards scraper."""
    # Table creation happens in each command's entry point (download_year,
    # import_year, update_rates) so the group callback — and plain CLI usage
    # like --help — does not require a live database.
    pass


@cli.command()
@click.option("--start-year", type=int, required=True, help="Start year")
@click.option("--end-year", type=int, help="End year (default: current year)")
@click.option(
    "--portal",
    type=str,
    default=None,
    help="Comma-separated portal names (default: all)",
)
def download(start_year, end_year, portal):
    """Download packages without importing to database.

    Skips packages that are already downloaded.
    """
    if end_year is None:
        end_year = datetime.now().year
    for p in _resolve_portals(portal):
        p.download(start_year, end_year)


@cli.command(name="import")
@click.option("--start-year", type=int, required=True, help="Start year")
@click.option("--end-year", type=int, help="End year (default: current year)")
@click.option(
    "--portal",
    type=str,
    default=None,
    help="Comma-separated portal names (default: all)",
)
def import_cmd(start_year, end_year, portal):
    """Import downloaded packages into the database."""
    if end_year is None:
        end_year = datetime.now().year
    for p in _resolve_portals(portal):
        p.import_data(start_year, end_year)
    refresh_materialized_view()


@cli.command(name="update-rates")
@click.option("--start-year", type=int, default=2011, help="Start year (default: 2011)")
@click.option("--end-year", type=int, help="End year (default: current year)")
def update_rates_cmd(start_year, end_year):
    """Fetch ECB exchange rates and Eurostat HICP data."""
    if end_year is None:
        end_year = datetime.now().year
    update_rates(start_year, end_year)
    refresh_materialized_view()


if __name__ == "__main__":
    cli()
