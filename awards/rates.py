"""Fetch and store ECB exchange rates and Eurostat HICP price indices."""

import csv
import io
import logging

import requests
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from .models import ExchangeRate, PriceIndex
from .db import get_session, init_db

logger = logging.getLogger(__name__)

# --- Module-level Core upsert statements ---

_er_ins = pg_insert(ExchangeRate.__table__)
_upsert_er = _er_ins.on_conflict_do_update(
    constraint="uq_exchange_rate",
    set_={"rate": _er_ins.excluded.rate},
)

_pi_ins = pg_insert(PriceIndex.__table__)
_upsert_pi = _pi_ins.on_conflict_do_update(
    index_elements=["year"],
    set_={"index_value": _pi_ins.excluded.index_value},
)


def _get_award_currencies(session: Session) -> list[str]:
    """Get distinct non-EUR currencies from awards table."""
    from .models import Award

    rows = session.execute(
        select(Award.awarded_value_currency)
        .where(Award.awarded_value_currency.isnot(None))
        .where(Award.awarded_value_currency != "EUR")
        .distinct()
    ).all()
    return sorted(r[0] for r in rows)


def fetch_ecb_rates(
    currencies: list[str], start_year: int, end_year: int
) -> list[dict]:
    """Fetch monthly average exchange rates from ECB for given currencies.

    ECB convention: rate = "1 EUR = X units of currency".
    """
    if not currencies:
        logger.info("No non-EUR currencies found, skipping ECB fetch")
        return []

    currency_str = "+".join(currencies)
    url = (
        f"https://data-api.ecb.europa.eu/service/data/EXR/"
        f"M.{currency_str}.EUR.SP00.A"
        f"?startPeriod={start_year}-01&endPeriod={end_year}-12"
        f"&detail=dataonly"
    )

    logger.info(
        f"Fetching ECB rates for {len(currencies)} currencies ({start_year}-{end_year})"
    )
    response = requests.get(url, headers={"Accept": "text/csv"}, timeout=60)
    response.raise_for_status()

    rows = []
    reader = csv.DictReader(io.StringIO(response.text))
    for row in reader:
        time_period = row["TIME_PERIOD"]
        year, month = time_period.split("-")
        rows.append(
            {
                "currency": row["CURRENCY"],
                "year": int(year),
                "month": int(month),
                "rate": row["OBS_VALUE"],
            }
        )

    logger.info(f"Fetched {len(rows)} exchange rate observations")
    return rows


def fetch_hicp(start_year: int, end_year: int) -> list[dict]:
    """Fetch annual average HICP index for euro area from Eurostat."""
    url = (
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_aind"
        f"?format=JSON&geo=EA20&coicop=CP00&unit=INX_A_AVG"
        f"&sinceTimePeriod={start_year}&untilTimePeriod={end_year}"
    )

    logger.info(f"Fetching Eurostat HICP ({start_year}-{end_year})")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    data = response.json()

    # JSON-stat 2.0: time dimension maps index positions to period labels
    time_dim = data["dimension"]["time"]["category"]["index"]
    values = data["value"]

    rows = []
    for period, idx in time_dim.items():
        str_idx = str(idx)
        if str_idx in values:
            rows.append({"year": int(period), "index_value": values[str_idx]})

    logger.info(f"Fetched {len(rows)} HICP index values")
    return rows


def save_exchange_rates(session: Session, rows: list[dict]) -> int:
    """Upsert exchange rate rows. Returns count."""
    if not rows:
        return 0
    session.execute(_upsert_er, rows)
    return len(rows)


def save_price_indices(session: Session, rows: list[dict]) -> int:
    """Upsert price index rows. Returns count."""
    if not rows:
        return 0
    session.execute(_upsert_pi, rows)
    return len(rows)


def update_rates(start_year: int, end_year: int):
    """Fetch ECB exchange rates and Eurostat HICP, upsert into database."""
    init_db()

    with get_session() as session:
        currencies = _get_award_currencies(session)

    # Fetch data (outside transaction)
    ecb_rows = fetch_ecb_rates(currencies, start_year, end_year)
    hicp_rows = fetch_hicp(start_year, end_year)

    # Save in a single transaction
    with get_session() as session:
        er_count = save_exchange_rates(session, ecb_rows)
        pi_count = save_price_indices(session, hicp_rows)

    logger.info(f"Saved {er_count} exchange rates and {pi_count} price indices")
