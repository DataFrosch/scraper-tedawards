"""Tests for rates.py — ECB exchange rates and Eurostat HICP fetching/saving."""

import pytest
from datetime import date
from decimal import Decimal
from unittest.mock import patch, Mock

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from awards.models import (
    Base,
    ExchangeRate,
    PriceIndex,
    Award,
    Contract,
    Document,
    Organization,
)
from awards.rates import (
    fetch_ecb_rates,
    fetch_hicp,
    save_exchange_rates,
    save_price_indices,
    update_rates,
    _get_award_currencies,
)
from awards.db import create_materialized_view, refresh_materialized_view

TEST_DATABASE_URL = "postgresql://awards:awards@localhost:5433/awards_test"

# --- Canned responses ---

ECB_CSV = """\
KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE
EXR.M.GBP.EUR.SP00.A,M,GBP,EUR,SP00,A,2024-01,0.86115
EXR.M.GBP.EUR.SP00.A,M,GBP,EUR,SP00,A,2024-02,0.85535
EXR.M.SEK.EUR.SP00.A,M,SEK,EUR,SP00,A,2024-01,11.2845
EXR.M.SEK.EUR.SP00.A,M,SEK,EUR,SP00,A,2024-02,11.1937
"""

EUROSTAT_JSON = {
    "version": "2.0",
    "label": "test",
    "source": "Eurostat",
    "updated": "2024-01-01",
    "id": ["unit", "coicop", "geo", "time"],
    "size": [1, 1, 1, 3],
    "dimension": {
        "unit": {"label": "unit", "category": {"index": {"INX_A_AVG": 0}}},
        "coicop": {"label": "coicop", "category": {"index": {"CP00": 0}}},
        "geo": {"label": "geo", "category": {"index": {"EA20": 0}}},
        "time": {
            "label": "time",
            "category": {"index": {"2022": 0, "2023": 1, "2024": 2}},
        },
    },
    "value": {"0": 113.93, "1": 120.59, "2": 123.43},
}


@pytest.fixture
def test_db():
    """Create a PostgreSQL test database with fresh tables for each test."""
    engine = create_engine(TEST_DATABASE_URL, echo=False)
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with (
        patch("awards.db.engine", engine),
        patch("awards.db.SessionLocal", SessionLocal),
    ):
        yield engine

    engine.dispose()


class TestFetchEcbRates:
    """Tests for fetch_ecb_rates with mocked HTTP."""

    def test_parses_csv_response(self):
        mock_resp = Mock()
        mock_resp.text = ECB_CSV
        mock_resp.raise_for_status = Mock()

        with patch("awards.rates.requests.get", return_value=mock_resp):
            rows = fetch_ecb_rates(["GBP", "SEK"], 2024, 2024)

        assert len(rows) == 4
        gbp_jan = next(r for r in rows if r["currency"] == "GBP" and r["month"] == 1)
        assert gbp_jan["year"] == 2024
        assert gbp_jan["rate"] == "0.86115"

    def test_empty_currencies_returns_empty(self):
        rows = fetch_ecb_rates([], 2024, 2024)
        assert rows == []


class TestFetchHicp:
    """Tests for fetch_hicp with mocked HTTP."""

    def test_parses_json_stat_response(self):
        mock_resp = Mock()
        mock_resp.json.return_value = EUROSTAT_JSON
        mock_resp.raise_for_status = Mock()

        with patch("awards.rates.requests.get", return_value=mock_resp):
            rows = fetch_hicp(2022, 2024)

        assert len(rows) == 3
        row_2024 = next(r for r in rows if r["year"] == 2024)
        assert row_2024["index_value"] == 123.43


class TestSaveExchangeRates:
    """Tests for save_exchange_rates with test database."""

    def test_upsert_idempotent(self, test_db):
        from awards.db import SessionLocal

        rows = [
            {"currency": "GBP", "year": 2024, "month": 1, "rate": "0.86115"},
            {"currency": "GBP", "year": 2024, "month": 2, "rate": "0.85535"},
        ]

        session = SessionLocal()
        try:
            save_exchange_rates(session, rows)
            session.commit()
        finally:
            session.close()

        # Upsert again with updated rate
        updated_rows = [
            {"currency": "GBP", "year": 2024, "month": 1, "rate": "0.87000"},
        ]
        session = SessionLocal()
        try:
            save_exchange_rates(session, updated_rows)
            session.commit()
        finally:
            session.close()

        # Verify: 2 rows total, first row updated
        session = SessionLocal()
        try:
            all_rows = session.execute(select(ExchangeRate)).scalars().all()
            assert len(all_rows) == 2
            jan = next(r for r in all_rows if r.month == 1)
            assert jan.rate == Decimal("0.870000")
        finally:
            session.close()


class TestSavePriceIndices:
    """Tests for save_price_indices with test database."""

    def test_upsert_idempotent(self, test_db):
        from awards.db import SessionLocal

        rows = [
            {"year": 2023, "index_value": 120.59},
            {"year": 2024, "index_value": 123.43},
        ]

        session = SessionLocal()
        try:
            save_price_indices(session, rows)
            session.commit()
        finally:
            session.close()

        # Upsert again with updated value
        updated = [{"year": 2024, "index_value": 124.00}]
        session = SessionLocal()
        try:
            save_price_indices(session, updated)
            session.commit()
        finally:
            session.close()

        session = SessionLocal()
        try:
            all_rows = session.execute(select(PriceIndex)).scalars().all()
            assert len(all_rows) == 2
            pi_2024 = next(r for r in all_rows if r.year == 2024)
            assert pi_2024.index_value == Decimal("124.0000")
        finally:
            session.close()


class TestGetAwardCurrencies:
    """Tests for _get_award_currencies."""

    def test_returns_distinct_non_eur(self, test_db):
        from awards.db import SessionLocal

        session = SessionLocal()
        try:
            # Create minimal data: contracting body -> document -> contract -> awards
            cb = Organization(official_name="Test Org")
            session.add(cb)
            session.flush()

            doc = Document(
                doc_id="test-1",
                publication_date=date(2024, 1, 1),
                buyer_organization_id=cb.id,
            )
            session.add(doc)
            session.flush()

            contract = Contract(doc_id="test-1", title="Test")
            session.add(contract)
            session.flush()

            session.add(Award(contract_id=contract.id, awarded_value_currency="GBP"))
            session.add(Award(contract_id=contract.id, awarded_value_currency="SEK"))
            session.add(Award(contract_id=contract.id, awarded_value_currency="EUR"))
            session.add(Award(contract_id=contract.id, awarded_value_currency="GBP"))
            session.add(Award(contract_id=contract.id, awarded_value_currency=None))
            session.commit()

            currencies = _get_award_currencies(session)
            assert currencies == ["GBP", "SEK"]
        finally:
            session.close()


class TestUpdateRates:
    """Integration test for update_rates."""

    def test_end_to_end(self, test_db):
        from awards.db import SessionLocal

        # Seed an award so currencies are discovered
        session = SessionLocal()
        try:
            cb = Organization(official_name="Test Org")
            session.add(cb)
            session.flush()
            doc = Document(
                doc_id="test-1",
                publication_date=date(2024, 1, 1),
                buyer_organization_id=cb.id,
            )
            session.add(doc)
            session.flush()
            contract = Contract(doc_id="test-1", title="Test")
            session.add(contract)
            session.flush()
            session.add(Award(contract_id=contract.id, awarded_value_currency="GBP"))
            session.commit()
        finally:
            session.close()

        mock_ecb = Mock()
        mock_ecb.text = ECB_CSV
        mock_ecb.raise_for_status = Mock()

        mock_hicp = Mock()
        mock_hicp.json.return_value = EUROSTAT_JSON
        mock_hicp.raise_for_status = Mock()

        def route_get(url, **kwargs):
            if "ecb.europa.eu" in url:
                return mock_ecb
            return mock_hicp

        with patch("awards.rates.requests.get", side_effect=route_get):
            update_rates(2024, 2024)

        session = SessionLocal()
        try:
            er_count = session.execute(select(ExchangeRate)).scalars().all()
            pi_count = session.execute(select(PriceIndex)).scalars().all()
            # ECB CSV has GBP rows only (SEK not in award currencies query result
            # but fetch_ecb_rates receives whatever _get_award_currencies returns;
            # the CSV response is canned so we get all 4 rows regardless)
            assert len(er_count) == 4
            assert len(pi_count) == 3
        finally:
            session.close()


class TestMaterializedView:
    """Tests for materialized view creation and refresh."""

    def test_create_view(self, test_db):
        """Test that the materialized view can be created."""
        create_materialized_view(test_db)

        with test_db.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM pg_matviews WHERE matviewname = 'awards_adjusted'"
                )
            )
            assert result.scalar() == 1

    def test_create_view_idempotent(self, test_db):
        """Test that creating twice doesn't error."""
        create_materialized_view(test_db)
        create_materialized_view(test_db)

    def test_refresh_view(self, test_db):
        """Test that refresh works with data."""
        from awards.db import SessionLocal

        # Seed data
        session = SessionLocal()
        try:
            cb = Organization(official_name="Test Org")
            session.add(cb)
            session.flush()
            doc = Document(
                doc_id="test-1",
                publication_date=date(2024, 6, 15),
                buyer_organization_id=cb.id,
            )
            session.add(doc)
            session.flush()
            contract = Contract(doc_id="test-1", title="Test Contract")
            session.add(contract)
            session.flush()
            session.add(
                Award(
                    contract_id=contract.id,
                    awarded_value=Decimal("100000.00"),
                    awarded_value_currency="EUR",
                )
            )
            session.commit()
        finally:
            session.close()

        refresh_materialized_view(test_db)

        with test_db.connect() as conn:
            row = conn.execute(text("SELECT * FROM awards_adjusted LIMIT 1")).fetchone()
            assert row is not None
            # EUR award → value_eur should equal awarded_value
            assert row._mapping["value_eur"] == Decimal("100000.00")

    def test_view_currency_conversion(self, test_db):
        """Test that non-EUR values are converted using exchange rates."""
        from awards.db import SessionLocal

        session = SessionLocal()
        try:
            # Seed exchange rate
            session.add(
                ExchangeRate(currency="GBP", year=2024, month=6, rate=Decimal("0.85"))
            )

            cb = Organization(official_name="Test Org")
            session.add(cb)
            session.flush()
            doc = Document(
                doc_id="test-1",
                publication_date=date(2024, 6, 15),
                buyer_organization_id=cb.id,
            )
            session.add(doc)
            session.flush()
            contract = Contract(doc_id="test-1", title="Test Contract")
            session.add(contract)
            session.flush()
            session.add(
                Award(
                    contract_id=contract.id,
                    awarded_value=Decimal("85000.00"),
                    awarded_value_currency="GBP",
                )
            )
            session.commit()
        finally:
            session.close()

        refresh_materialized_view(test_db)

        with test_db.connect() as conn:
            row = conn.execute(text("SELECT * FROM awards_adjusted LIMIT 1")).fetchone()
            # 85000 / 0.85 = 100000.00
            assert row._mapping["value_eur"] == Decimal("100000.00")

    def test_view_inflation_adjustment(self, test_db):
        """Test that inflation adjustment uses 2024 as base year."""
        from awards.db import SessionLocal

        session = SessionLocal()
        try:
            # Seed price indices
            session.add(PriceIndex(year=2020, index_value=Decimal("100.0000")))
            session.add(PriceIndex(year=2024, index_value=Decimal("120.0000")))

            cb = Organization(official_name="Test Org")
            session.add(cb)
            session.flush()
            doc = Document(
                doc_id="test-1",
                publication_date=date(2020, 3, 1),
                buyer_organization_id=cb.id,
            )
            session.add(doc)
            session.flush()
            contract = Contract(doc_id="test-1", title="Test Contract")
            session.add(contract)
            session.flush()
            session.add(
                Award(
                    contract_id=contract.id,
                    awarded_value=Decimal("100000.00"),
                    awarded_value_currency="EUR",
                )
            )
            session.commit()
        finally:
            session.close()

        refresh_materialized_view(test_db)

        with test_db.connect() as conn:
            row = conn.execute(text("SELECT * FROM awards_adjusted LIMIT 1")).fetchone()
            # 100000 * 120 / 100 = 120000.00
            assert row._mapping["value_eur_real"] == Decimal("120000.00")
