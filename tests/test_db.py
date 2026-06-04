"""
Tests for db.py — shared database logic.
"""

import pytest
from datetime import date
from unittest.mock import patch
from decimal import Decimal

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from awards.countries import get_country_name
from awards.db import (
    save_document,
    get_session,
    _normalize_country_code,
)
from awards.models import (
    Base,
    Document,
    Organization,
    Contract,
    Award,
    CpvCode,
    Country,
    OrganizationIdentifier,
    ProcedureType,
    award_contractors,
    contract_cpv_codes,
)
from awards.schema import (
    AwardDataModel,
    DocumentModel,
    OrganizationModel,
    ContractModel,
    CpvCodeEntry,
    IdentifierEntry,
    ProcedureTypeEntry,
    AwardModel,
)

TEST_DATABASE_URL = "postgresql://awards:awards@localhost:5433/awards_test"


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


@pytest.fixture
def sample_award_data():
    """Create sample award data for testing."""
    return AwardDataModel(
        document=DocumentModel(
            doc_id="12345-2024",
            edition="2024/S 001-000001",
            publication_date=date(2024, 1, 1),
            source_country="DE",
        ),
        buyer=OrganizationModel(
            official_name="Test Contracting Body",
            town="Berlin",
            country_code="DE",
            nuts_code="DE300",
        ),
        contract=ContractModel(
            title="Test Contract",
            main_cpv_code="45000000",
            cpv_codes=[CpvCodeEntry(code="45000000")],
            nuts_code="DE212",
        ),
        awards=[
            AwardModel(
                award_title="Award 1",
                awarded_value=50000.0,
                awarded_value_currency="EUR",
                tenders_received=5,
                contractors=[
                    OrganizationModel(
                        official_name="Test Contractor GmbH",
                        town="Munich",
                        country_code="DE",
                        nuts_code="DE212",
                    )
                ],
            )
        ],
    )


class TestSaveDocument:
    """Tests for save_document function."""

    def test_save_single_award(self, test_db, sample_award_data):
        """Test saving a single award to database."""
        from awards.db import SessionLocal

        assert save_document(sample_award_data) is True

        session = SessionLocal()
        try:
            doc = session.execute(
                select(Document).where(Document.doc_id == "12345-2024")
            ).scalar_one()
            assert doc.edition == "2024/S 001-000001"
            assert doc.buyer_organization_id is not None

            buyer = session.execute(
                select(Organization).where(
                    Organization.official_name == "Test Contracting Body"
                )
            ).scalar_one()
            assert doc.buyer_organization_id == buyer.id
            assert buyer.nuts_code == "DE300"

            contract = session.execute(
                select(Contract).where(Contract.doc_id == "12345-2024")
            ).scalar_one()
            assert contract.title == "Test Contract"
            assert contract.nuts_code == "DE212"

            award = session.execute(
                select(Award).where(Award.contract_id == contract.id)
            ).scalar_one()
            assert award.awarded_value == Decimal("50000.00")
            assert award.tenders_received == 5

            contractor = session.execute(
                select(Organization).where(
                    Organization.official_name == "Test Contractor GmbH"
                )
            ).scalar_one()
            assert contractor.country_code == "DE"
            assert contractor.nuts_code == "DE212"

            # Verify junction table link
            link = session.execute(
                select(award_contractors).where(
                    award_contractors.c.award_id == award.id,
                    award_contractors.c.organization_id == contractor.id,
                )
            ).one()
            assert link is not None
        finally:
            session.close()

    def test_save_duplicate_document_skipped(self, test_db, sample_award_data):
        """Test that duplicate documents are skipped (idempotent import)."""
        from awards.db import SessionLocal

        assert save_document(sample_award_data) is True
        assert save_document(sample_award_data) is False

        session = SessionLocal()
        try:
            docs = session.execute(
                select(Document).where(Document.doc_id == "12345-2024")
            ).all()
            assert len(docs) == 1
        finally:
            session.close()

    def test_save_same_contractor_deduplicated(self, test_db):
        """Test that same contractor in different documents creates one shared record."""
        from awards.db import SessionLocal

        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                edition="2024/S 001-000001",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Test Body 1", country_code="DE"),
            contract=ContractModel(title="Contract 1"),
            awards=[
                AwardModel(
                    contractors=[
                        OrganizationModel(
                            official_name="Shared Contractor Ltd", country_code="GB"
                        )
                    ]
                )
            ],
        )

        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                edition="2024/S 001-000002",
                publication_date=date(2024, 1, 2),
                source_country="FR",
            ),
            buyer=OrganizationModel(official_name="Test Body 2", country_code="FR"),
            contract=ContractModel(title="Contract 2"),
            awards=[
                AwardModel(
                    contractors=[
                        OrganizationModel(
                            official_name="Shared Contractor Ltd", country_code="GB"
                        )
                    ]
                )
            ],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            # Only one org row for the contractor (deduplicated)
            contractors = session.execute(
                select(Organization).where(
                    Organization.official_name == "Shared Contractor Ltd"
                )
            ).all()
            assert len(contractors) == 1

            # But two junction table rows (one per award)
            links = session.execute(select(award_contractors)).all()
            assert len(links) == 2
        finally:
            session.close()

    def test_save_multiple_awards_same_contract(self, test_db):
        """Test saving multiple awards for same contract."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                edition="2024/S 001-000001",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Test Body", country_code="DE"),
            contract=ContractModel(title="Multi-lot Contract"),
            awards=[
                AwardModel(
                    award_title="Lot 1",
                    awarded_value=10000.0,
                    awarded_value_currency="EUR",
                    contractors=[
                        OrganizationModel(
                            official_name="Contractor A", country_code="DE"
                        )
                    ],
                ),
                AwardModel(
                    award_title="Lot 2",
                    awarded_value=20000.0,
                    awarded_value_currency="EUR",
                    contractors=[
                        OrganizationModel(
                            official_name="Contractor B", country_code="FR"
                        )
                    ],
                ),
            ],
        )

        assert save_document(award_data) is True

        session = SessionLocal()
        try:
            awards = session.execute(select(Award)).all()
            assert len(awards) == 2

            # 3 orgs total: buyer + 2 contractors
            orgs = session.execute(select(Organization)).all()
            assert len(orgs) == 3

            links = session.execute(select(award_contractors)).all()
            assert len(links) == 2
        finally:
            session.close()

    def test_save_reimport_is_idempotent(self, test_db, sample_award_data):
        """Test that re-importing the same data is idempotent (skips existing docs)."""
        from awards.db import SessionLocal

        assert save_document(sample_award_data) is True
        assert save_document(sample_award_data) is False

        session = SessionLocal()
        try:
            assert len(session.execute(select(Document)).all()) == 1
            # 2 orgs: buyer + contractor
            assert len(session.execute(select(Organization)).all()) == 2
            assert len(session.execute(select(Contract)).all()) == 1
            assert len(session.execute(select(Award)).all()) == 1
        finally:
            session.close()

    def test_buyer_deduplicated(self, test_db):
        """Test that identical buyers across documents are deduplicated."""
        from awards.db import SessionLocal

        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(
                official_name="Ministry of Health", country_code="DE", town="Berlin"
            ),
            contract=ContractModel(title="Medical Supplies Contract 2024"),
            awards=[AwardModel(contractors=[])],
        )

        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 15),
                source_country="DE",
            ),
            buyer=OrganizationModel(
                official_name="Ministry of Health", country_code="DE", town="Berlin"
            ),
            contract=ContractModel(title="IT Services Contract 2024"),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            assert len(session.execute(select(Document)).all()) == 2

            # Only one org row (deduplicated)
            orgs = session.execute(
                select(Organization).where(
                    Organization.official_name == "Ministry of Health"
                )
            ).all()
            assert len(orgs) == 1

            # Both documents point to the same buyer organization
            doc1 = session.execute(
                select(Document).where(Document.doc_id == "12345-2024")
            ).scalar_one()
            doc2 = session.execute(
                select(Document).where(Document.doc_id == "67890-2024")
            ).scalar_one()
            assert doc1.buyer_organization_id == doc2.buyer_organization_id

            assert len(session.execute(select(Contract)).all()) == 2
        finally:
            session.close()

    def test_different_buyers_not_deduplicated(self, test_db):
        """Test that buyers with different fields remain separate."""
        from awards.db import SessionLocal

        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(
                official_name="Ministry of Health", country_code="DE", town="Berlin"
            ),
            contract=ContractModel(title="Contract 1"),
            awards=[AwardModel(contractors=[])],
        )

        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 15),
                source_country="FR",
            ),
            buyer=OrganizationModel(
                official_name="Ministry of Health", country_code="FR", town="Paris"
            ),
            contract=ContractModel(title="Contract 2"),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            orgs = session.execute(
                select(Organization).where(
                    Organization.official_name == "Ministry of Health"
                )
            ).all()
            assert len(orgs) == 2
        finally:
            session.close()

    def test_cpv_code_lookup_table_deduplication(self, test_db):
        """Test that same CPV code from two documents creates one lookup row."""
        from awards.db import SessionLocal

        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(
                title="Contract 1",
                main_cpv_code="45000000",
                cpv_codes=[
                    CpvCodeEntry(
                        code="45000000",
                        description="Construction work",
                    )
                ],
            ),
            awards=[AwardModel(contractors=[])],
        )

        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 2),
                source_country="FR",
            ),
            buyer=OrganizationModel(official_name="Body 2", country_code="FR"),
            contract=ContractModel(
                title="Contract 2",
                main_cpv_code="45000000",
                cpv_codes=[
                    CpvCodeEntry(
                        code="45000000",
                        description="Construction work",
                    )
                ],
            ),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            # Only one CPV code row (deduplicated)
            cpv_rows = session.execute(select(CpvCode)).all()
            assert len(cpv_rows) == 1
            assert cpv_rows[0][0].code == "45000000"
            assert cpv_rows[0][0].description == "Construction work"

            # But two junction table rows (one per contract)
            links = session.execute(select(contract_cpv_codes)).all()
            assert len(links) == 2
        finally:
            session.close()

    def test_cpv_description_preserved_when_null(self, test_db):
        """Test that existing description is preserved when later doc has NULL description."""
        from awards.db import SessionLocal

        # First doc has description
        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(
                title="Contract 1",
                main_cpv_code="45000000",
                cpv_codes=[
                    CpvCodeEntry(
                        code="45000000",
                        description="Construction work",
                    )
                ],
            ),
            awards=[AwardModel(contractors=[])],
        )

        # Second doc has NULL description (e.g. eForms)
        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 2),
                source_country="FR",
            ),
            buyer=OrganizationModel(official_name="Body 2", country_code="FR"),
            contract=ContractModel(
                title="Contract 2",
                main_cpv_code="45000000",
                cpv_codes=[CpvCodeEntry(code="45000000", description=None)],
            ),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            cpv = session.execute(
                select(CpvCode).where(CpvCode.code == "45000000")
            ).scalar_one()
            assert cpv.description == "Construction work", (
                "Description should be preserved when later doc has NULL"
            )
        finally:
            session.close()

    def test_procedure_type_lookup_table_deduplication(self, test_db):
        """Test that same procedure type from two documents creates one lookup row."""
        from awards.db import SessionLocal

        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(
                title="Contract 1",
                procedure_type=ProcedureTypeEntry(
                    code="open", description="Open procedure"
                ),
            ),
            awards=[AwardModel(contractors=[])],
        )

        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 2),
                source_country="FR",
            ),
            buyer=OrganizationModel(official_name="Body 2", country_code="FR"),
            contract=ContractModel(
                title="Contract 2",
                procedure_type=ProcedureTypeEntry(
                    code="open", description="Open procedure"
                ),
            ),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            # Only one procedure type row (deduplicated)
            pt_rows = session.execute(select(ProcedureType)).all()
            assert len(pt_rows) == 1
            assert pt_rows[0][0].code == "open"
            assert pt_rows[0][0].description == "Open procedure"

            # Both contracts reference the same procedure type
            contracts = session.execute(select(Contract)).all()
            assert len(contracts) == 2
            for row in contracts:
                assert row[0].procedure_type_code == "open"
        finally:
            session.close()

    def test_procedure_type_description_preserved_when_null(self, test_db):
        """Test that existing description is preserved when later doc has NULL description."""
        from awards.db import SessionLocal

        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(
                title="Contract 1",
                procedure_type=ProcedureTypeEntry(
                    code="open", description="Open procedure"
                ),
            ),
            awards=[AwardModel(contractors=[])],
        )

        # Second doc has NULL description (e.g. eForms)
        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 2),
                source_country="FR",
            ),
            buyer=OrganizationModel(official_name="Body 2", country_code="FR"),
            contract=ContractModel(
                title="Contract 2",
                procedure_type=ProcedureTypeEntry(code="open", description=None),
            ),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            pt = session.execute(
                select(ProcedureType).where(ProcedureType.code == "open")
            ).scalar_one()
            assert pt.description == "Open procedure", (
                "Description should be preserved when later doc has NULL"
            )
        finally:
            session.close()

    def test_duplicate_cpv_code_deduplicated(self, test_db):
        """Test that duplicate CPV codes in list create one junction table row."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(
                title="Contract 1",
                main_cpv_code="50750000",
                cpv_codes=[
                    CpvCodeEntry(code="50750000"),
                    CpvCodeEntry(code="50750000"),
                ],
            ),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data)

        session = SessionLocal()
        try:
            links = session.execute(select(contract_cpv_codes)).all()
            assert len(links) == 1
        finally:
            session.close()


class TestGetSession:
    """Tests for get_session context manager."""

    def test_session_commits_on_success(self, test_db):
        """Test that session commits when no exception occurs."""
        from awards.db import SessionLocal

        # Need to create an organization first for the FK
        org_session = SessionLocal()
        org = Organization(official_name="Test Org")
        org_session.add(org)
        org_session.commit()
        org_id = org.id
        org_session.close()

        with get_session() as session:
            doc = Document(
                doc_id="test-doc",
                edition="2024/S 001-000001",
                publication_date=date(2024, 1, 1),
                buyer_organization_id=org_id,
            )
            session.add(doc)

        # Verify document was committed
        verify_session = SessionLocal()
        try:
            result = verify_session.execute(
                select(Document).where(Document.doc_id == "test-doc")
            ).scalar_one_or_none()
            assert result is not None
        finally:
            verify_session.close()

    def test_session_rolls_back_on_exception(self, test_db):
        """Test that session rolls back when exception occurs."""
        from awards.db import SessionLocal

        org_session = SessionLocal()
        org = Organization(official_name="Test Org")
        org_session.add(org)
        org_session.commit()
        org_id = org.id
        org_session.close()

        with pytest.raises(ValueError):
            with get_session() as session:
                doc = Document(
                    doc_id="test-doc",
                    edition="2024/S 001-000001",
                    publication_date=date(2024, 1, 1),
                    buyer_organization_id=org_id,
                )
                session.add(doc)
                raise ValueError("Test error")

        # Verify document was NOT committed
        verify_session = SessionLocal()
        try:
            result = verify_session.execute(
                select(Document).where(Document.doc_id == "test-doc")
            ).scalar_one_or_none()
            assert result is None
        finally:
            verify_session.close()


class TestNormalizeCountryCode:
    """Tests for _normalize_country_code function."""

    def test_uk_maps_to_gb(self):
        assert _normalize_country_code("UK") == "GB"

    def test_uk_lowercase_maps_to_gb(self):
        assert _normalize_country_code("uk") == "GB"

    def test_1a_maps_to_none(self):
        assert _normalize_country_code("1A") is None

    def test_empty_string_maps_to_none(self):
        assert _normalize_country_code("") is None

    def test_none_maps_to_none(self):
        assert _normalize_country_code(None) is None

    def test_normal_code_uppercased(self):
        assert _normalize_country_code("de") == "DE"

    def test_normal_code_preserved(self):
        assert _normalize_country_code("FR") == "FR"

    def test_alpha3_maps_to_alpha2(self):
        assert _normalize_country_code("FRA") == "FR"
        assert _normalize_country_code("DEU") == "DE"
        assert _normalize_country_code("GRC") == "GR"

    def test_alpha3_lowercase_maps_to_alpha2(self):
        assert _normalize_country_code("fra") == "FR"

    def test_1a0_maps_to_none(self):
        assert _normalize_country_code("1A0") is None

    def test_kosovo_alpha3_maps_to_xk(self):
        assert _normalize_country_code("XKX") == "XK"

    def test_unresolvable_alpha3_kept_raw(self):
        # Fail-loud: unknown alpha-3 is preserved, not silently dropped.
        assert _normalize_country_code("ZZZ") == "ZZZ"


class TestGetCountryName:
    """Tests for get_country_name lookup."""

    def test_iso_alpha2_resolves(self):
        assert get_country_name("FR") == "France"

    def test_historical_code_resolves(self):
        assert get_country_name("AN") == "Netherlands Antilles"

    def test_kosovo_resolves(self):
        # XK has no ISO entry; covered by the manual override.
        assert get_country_name("XK") == "Kosovo"

    def test_unknown_code_returns_none(self):
        assert get_country_name("ZZ") is None


class TestCountryLookupTable:
    """Tests for country lookup table integration."""

    def test_country_deduplication(self, test_db):
        """Two docs with same country code -> one countries row."""
        from awards.db import SessionLocal

        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(title="Contract 1"),
            awards=[AwardModel(contractors=[])],
        )

        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 2),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 2", country_code="DE"),
            contract=ContractModel(title="Contract 2"),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            de_rows = session.execute(select(Country).where(Country.code == "DE")).all()
            assert len(de_rows) == 1
            assert de_rows[0][0].name == "Germany"
        finally:
            session.close()

    def test_country_name_preserved(self, test_db):
        """Country name is preserved when a later doc doesn't provide one (COALESCE)."""
        from awards.db import SessionLocal

        # First doc creates country with name from pycountry
        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="FR",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="FR"),
            contract=ContractModel(title="Contract 1"),
            awards=[AwardModel(contractors=[])],
        )

        # Second doc also references FR
        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 2),
                source_country="FR",
            ),
            buyer=OrganizationModel(official_name="Body 2", country_code="FR"),
            contract=ContractModel(title="Contract 2"),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            fr = session.execute(
                select(Country).where(Country.code == "FR")
            ).scalar_one()
            assert fr.name == "France"
        finally:
            session.close()

    def test_uk_normalized_to_gb_in_country_table(self, test_db):
        """UK country code is normalized to GB and stored in countries table."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="UK",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="UK"),
            contract=ContractModel(title="Contract 1"),
            awards=[
                AwardModel(
                    contractors=[
                        OrganizationModel(official_name="UK Ltd", country_code="UK")
                    ]
                )
            ],
        )

        save_document(award_data)

        session = SessionLocal()
        try:
            # GB row exists with correct name
            gb = session.execute(
                select(Country).where(Country.code == "GB")
            ).scalar_one()
            assert "United Kingdom" in gb.name

            # No UK row
            uk = session.execute(
                select(Country).where(Country.code == "UK")
            ).scalar_one_or_none()
            assert uk is None

            # All entities stored as GB
            doc = session.execute(
                select(Document).where(Document.doc_id == "12345-2024")
            ).scalar_one()
            assert doc.source_country == "GB"

            # Buyer org stored as GB
            buyer = session.execute(
                select(Organization).where(Organization.official_name == "Body 1")
            ).scalar_one()
            assert buyer.country_code == "GB"

            # Contractor org stored as GB
            ct = session.execute(
                select(Organization).where(Organization.official_name == "UK Ltd")
            ).scalar_one()
            assert ct.country_code == "GB"
        finally:
            session.close()

    def test_contractor_country_upserted(self, test_db):
        """Country from contractor is upserted into countries table."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(title="Contract 1"),
            awards=[
                AwardModel(
                    contractors=[
                        OrganizationModel(official_name="Polish Co", country_code="PL")
                    ]
                )
            ],
        )

        save_document(award_data)

        session = SessionLocal()
        try:
            countries = {row[0].code for row in session.execute(select(Country)).all()}
            assert "DE" in countries
            assert "PL" in countries
        finally:
            session.close()


class TestNewFields:
    """Tests for new Award and Contract fields."""

    def test_award_new_fields_stored(self, test_db):
        """Test that award_date, lot_number, contract dates are stored."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(title="Contract 1"),
            awards=[
                AwardModel(
                    award_title="Lot 1",
                    lot_number="LOT-0001",
                    award_date=date(2024, 6, 15),
                    contract_start_date=date(2024, 7, 1),
                    contract_end_date=date(2025, 6, 30),
                    contractors=[],
                )
            ],
        )

        save_document(award_data)

        session = SessionLocal()
        try:
            award = session.execute(select(Award)).scalar_one()
            assert award.lot_number == "LOT-0001"
            assert award.award_date == date(2024, 6, 15)
            assert award.contract_start_date == date(2024, 7, 1)
            assert award.contract_end_date == date(2025, 6, 30)
        finally:
            session.close()

    def test_contract_new_fields_stored(self, test_db):
        """Test that estimated_value, framework_agreement, eu_funded are stored."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(
                title="Contract 1",
                estimated_value=Decimal("500000.00"),
                estimated_value_currency="EUR",
                framework_agreement=True,
                eu_funded=True,
            ),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data)

        session = SessionLocal()
        try:
            contract = session.execute(select(Contract)).scalar_one()
            assert contract.estimated_value == Decimal("500000.00")
            assert contract.estimated_value_currency == "EUR"
            assert contract.framework_agreement is True
            assert contract.eu_funded is True
        finally:
            session.close()

    def test_contract_boolean_defaults(self, test_db):
        """Test that framework_agreement and eu_funded default to False."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(title="Contract 1"),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data)

        session = SessionLocal()
        try:
            contract = session.execute(select(Contract)).scalar_one()
            assert contract.framework_agreement is False
            assert contract.eu_funded is False
        finally:
            session.close()


class TestOrganizationIdentifiers:
    """Tests for organization identifier storage."""

    def test_buyer_identifiers_stored(self, test_db):
        """Test that buyer organization identifiers are stored."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(
                official_name="Body 1",
                country_code="DE",
                identifiers=[
                    IdentifierEntry(scheme="ORG", identifier="90004585"),
                ],
            ),
            contract=ContractModel(title="Contract 1"),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data)

        session = SessionLocal()
        try:
            org_ids = session.execute(select(OrganizationIdentifier)).all()
            assert len(org_ids) == 1
            org_id = org_ids[0][0]
            assert org_id.scheme == "ORG"
            assert org_id.identifier == "90004585"
            assert org_id.organization_id is not None
        finally:
            session.close()

    def test_contractor_identifiers_stored(self, test_db):
        """Test that contractor identifiers are stored."""
        from awards.db import SessionLocal

        award_data = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=OrganizationModel(official_name="Body 1", country_code="DE"),
            contract=ContractModel(title="Contract 1"),
            awards=[
                AwardModel(
                    contractors=[
                        OrganizationModel(
                            official_name="Contractor A",
                            country_code="DE",
                            identifiers=[
                                IdentifierEntry(scheme="ORG", identifier="12339040"),
                            ],
                        )
                    ]
                )
            ],
        )

        save_document(award_data)

        session = SessionLocal()
        try:
            org_ids = session.execute(select(OrganizationIdentifier)).all()
            assert len(org_ids) == 1
            org_id = org_ids[0][0]
            assert org_id.scheme == "ORG"
            assert org_id.identifier == "12339040"
            assert org_id.organization_id is not None
        finally:
            session.close()

    def test_identifier_deduplication(self, test_db):
        """Test that duplicate identifiers are not inserted twice."""
        from awards.db import SessionLocal

        buyer = OrganizationModel(
            official_name="Body 1",
            country_code="DE",
            identifiers=[
                IdentifierEntry(scheme="ORG", identifier="90004585"),
            ],
        )

        award_data_1 = AwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                publication_date=date(2024, 1, 1),
                source_country="DE",
            ),
            buyer=buyer,
            contract=ContractModel(title="Contract 1"),
            awards=[AwardModel(contractors=[])],
        )

        award_data_2 = AwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                publication_date=date(2024, 1, 2),
                source_country="DE",
            ),
            buyer=buyer,
            contract=ContractModel(title="Contract 2"),
            awards=[AwardModel(contractors=[])],
        )

        save_document(award_data_1)
        save_document(award_data_2)

        session = SessionLocal()
        try:
            # Same org with same identifier -> only one org identifier row
            org_ids = session.execute(select(OrganizationIdentifier)).all()
            assert len(org_ids) == 1
        finally:
            session.close()
