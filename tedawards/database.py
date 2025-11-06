"""
Database management using SQLAlchemy ORM.
Handles database operations for TED awards data.
"""

import logging
from pathlib import Path
from typing import List, Optional

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .config import config
from .models import (
    Base, TEDDocument, ContractingBody, Contract, Lot, Award, Contractor,
    Country, Language, Currency, CPVCode, NUTSCode,
    award_contractors, contract_cpv_codes, contract_nuts_codes
)
from .schema import TedAwardDataModel

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Handle database operations for TED awards data using SQLAlchemy."""

    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self.session: Optional[Session] = None

    def connect(self):
        """Connect to SQLite database and create a session."""
        try:
            db_path = config.DB_PATH
            # Ensure parent directory exists
            db_path.parent.mkdir(parents=True, exist_ok=True)

            # Create SQLite database URL
            database_url = f"sqlite:///{db_path}"

            self.engine = create_engine(
                database_url,
                echo=False,  # Set to True for SQL debugging
                connect_args={"check_same_thread": False}  # Needed for SQLite
            )

            # Create session factory
            self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)

            # Create session for this manager instance
            self.session = self.SessionLocal()

            logger.info(f"Connected to database: {db_path}")
            self._ensure_schema_exists()

        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def close(self):
        """Close database session and clean up."""
        if self.session:
            self.session.close()
            logger.info("Database session closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _ensure_schema_exists(self):
        """Ensure database schema exists by creating all tables."""
        try:
            # Create all tables
            Base.metadata.create_all(self.engine)
            logger.info("Database schema created/verified successfully")
        except Exception as e:
            logger.error(f"Error creating database schema: {e}")
            raise

    def save_award_data(self, data: TedAwardDataModel):
        """Save parsed award data to database."""
        try:
            # Save document
            doc = self._insert_document(data.document.dict())

            # Save contracting body
            cb = self._insert_contracting_body(data.document.doc_id, data.contracting_body.dict())

            # Save contract
            contract = self._insert_contract(data.document.doc_id, cb.id, data.contract.dict())

            # Save awards
            for award_data in data.awards:
                award = self._insert_award(contract.id, award_data.dict())

                for contractor_data in award_data.contractors:
                    contractor = self._insert_contractor(contractor_data.dict())
                    self._link_award_contractor(award, contractor)

            self.session.commit()
            logger.info(f"Saved award data for document {data.document.doc_id}")

        except Exception as e:
            self.session.rollback()
            logger.error(f"Error saving award data: {e}")
            raise

    def _insert_document(self, data: dict) -> TEDDocument:
        """Insert document record using INSERT OR IGNORE for better performance."""
        # Use INSERT OR IGNORE - if doc already exists, get it; otherwise insert
        stmt = sqlite_insert(TEDDocument).values(**data)
        stmt = stmt.on_conflict_do_nothing(index_elements=['doc_id'])
        self.session.execute(stmt)
        self.session.flush()

        # Fetch the document (either newly inserted or existing)
        doc = self.session.execute(
            select(TEDDocument).where(TEDDocument.doc_id == data['doc_id'])
        ).scalar_one()
        return doc

    def _insert_contracting_body(self, doc_id: str, data: dict) -> ContractingBody:
        """Insert contracting body and return object."""
        data['ted_doc_id'] = doc_id
        cb = ContractingBody(**data)
        self.session.add(cb)
        self.session.flush()
        return cb

    def _insert_contract(self, doc_id: str, cb_id: int, data: dict) -> Contract:
        """Insert contract and return object."""
        data['ted_doc_id'] = doc_id
        data['contracting_body_id'] = cb_id

        # Remove fields that don't exist in Contract model but might be in pydantic model
        data.pop('performance_nuts_code', None)

        contract = Contract(**data)
        self.session.add(contract)
        self.session.flush()
        return contract

    def _insert_award(self, contract_id: int, data: dict) -> Award:
        """Insert award and return object."""
        # Remove contractors from data as they're handled separately
        contractors_data = data.pop('contractors', [])

        data['contract_id'] = contract_id
        award = Award(**data)
        self.session.add(award)
        self.session.flush()
        return award

    def _insert_contractor(self, data: dict) -> Contractor:
        """Insert contractor using INSERT OR IGNORE, leveraging unique constraint."""
        # Use INSERT OR IGNORE - unique constraint handles deduplication
        stmt = sqlite_insert(Contractor).values(**data)
        stmt = stmt.on_conflict_do_nothing(index_elements=['official_name', 'country_code'])
        self.session.execute(stmt)
        self.session.flush()

        # Fetch the contractor (either newly inserted or existing)
        contractor = self.session.execute(
            select(Contractor).where(
                Contractor.official_name == data['official_name'],
                Contractor.country_code == data.get('country_code')
            )
        ).scalar_one()
        return contractor

    def _link_award_contractor(self, award: Award, contractor: Contractor):
        """Link award to contractor."""
        # Check if relationship already exists
        if contractor not in award.contractors:
            award.contractors.append(contractor)
            self.session.flush()

    def save_award_data_batch(self, awards: List[TedAwardDataModel]):
        """Save multiple award data records efficiently in batch."""
        if not awards:
            return

        try:
            # Process each award
            for award_data in awards:
                # Save document
                doc = self._insert_document(award_data.document.dict())

                # Save contracting body
                cb = self._insert_contracting_body(award_data.document.doc_id, award_data.contracting_body.dict())

                # Save contract
                contract = self._insert_contract(award_data.document.doc_id, cb.id, award_data.contract.dict())

                # Save awards
                for award_item in award_data.awards:
                    award = self._insert_award(contract.id, award_item.dict())

                    for contractor_data in award_item.contractors:
                        contractor = self._insert_contractor(contractor_data.dict())
                        self._link_award_contractor(award, contractor)

            self.session.commit()
            logger.info(f"Saved batch of {len(awards)} award documents")

        except Exception as e:
            self.session.rollback()
            logger.error(f"Error saving award data batch: {e}")
            raise
