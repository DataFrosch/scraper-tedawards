"""
Database management using SQLAlchemy ORM.
Handles database operations for TED awards data.
"""

import logging
from pathlib import Path
from typing import List, Optional

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from .config import config
from .models import (
    Base, TEDDocument, ContractingBody, Contract, Lot, Award, Contractor,
    Country, Language, Currency, CPVCode, NUTSCode,
    award_contractors, contract_cpv_codes, contract_nuts_codes
)
from .schema import TedAwardDataModel
from .reference_data import ReferenceDataManager

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Handle database operations for TED awards data using SQLAlchemy."""

    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self.session: Optional[Session] = None
        self.reference_manager = ReferenceDataManager()

    def connect(self):
        """Connect to SQLite database."""
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

            self.SessionLocal = sessionmaker(bind=self.engine)
            self.session = self.SessionLocal()

            logger.info(f"Connected to database: {db_path}")
            self._ensure_schema_exists()

        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.session:
            self.session.close()
            logger.info("Database connection closed")

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

    def document_exists(self, doc_id: str) -> bool:
        """Check if document already exists in database."""
        result = self.session.execute(
            select(TEDDocument).where(TEDDocument.doc_id == doc_id)
        ).first()
        return result is not None

    def save_award_data(self, data: TedAwardDataModel):
        """Save parsed award data to database."""
        try:
            # Collect and flush reference data
            self.reference_manager.collect_from_award_data(data)
            self.reference_manager.flush_to_database(self.session)

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
        """Insert document record."""
        # Check if document already exists
        existing = self.session.execute(
            select(TEDDocument).where(TEDDocument.doc_id == data['doc_id'])
        ).scalar_one_or_none()

        if existing:
            return existing

        doc = TEDDocument(**data)
        self.session.add(doc)
        self.session.flush()  # Flush to get the ID
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
        """Insert contractor and return object, or return existing."""
        # First try to find existing contractor by name and country
        existing = self.session.execute(
            select(Contractor).where(
                Contractor.official_name == data['official_name'],
                Contractor.country_code == data.get('country_code')
            )
        ).scalar_one_or_none()

        if existing:
            return existing

        # Insert new contractor
        contractor = Contractor(**data)
        self.session.add(contractor)
        self.session.flush()
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
            # Collect and flush reference data for entire batch
            self.reference_manager.collect_from_batch(awards)
            self.reference_manager.flush_to_database(self.session)

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
