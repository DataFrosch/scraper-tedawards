"""
SQLAlchemy models for TED awards database.
Designed for SQLite with easy PostgreSQL migration support.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Integer,
    Numeric, String, Text, Table, UniqueConstraint, Index
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


# Association tables for many-to-many relationships
award_contractors = Table(
    'award_contractors',
    Base.metadata,
    Column('award_id', Integer, ForeignKey('awards.id', ondelete='CASCADE'), primary_key=True),
    Column('contractor_id', Integer, ForeignKey('contractors.id'), primary_key=True),
    Column('is_lead_contractor', Boolean, default=False),
    Column('created_at', DateTime, default=func.now())
)

contract_cpv_codes = Table(
    'contract_cpv_codes',
    Base.metadata,
    Column('contract_id', Integer, ForeignKey('contracts.id', ondelete='CASCADE'), primary_key=True),
    Column('cpv_code', String, ForeignKey('cpv_codes.code'), primary_key=True),
    Column('is_main', Boolean, default=False),
    Column('created_at', DateTime, default=func.now())
)

contract_nuts_codes = Table(
    'contract_nuts_codes',
    Base.metadata,
    Column('contract_id', Integer, ForeignKey('contracts.id', ondelete='CASCADE'), nullable=False),
    Column('nuts_code', String, ForeignKey('nuts_codes.code'), nullable=False),
    Column('nuts_type', String, nullable=False),  # 'PERFORMANCE', 'CA_CE', 'TENDERER'
    Column('created_at', DateTime, default=func.now()),
    UniqueConstraint('contract_id', 'nuts_code', 'nuts_type', name='uq_contract_nuts_type')
)


# Reference data models
class Country(Base):
    """Country reference data."""
    __tablename__ = 'countries'

    code: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


class Language(Base):
    """Language reference data."""
    __tablename__ = 'languages'

    code: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


class Currency(Base):
    """Currency reference data."""
    __tablename__ = 'currencies'

    code: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


class CPVCode(Base):
    """Common Procurement Vocabulary codes."""
    __tablename__ = 'cpv_codes'

    code: Mapped[str] = mapped_column(String, primary_key=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


class NUTSCode(Base):
    """NUTS (Nomenclature of Territorial Units for Statistics) codes."""
    __tablename__ = 'nuts_codes'

    code: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    level: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    parent_code: Mapped[Optional[str]] = mapped_column(String, ForeignKey('nuts_codes.code'), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Self-referential relationship for hierarchy
    parent: Mapped[Optional["NUTSCode"]] = relationship("NUTSCode", remote_side=[code], backref="children")


# Main data models
class TEDDocument(Base):
    """Main TED document metadata."""
    __tablename__ = 'ted_documents'

    doc_id: Mapped[str] = mapped_column(String, primary_key=True)
    edition: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    reception_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    deletion_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Official Journal references
    official_journal_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    collection: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    journal_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    publication_date: Mapped[date] = mapped_column(Date, nullable=False)

    # Document classification
    document_type_code: Mapped[str] = mapped_column(String, default='7')
    dispatch_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Source document metadata
    source_country: Mapped[Optional[str]] = mapped_column(String, ForeignKey('countries.code'), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    contracting_bodies: Mapped[List["ContractingBody"]] = relationship("ContractingBody", back_populates="document", cascade="all, delete-orphan")
    contracts: Mapped[List["Contract"]] = relationship("Contract", back_populates="document", cascade="all, delete-orphan")
    document_urls: Mapped[List["DocumentURL"]] = relationship("DocumentURL", back_populates="document", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index('idx_ted_documents_pub_date', 'publication_date'),
        Index('idx_ted_documents_country', 'source_country'),
    )


class ContractingBody(Base):
    """Contracting bodies (purchasing organizations)."""
    __tablename__ = 'contracting_bodies'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ted_doc_id: Mapped[str] = mapped_column(String, ForeignKey('ted_documents.doc_id'), nullable=False)

    # Organization details
    official_name: Mapped[str] = mapped_column(Text, nullable=False)
    national_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Address information
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    town: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    country_code: Mapped[Optional[str]] = mapped_column(String, ForeignKey('countries.code'), nullable=True)
    nuts_code: Mapped[Optional[str]] = mapped_column(String, ForeignKey('nuts_codes.code'), nullable=True)

    # Contact information
    contact_point: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    fax: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    url_general: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url_buyer: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Authority classification
    authority_type_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    authority_type_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    main_activity_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    main_activity_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    document: Mapped["TEDDocument"] = relationship("TEDDocument", back_populates="contracting_bodies")
    contracts: Mapped[List["Contract"]] = relationship("Contract", back_populates="contracting_body", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index('idx_contracting_body_document', 'ted_doc_id'),
        Index('idx_contracting_body_country', 'country_code'),
    )


class Contract(Base):
    """Contract objects (the main procurement items)."""
    __tablename__ = 'contracts'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ted_doc_id: Mapped[str] = mapped_column(String, ForeignKey('ted_documents.doc_id'), nullable=False)
    contracting_body_id: Mapped[int] = mapped_column(Integer, ForeignKey('contracting_bodies.id'), nullable=False)

    # Basic contract information
    title: Mapped[str] = mapped_column(Text, nullable=False)
    reference_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    short_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Classification
    main_cpv_code: Mapped[Optional[str]] = mapped_column(String, ForeignKey('cpv_codes.code'), nullable=True)
    contract_nature_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    contract_nature: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Financial information
    total_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    total_value_currency: Mapped[Optional[str]] = mapped_column(String, ForeignKey('currencies.code'), nullable=True)
    estimated_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    estimated_value_currency: Mapped[Optional[str]] = mapped_column(String, ForeignKey('currencies.code'), nullable=True)

    # Contract structure
    has_lots: Mapped[bool] = mapped_column(Boolean, default=False)
    lot_count: Mapped[int] = mapped_column(Integer, default=0)

    # Procedure information
    procedure_type_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    procedure_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    award_criteria_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    award_criteria: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # EU program relation
    is_eu_funded: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    document: Mapped["TEDDocument"] = relationship("TEDDocument", back_populates="contracts")
    contracting_body: Mapped["ContractingBody"] = relationship("ContractingBody", back_populates="contracts")
    lots: Mapped[List["Lot"]] = relationship("Lot", back_populates="contract", cascade="all, delete-orphan")
    awards: Mapped[List["Award"]] = relationship("Award", back_populates="contract", cascade="all, delete-orphan")

    # Many-to-many relationships
    cpv_codes_rel = relationship("CPVCode", secondary=contract_cpv_codes, backref="contracts")
    nuts_codes_rel = relationship("NUTSCode", secondary=contract_nuts_codes, backref="contracts")

    # Indexes
    __table_args__ = (
        Index('idx_contract_document', 'ted_doc_id'),
        Index('idx_contract_body', 'contracting_body_id'),
        Index('idx_contracts_value', 'total_value'),
        Index('idx_contracts_cpv', 'main_cpv_code'),
    )


class Lot(Base):
    """Contract lots (when contracts are divided into parts)."""
    __tablename__ = 'lots'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey('contracts.id'), nullable=False)

    lot_number: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    short_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Financial information specific to this lot
    estimated_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    estimated_value_currency: Mapped[Optional[str]] = mapped_column(String, ForeignKey('currencies.code'), nullable=True)

    # Performance location
    performance_nuts_code: Mapped[Optional[str]] = mapped_column(String, ForeignKey('nuts_codes.code'), nullable=True)
    performance_location: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Award criteria for this lot
    award_criteria_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    award_criteria: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Duration
    duration_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    duration_months: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    contract: Mapped["Contract"] = relationship("Contract", back_populates="lots")
    awards: Mapped[List["Award"]] = relationship("Award", back_populates="lot", cascade="all, delete-orphan")

    # Constraints
    __table_args__ = (
        UniqueConstraint('contract_id', 'lot_number', name='uq_contract_lot_number'),
    )


class Award(Base):
    """Contract awards (the actual winners)."""
    __tablename__ = 'awards'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey('contracts.id'), nullable=False)
    lot_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('lots.id'), nullable=True)

    # Award identification
    contract_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    award_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Award decision
    conclusion_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    is_awarded: Mapped[bool] = mapped_column(Boolean, default=True)
    unsuccessful_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Tender statistics
    tenders_received: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenders_received_sme: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenders_received_other_eu: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenders_received_non_eu: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenders_received_electronic: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Financial details
    awarded_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    awarded_value_currency: Mapped[Optional[str]] = mapped_column(String, ForeignKey('currencies.code'), nullable=True)
    awarded_value_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)

    # Subcontracting
    is_subcontracted: Mapped[bool] = mapped_column(Boolean, default=False)
    subcontracted_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    subcontracted_value_currency: Mapped[Optional[str]] = mapped_column(String, ForeignKey('currencies.code'), nullable=True)
    subcontracting_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    contract: Mapped["Contract"] = relationship("Contract", back_populates="awards")
    lot: Mapped[Optional["Lot"]] = relationship("Lot", back_populates="awards")
    contractors: Mapped[List["Contractor"]] = relationship(
        "Contractor",
        secondary=award_contractors,
        back_populates="awards"
    )

    # Indexes
    __table_args__ = (
        Index('idx_award_contract', 'contract_id'),
        Index('idx_award_lot', 'lot_id'),
        Index('idx_awards_conclusion_date', 'conclusion_date'),
        Index('idx_awards_value', 'awarded_value'),
    )


class Contractor(Base):
    """Contractors (winning companies/organizations)."""
    __tablename__ = 'contractors'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Organization details
    official_name: Mapped[str] = mapped_column(Text, nullable=False)
    national_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Address information
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    town: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    country_code: Mapped[Optional[str]] = mapped_column(String, ForeignKey('countries.code'), nullable=True)
    nuts_code: Mapped[Optional[str]] = mapped_column(String, ForeignKey('nuts_codes.code'), nullable=True)

    # Contact information
    phone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    fax: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Business classification
    is_sme: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    awards: Mapped[List["Award"]] = relationship(
        "Award",
        secondary=award_contractors,
        back_populates="contractors"
    )

    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('official_name', 'country_code', name='uq_contractor_name_country'),
        Index('idx_contractors_country', 'country_code'),
        Index('idx_contractors_sme', 'is_sme'),
    )


class DocumentURL(Base):
    """Document URLs and links."""
    __tablename__ = 'document_urls'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ted_doc_id: Mapped[str] = mapped_column(String, ForeignKey('ted_documents.doc_id'), nullable=False)
    url_type: Mapped[str] = mapped_column(String, nullable=False)  # 'GENERAL', 'BUYER', 'DOCUMENT', etc.
    url: Mapped[str] = mapped_column(Text, nullable=False)
    language_code: Mapped[Optional[str]] = mapped_column(String, ForeignKey('languages.code'), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    document: Mapped["TEDDocument"] = relationship("TEDDocument", back_populates="document_urls")


class Translation(Base):
    """Multi-language translations for titles and descriptions."""
    __tablename__ = 'translations'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    record_id: Mapped[int] = mapped_column(Integer, nullable=False)
    field_name: Mapped[str] = mapped_column(String, nullable=False)
    language_code: Mapped[str] = mapped_column(String, ForeignKey('languages.code'), nullable=False)
    translated_text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('table_name', 'record_id', 'field_name', 'language_code', name='uq_translation'),
        Index('idx_translations_lookup', 'table_name', 'record_id', 'field_name'),
    )
