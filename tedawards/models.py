"""
SQLAlchemy models for TED awards database.
Designed for SQLite with easy PostgreSQL migration support.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Integer,
    Numeric, String, Text, Table, UniqueConstraint, Index
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


# Association table for award-contractor many-to-many relationship
award_contractors = Table(
    'award_contractors',
    Base.metadata,
    Column('award_id', Integer, ForeignKey('awards.id', ondelete='CASCADE'), primary_key=True),
    Column('contractor_id', Integer, ForeignKey('contractors.id'), primary_key=True),
    Column('created_at', DateTime, default=func.now())
)


class TEDDocument(Base):
    """Main TED document metadata."""
    __tablename__ = 'ted_documents'

    doc_id: Mapped[str] = mapped_column(String, primary_key=True)
    edition: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    version: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    reception_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    deletion_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    official_journal_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    publication_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    dispatch_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    source_country: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    contracting_bodies: Mapped[List["ContractingBody"]] = relationship(
        "ContractingBody", back_populates="document", cascade="all, delete-orphan"
    )
    contracts: Mapped[List["Contract"]] = relationship(
        "Contract", back_populates="document", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index('idx_ted_documents_pub_date', 'publication_date'),
        Index('idx_ted_documents_country', 'source_country'),
    )


class ContractingBody(Base):
    """Contracting bodies (purchasing organizations)."""
    __tablename__ = 'contracting_bodies'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ted_doc_id: Mapped[str] = mapped_column(String, ForeignKey('ted_documents.doc_id'), nullable=False)
    official_name: Mapped[str] = mapped_column(Text, nullable=False)
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    town: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    country_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    nuts_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    fax: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    url_general: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url_buyer: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    contact_point: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    authority_type_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    main_activity_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    document: Mapped["TEDDocument"] = relationship("TEDDocument", back_populates="contracting_bodies")
    contracts: Mapped[List["Contract"]] = relationship(
        "Contract", back_populates="contracting_body", cascade="all, delete-orphan"
    )

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
    title: Mapped[str] = mapped_column(Text, nullable=False)
    reference_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    short_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    main_cpv_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    contract_nature_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    total_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    total_value_currency: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    procedure_type_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    award_criteria_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    document: Mapped["TEDDocument"] = relationship("TEDDocument", back_populates="contracts")
    contracting_body: Mapped["ContractingBody"] = relationship("ContractingBody", back_populates="contracts")
    awards: Mapped[List["Award"]] = relationship(
        "Award", back_populates="contract", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index('idx_contract_document', 'ted_doc_id'),
        Index('idx_contract_body', 'contracting_body_id'),
        Index('idx_contracts_value', 'total_value'),
        Index('idx_contracts_cpv', 'main_cpv_code'),
    )


class Award(Base):
    """Contract awards (the actual winners)."""
    __tablename__ = 'awards'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey('contracts.id'), nullable=False)
    contract_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    award_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    conclusion_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    tenders_received: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenders_received_sme: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenders_received_other_eu: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenders_received_non_eu: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenders_received_electronic: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    awarded_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    awarded_value_currency: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    subcontracted_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    subcontracted_value_currency: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    subcontracting_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    contract: Mapped["Contract"] = relationship("Contract", back_populates="awards")
    contractors: Mapped[List["Contractor"]] = relationship(
        "Contractor",
        secondary=award_contractors,
        back_populates="awards"
    )

    __table_args__ = (
        Index('idx_award_contract', 'contract_id'),
        Index('idx_awards_conclusion_date', 'conclusion_date'),
        Index('idx_awards_value', 'awarded_value'),
    )


class Contractor(Base):
    """Contractors (winning companies/organizations)."""
    __tablename__ = 'contractors'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    official_name: Mapped[str] = mapped_column(Text, nullable=False)
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    town: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    country_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    nuts_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    fax: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_sme: Mapped[bool] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    awards: Mapped[List["Award"]] = relationship(
        "Award",
        secondary=award_contractors,
        back_populates="contractors"
    )

    __table_args__ = (
        UniqueConstraint('official_name', 'country_code', name='uq_contractor_name_country'),
        Index('idx_contractors_country', 'country_code'),
        Index('idx_contractors_sme', 'is_sme'),
    )
