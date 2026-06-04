"""
SQLAlchemy models for the awards database.
Organizations (buyers and contractors) are normalized into a shared lookup table
with exact-match deduplication via a composite unique constraint.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DDL,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Table,
    Text,
    Index,
    UniqueConstraint,
    event,
    func,
    text,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


event.listen(
    Base.metadata,
    "before_create",
    DDL("CREATE EXTENSION IF NOT EXISTS pg_trgm"),
)

event.listen(
    Base.metadata,
    "before_drop",
    DDL("DROP MATERIALIZED VIEW IF EXISTS awards_adjusted"),
)


# Junction table for many-to-many relationship between awards and contractors
award_contractors = Table(
    "award_contractors",
    Base.metadata,
    Column(
        "award_id",
        Integer,
        ForeignKey("awards.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "organization_id",
        Integer,
        ForeignKey("organizations.id"),
        primary_key=True,
    ),
    Index("idx_award_contractors_org", "organization_id"),
)


class CpvCode(Base):
    """CPV code lookup table with code as natural primary key."""

    __tablename__ = "cpv_codes"

    code: Mapped[str] = mapped_column(String, primary_key=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class ProcedureType(Base):
    """Procedure type lookup table with code as natural primary key."""

    __tablename__ = "procedure_types"

    code: Mapped[str] = mapped_column(String, primary_key=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class AuthorityType(Base):
    """Authority type lookup table with code as natural primary key."""

    __tablename__ = "authority_types"

    code: Mapped[str] = mapped_column(String, primary_key=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class Country(Base):
    """Country lookup table with ISO 3166-1 alpha-2 code as primary key."""

    __tablename__ = "countries"

    code: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


# Junction table for many-to-many relationship between contracts and CPV codes
contract_cpv_codes = Table(
    "contract_cpv_codes",
    Base.metadata,
    Column(
        "contract_id",
        Integer,
        ForeignKey("contracts.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "cpv_code",
        String,
        ForeignKey("cpv_codes.code"),
        primary_key=True,
    ),
    Index("idx_contract_cpv_codes_cpv", "cpv_code"),
)


class Organization(Base):
    """Shared organization lookup table (exact-match deduplication).

    Used for both buyers (contracting bodies) and contractors.
    """

    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    official_name: Mapped[str] = mapped_column(Text, nullable=False)
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    town: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    country_code: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("countries.code"), nullable=True
    )
    nuts_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # Relationships
    documents: Mapped[List["Document"]] = relationship(
        "Document", back_populates="buyer_organization"
    )
    awards: Mapped[List["Award"]] = relationship(
        "Award", secondary=award_contractors, back_populates="contractors"
    )

    __table_args__ = (
        UniqueConstraint(
            "official_name",
            "address",
            "town",
            "postal_code",
            "country_code",
            "nuts_code",
            name="uq_organization_identity",
            postgresql_nulls_not_distinct=True,
        ),
        Index("idx_organization_country", "country_code"),
        Index("idx_organization_nuts", "nuts_code"),
        Index(
            "idx_organization_name_trgm",
            "official_name",
            postgresql_using="gin",
            postgresql_ops={"official_name": "gin_trgm_ops"},
        ),
    )


class Document(Base):
    """Document metadata."""

    __tablename__ = "documents"

    doc_id: Mapped[str] = mapped_column(String, primary_key=True)
    edition: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    version: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    reception_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    official_journal_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    publication_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    dispatch_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    source_country: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("countries.code"), nullable=True
    )
    contact_point: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    url_general: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    buyer_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    buyer_organization_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("organizations.id"), nullable=False
    )
    buyer_authority_type_code: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("authority_types.code"), nullable=True
    )
    buyer_main_activity_code: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    # Relationships
    buyer_organization: Mapped["Organization"] = relationship(
        "Organization", back_populates="documents"
    )
    contracts: Mapped[List["Contract"]] = relationship(
        "Contract", back_populates="document", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_documents_pub_date", "publication_date"),
        Index(
            "idx_documents_pub_year",
            text("extract(year from publication_date)"),
        ),
        Index("idx_documents_country", "source_country"),
        Index("idx_documents_buyer_org", "buyer_organization_id"),
    )


class Contract(Base):
    """Contract objects (the main procurement items)."""

    __tablename__ = "contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doc_id: Mapped[str] = mapped_column(
        String, ForeignKey("documents.doc_id", ondelete="CASCADE"), nullable=False
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    short_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    main_cpv_code: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("cpv_codes.code"), nullable=True
    )
    contract_nature_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    nuts_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    procedure_type_code: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("procedure_types.code"), nullable=True
    )
    accelerated: Mapped[bool] = mapped_column(
        default=False, server_default="false", nullable=False
    )
    estimated_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(24, 2), nullable=True
    )
    estimated_value_currency: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    framework_agreement: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    eu_funded: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    # Relationships
    document: Mapped["Document"] = relationship("Document", back_populates="contracts")
    awards: Mapped[List["Award"]] = relationship(
        "Award", back_populates="contract", cascade="all, delete-orphan"
    )
    cpv_codes: Mapped[List["CpvCode"]] = relationship(
        "CpvCode", secondary=contract_cpv_codes
    )

    __table_args__ = (
        Index("idx_contract_document", "doc_id"),
        Index("idx_contracts_nuts", "nuts_code"),
        Index("idx_contracts_procedure", "procedure_type_code"),
        Index("idx_contracts_main_cpv", "main_cpv_code"),
    )


class Award(Base):
    """Contract awards (the actual winners)."""

    __tablename__ = "awards"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False
    )
    contract_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    award_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tenders_received: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    awarded_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(24, 2), nullable=True
    )
    awarded_value_currency: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    award_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    lot_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    contract_start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    contract_end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    # Relationships
    contract: Mapped["Contract"] = relationship("Contract", back_populates="awards")
    contractors: Mapped[List["Organization"]] = relationship(
        "Organization", secondary=award_contractors, back_populates="awards"
    )

    __table_args__ = (
        Index("idx_award_contract", "contract_id"),
        Index("idx_awards_tenders_received", "tenders_received"),
    )


class ExchangeRate(Base):
    """Monthly ECB exchange rates (1 EUR = X units of currency)."""

    __tablename__ = "exchange_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    rate: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)

    __table_args__ = (
        UniqueConstraint("currency", "year", "month", name="uq_exchange_rate"),
    )


class PriceIndex(Base):
    """Annual Eurostat HICP price index (euro area average)."""

    __tablename__ = "price_indices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    index_value: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)


class ProcessedPackage(Base):
    """Record of TED daily packages that have been imported.

    Persists which OJ issues were processed so download/import stay resumable
    after the extracted XML is deleted from disk.
    """

    __tablename__ = "processed_packages"

    package_number: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False
    )
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (Index("idx_processed_packages_year", "year"),)


class OrganizationIdentifier(Base):
    """Organization identifiers (SIRET, VAT, KVK, etc.)."""

    __tablename__ = "organization_identifiers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scheme: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    identifier: Mapped[str] = mapped_column(String, nullable=False)
    organization_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "scheme",
            "identifier",
            "organization_id",
            name="uq_org_identifier",
            postgresql_nulls_not_distinct=True,
        ),
        Index("idx_org_id_org", "organization_id"),
        Index("idx_org_id_scheme_id", "scheme", "identifier"),
    )
