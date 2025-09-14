"""
Pydantic models for TED awards data structure.
Shared across all parsers to ensure consistent data format.
"""

from datetime import date
from typing import List, Optional
from pydantic import BaseModel, Field, validator


class DocumentModel(BaseModel):
    """Document metadata model."""
    doc_id: str = Field(..., description="Document identifier")
    edition: Optional[str] = Field(None, description="Document edition")
    version: Optional[str] = Field(None, description="Document version")
    reception_id: Optional[str] = Field(None, description="Reception identifier")
    deletion_date: Optional[date] = Field(None, description="Deletion date")
    form_language: str = Field(..., description="Form language code")
    official_journal_ref: Optional[str] = Field(None, description="Official Journal reference")
    publication_date: Optional[date] = Field(None, description="Publication date")
    dispatch_date: Optional[date] = Field(None, description="Dispatch date")
    original_language: Optional[str] = Field(None, description="Original language code")
    source_country: Optional[str] = Field(None, description="Source country code")


class ContractingBodyModel(BaseModel):
    """Contracting body model."""
    official_name: str = Field(..., description="Official name of contracting body")
    address: Optional[str] = Field(None, description="Address")
    town: Optional[str] = Field(None, description="Town/city")
    postal_code: Optional[str] = Field(None, description="Postal code")
    country_code: Optional[str] = Field(None, description="Country code")
    nuts_code: Optional[str] = Field(None, description="NUTS code")
    contact_point: Optional[str] = Field(None, description="Contact point")
    phone: Optional[str] = Field(None, description="Phone number")
    email: Optional[str] = Field(None, description="Email address")
    fax: Optional[str] = Field(None, description="Fax number")
    url_general: Optional[str] = Field(None, description="General URL")
    url_buyer: Optional[str] = Field(None, description="Buyer profile URL")
    authority_type_code: Optional[str] = Field(None, description="Authority type code")
    main_activity_code: Optional[str] = Field(None, description="Main activity code")


class ContractModel(BaseModel):
    """Contract model."""
    title: str = Field(..., description="Contract title")
    reference_number: Optional[str] = Field(None, description="Reference number")
    short_description: Optional[str] = Field(None, description="Short description")
    main_cpv_code: Optional[str] = Field(None, description="Main CPV code")
    contract_nature_code: Optional[str] = Field(None, description="Contract nature code")
    total_value: Optional[float] = Field(None, description="Total contract value")
    total_value_currency: Optional[str] = Field(None, description="Total value currency")
    procedure_type_code: Optional[str] = Field(None, description="Procedure type code")
    award_criteria_code: Optional[str] = Field(None, description="Award criteria code")
    performance_nuts_code: Optional[str] = Field(None, description="Performance NUTS code")


class ContractorModel(BaseModel):
    """Contractor model."""
    official_name: str = Field(..., description="Official name of contractor")
    address: Optional[str] = Field(None, description="Address")
    town: Optional[str] = Field(None, description="Town/city")
    postal_code: Optional[str] = Field(None, description="Postal code")
    country_code: Optional[str] = Field(None, description="Country code")
    nuts_code: Optional[str] = Field(None, description="NUTS code")
    phone: Optional[str] = Field(None, description="Phone number")
    email: Optional[str] = Field(None, description="Email address")
    fax: Optional[str] = Field(None, description="Fax number")
    url: Optional[str] = Field(None, description="URL")
    is_sme: bool = Field(False, description="Is small/medium enterprise")


class AwardModel(BaseModel):
    """Award model."""
    award_title: Optional[str] = Field(None, description="Award title")
    conclusion_date: Optional[date] = Field(None, description="Conclusion date")
    contract_number: Optional[str] = Field(None, description="Contract number")
    tenders_received: Optional[int] = Field(None, description="Number of tenders received")
    tenders_received_sme: Optional[int] = Field(None, description="Tenders received from SMEs")
    tenders_received_other_eu: Optional[int] = Field(None, description="Tenders from other EU countries")
    tenders_received_non_eu: Optional[int] = Field(None, description="Tenders from non-EU countries")
    tenders_received_electronic: Optional[int] = Field(None, description="Electronic tenders received")
    awarded_value: Optional[float] = Field(None, description="Awarded value")
    awarded_value_currency: Optional[str] = Field(None, description="Awarded value currency")
    subcontracted_value: Optional[float] = Field(None, description="Subcontracted value")
    subcontracted_value_currency: Optional[str] = Field(None, description="Subcontracted value currency")
    subcontracting_description: Optional[str] = Field(None, description="Subcontracting description")
    contractors: List[ContractorModel] = Field(default_factory=list, description="List of contractors")

    @validator('contractors', pre=True)
    def ensure_contractors_list(cls, v):
        """Ensure contractors is always a list."""
        if not v:
            return []
        return v


class TedAwardDataModel(BaseModel):
    """Complete TED award data model - this is what all parsers should return."""
    document: DocumentModel = Field(..., description="Document metadata")
    contracting_body: ContractingBodyModel = Field(..., description="Contracting body information")
    contract: ContractModel = Field(..., description="Contract information")
    awards: List[AwardModel] = Field(..., description="List of awards")

    @validator('awards', pre=True)
    def ensure_awards_list(cls, v):
        """Ensure awards is always a list with at least one award."""
        if not v:
            raise ValueError("Awards list cannot be empty")
        return v


class TedParserResultModel(BaseModel):
    """Parser result model - can contain multiple award documents for text format."""
    awards: List[TedAwardDataModel] = Field(..., description="List of award data documents")

    @validator('awards', pre=True)
    def ensure_awards_list(cls, v):
        """Ensure awards is always a list."""
        if not isinstance(v, list):
            return [v]
        return v


# Helper functions for parsers
def normalize_date_string(date_str: Optional[str]) -> Optional[date]:
    """Helper to normalize date strings to date objects."""
    if not date_str:
        return None

    if isinstance(date_str, date):
        return date_str

    if isinstance(date_str, str):
        try:
            # Try ISO format first
            from datetime import datetime
            return datetime.fromisoformat(date_str).date()
        except ValueError:
            pass

    return None