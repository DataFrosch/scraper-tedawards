-- TED Contract Award Notices Database Schema
-- This schema is designed for analyzing EU procurement contract awards
-- Generated from TED XML data dumps

-- Drop existing tables in dependency order
DROP TABLE IF EXISTS award_contractors CASCADE;
DROP TABLE IF EXISTS contract_cpv_codes CASCADE;
DROP TABLE IF EXISTS contract_nuts_codes CASCADE;
DROP TABLE IF EXISTS translations CASCADE;
DROP TABLE IF EXISTS document_urls CASCADE;
DROP TABLE IF EXISTS awards CASCADE;
DROP TABLE IF EXISTS lots CASCADE;
DROP TABLE IF EXISTS contracts CASCADE;
DROP TABLE IF EXISTS contracting_bodies CASCADE;
DROP TABLE IF EXISTS ted_documents CASCADE;
DROP TABLE IF EXISTS cpv_codes CASCADE;
DROP TABLE IF EXISTS nuts_codes CASCADE;
DROP TABLE IF EXISTS currencies CASCADE;
DROP TABLE IF EXISTS languages CASCADE;
DROP TABLE IF EXISTS countries CASCADE;

-- Core reference tables
CREATE TABLE countries (
    code VARCHAR(2) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE languages (
    code VARCHAR(2) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE currencies (
    code VARCHAR(3) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CPV (Common Procurement Vocabulary) codes for classification
CREATE TABLE cpv_codes (
    code VARCHAR(20) PRIMARY KEY,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- NUTS (Nomenclature of Territorial Units for Statistics) codes
CREATE TABLE nuts_codes (
    code VARCHAR(10) PRIMARY KEY,
    name VARCHAR(200),
    level INTEGER,
    parent_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_code) REFERENCES nuts_codes(code)
);

-- Main TED documents table
CREATE TABLE ted_documents (
    doc_id VARCHAR(20) PRIMARY KEY, -- e.g., "000001-2024"
    edition VARCHAR(20) NOT NULL,   -- e.g., "2024001"
    version VARCHAR(50) NOT NULL,   -- e.g., "R2.0.9.S05.E01"
    reception_id VARCHAR(50),       -- Internal tracking ID
    deletion_date DATE,             -- Archive deletion date
    form_language VARCHAR(2) NOT NULL REFERENCES languages(code),

    -- Official Journal references
    official_journal_ref VARCHAR(50), -- e.g., "2024/S 001-000001"
    collection VARCHAR(5),             -- e.g., "S"
    journal_number INTEGER,
    publication_date DATE NOT NULL,

    -- Document classification
    document_type_code VARCHAR(5) DEFAULT '7', -- Always "7" for award notices
    dispatch_date DATE,

    -- Source document metadata
    original_language VARCHAR(2) REFERENCES languages(code),
    source_country VARCHAR(2) REFERENCES countries(code),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contracting bodies (purchasing organizations)
CREATE TABLE contracting_bodies (
    id SERIAL PRIMARY KEY,
    ted_doc_id VARCHAR(20) NOT NULL REFERENCES ted_documents(doc_id),

    -- Organization details
    official_name TEXT NOT NULL,
    national_id VARCHAR(50),        -- National registration number

    -- Address information
    address TEXT,
    town VARCHAR(100),
    postal_code VARCHAR(20),
    country_code VARCHAR(2) REFERENCES countries(code),
    nuts_code VARCHAR(10) REFERENCES nuts_codes(code),

    -- Contact information
    contact_point VARCHAR(200),
    phone VARCHAR(50),
    email VARCHAR(200),
    fax VARCHAR(50),
    url_general TEXT,
    url_buyer TEXT,                 -- Buyer portal URL

    -- Authority classification
    authority_type_code VARCHAR(5), -- e.g., "3" for regional/local
    authority_type_description TEXT,
    main_activity_code VARCHAR(5),
    main_activity_description TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contract objects (the main procurement items)
CREATE TABLE contracts (
    id SERIAL PRIMARY KEY,
    ted_doc_id VARCHAR(20) NOT NULL REFERENCES ted_documents(doc_id),
    contracting_body_id INTEGER NOT NULL REFERENCES contracting_bodies(id),

    -- Basic contract information
    title TEXT NOT NULL,
    reference_number VARCHAR(100),  -- Contracting body's reference
    short_description TEXT,

    -- Classification
    main_cpv_code VARCHAR(20) REFERENCES cpv_codes(code),
    contract_nature_code VARCHAR(5), -- Services, Works, Supplies
    contract_nature TEXT,

    -- Financial information
    total_value DECIMAL(15,2),
    total_value_currency VARCHAR(3) REFERENCES currencies(code),
    estimated_value DECIMAL(15,2),
    estimated_value_currency VARCHAR(3) REFERENCES currencies(code),

    -- Contract structure
    has_lots BOOLEAN DEFAULT FALSE,
    lot_count INTEGER DEFAULT 0,

    -- Procedure information
    procedure_type_code VARCHAR(5),
    procedure_type TEXT,
    award_criteria_code VARCHAR(5), -- 1=Lowest price, 2=Most economic
    award_criteria TEXT,

    -- EU program relation
    is_eu_funded BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contract lots (when contracts are divided into parts)
CREATE TABLE lots (
    id SERIAL PRIMARY KEY,
    contract_id INTEGER NOT NULL REFERENCES contracts(id),

    lot_number INTEGER NOT NULL,
    title TEXT NOT NULL,
    short_description TEXT,

    -- Financial information specific to this lot
    estimated_value DECIMAL(15,2),
    estimated_value_currency VARCHAR(3) REFERENCES currencies(code),

    -- Performance location
    performance_nuts_code VARCHAR(10) REFERENCES nuts_codes(code),
    performance_location TEXT,

    -- Award criteria for this lot
    award_criteria_code VARCHAR(5),
    award_criteria TEXT,

    -- Duration
    duration_days INTEGER,
    duration_months INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(contract_id, lot_number)
);

-- Contract awards (the actual winners)
CREATE TABLE awards (
    id SERIAL PRIMARY KEY,
    contract_id INTEGER NOT NULL REFERENCES contracts(id),
    lot_id INTEGER REFERENCES lots(id),

    -- Award identification
    contract_number VARCHAR(100),
    award_title TEXT,

    -- Award decision
    conclusion_date DATE NOT NULL,
    is_awarded BOOLEAN DEFAULT TRUE,    -- Some lots can be unsuccessful
    unsuccessful_reason TEXT,

    -- Tender statistics
    tenders_received INTEGER,
    tenders_received_sme INTEGER,       -- Small/Medium Enterprises
    tenders_received_other_eu INTEGER,
    tenders_received_non_eu INTEGER,
    tenders_received_electronic INTEGER,

    -- Financial details
    awarded_value DECIMAL(15,2),
    awarded_value_currency VARCHAR(3) REFERENCES currencies(code),
    awarded_value_eur DECIMAL(15,2),   -- Converted to EUR for analysis

    -- Subcontracting
    is_subcontracted BOOLEAN DEFAULT FALSE,
    subcontracted_value DECIMAL(15,2),
    subcontracted_value_currency VARCHAR(3) REFERENCES currencies(code),
    subcontracting_description TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contractors (winning companies/organizations)
CREATE TABLE contractors (
    id SERIAL PRIMARY KEY,

    -- Organization details
    official_name TEXT NOT NULL,
    national_id VARCHAR(50),

    -- Address information
    address TEXT,
    town VARCHAR(100),
    postal_code VARCHAR(20),
    country_code VARCHAR(2) REFERENCES countries(code),
    nuts_code VARCHAR(10) REFERENCES nuts_codes(code),

    -- Contact information
    phone VARCHAR(50),
    email VARCHAR(200),
    fax VARCHAR(50),
    url TEXT,

    -- Business classification
    is_sme BOOLEAN,                     -- Small/Medium Enterprise

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Many-to-many relationship tables

-- Link awards to contractors (supports joint ventures)
CREATE TABLE award_contractors (
    award_id INTEGER NOT NULL REFERENCES awards(id) ON DELETE CASCADE,
    contractor_id INTEGER NOT NULL REFERENCES contractors(id),
    is_lead_contractor BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (award_id, contractor_id)
);

-- Link contracts to CPV codes (contracts can have multiple classifications)
CREATE TABLE contract_cpv_codes (
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    cpv_code VARCHAR(20) NOT NULL REFERENCES cpv_codes(code),
    is_main BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (contract_id, cpv_code)
);

-- Link contracts to NUTS codes (performance locations)
CREATE TABLE contract_nuts_codes (
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    nuts_code VARCHAR(10) NOT NULL REFERENCES nuts_codes(code),
    nuts_type VARCHAR(20) NOT NULL, -- 'PERFORMANCE', 'CA_CE', 'TENDERER'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (contract_id, nuts_code, nuts_type)
);

-- Document URLs and links
CREATE TABLE document_urls (
    id SERIAL PRIMARY KEY,
    ted_doc_id VARCHAR(20) NOT NULL REFERENCES ted_documents(doc_id),
    url_type VARCHAR(50) NOT NULL,    -- 'GENERAL', 'BUYER', 'DOCUMENT', etc.
    url TEXT NOT NULL,
    language_code VARCHAR(2) REFERENCES languages(code),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Multi-language translations for titles and descriptions
CREATE TABLE translations (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,  -- Which table this translation belongs to
    record_id INTEGER NOT NULL,       -- ID of the record in that table
    field_name VARCHAR(50) NOT NULL,  -- Which field is translated
    language_code VARCHAR(2) NOT NULL REFERENCES languages(code),
    translated_text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(table_name, record_id, field_name, language_code)
);

-- Indexes for better query performance
CREATE INDEX idx_ted_documents_pub_date ON ted_documents(publication_date);
CREATE INDEX idx_ted_documents_country ON ted_documents(source_country);
CREATE INDEX idx_contracts_value ON contracts(total_value);
CREATE INDEX idx_contracts_cpv ON contracts(main_cpv_code);
CREATE INDEX idx_awards_conclusion_date ON awards(conclusion_date);
CREATE INDEX idx_awards_value ON awards(awarded_value);
CREATE INDEX idx_contractors_country ON contractors(country_code);
CREATE INDEX idx_contractors_sme ON contractors(is_sme);
CREATE INDEX idx_translations_lookup ON translations(table_name, record_id, field_name);

-- Table comments for documentation
COMMENT ON TABLE ted_documents IS 'Main TED document metadata and references';
COMMENT ON TABLE contracting_bodies IS 'Organizations that issue procurement contracts';
COMMENT ON TABLE contracts IS 'Procurement contract objects being awarded';
COMMENT ON TABLE lots IS 'Individual lots when contracts are divided into parts';
COMMENT ON TABLE awards IS 'Award decisions for contracts or lots';
COMMENT ON TABLE contractors IS 'Companies and organizations that win contracts';
COMMENT ON TABLE cpv_codes IS 'Common Procurement Vocabulary classification codes';
COMMENT ON TABLE nuts_codes IS 'NUTS geographic classification codes';
COMMENT ON TABLE translations IS 'Multi-language translations for text fields';