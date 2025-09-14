import logging
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from pathlib import Path
from typing import Dict, List
from .config import config
from .schema import TedAwardDataModel

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handle database operations for TED awards data."""

    def __init__(self):
        self.conn = None
        self._reference_data_cache = {
            'languages': set(),
            'countries': set(),
            'nuts_codes': set(),
            'currencies': set(),
            'cpv_codes': set()
        }

    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                cursor_factory=RealDictCursor
            )
            logger.info("Connected to database")
            self._ensure_schema_exists()
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def document_exists(self, doc_id: str) -> bool:
        """Check if document already exists in database."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM ted_documents WHERE doc_id = %s", (doc_id,))
            return cur.fetchone() is not None

    def save_award_data(self, data: TedAwardDataModel):
        """Save parsed award data to database."""
        try:
            with self.conn.cursor() as cur:
                # Collect reference data
                self._collect_reference_data(data)

                # Flush reference data in smaller batches for single records
                self._flush_reference_data(cur)

                # Save document
                self._insert_document(cur, data.document.dict())

                # Save contracting body
                cb_id = self._insert_contracting_body(cur, data.document.doc_id, data.contracting_body.dict())

                # Save contract
                contract_id = self._insert_contract(cur, data.document.doc_id, cb_id, data.contract.dict())

                # Save awards
                for award_data in data.awards:
                    award_id = self._insert_award(cur, contract_id, award_data.dict())

                    for contractor_data in award_data.contractors:
                        contractor_id = self._insert_contractor(cur, contractor_data.dict())
                        self._link_award_contractor(cur, award_id, contractor_id)

            self.conn.commit()
            logger.info(f"Saved award data for document {data.document.doc_id}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error saving award data: {e}")
            raise

    def _insert_document(self, cur, data):
        """Insert document record."""
        sql = """
        INSERT INTO ted_documents (
            doc_id, edition, version, reception_id, deletion_date, form_language,
            official_journal_ref, publication_date, dispatch_date, original_language, source_country
        ) VALUES (%(doc_id)s, %(edition)s, %(version)s, %(reception_id)s, %(deletion_date)s,
                  %(form_language)s, %(official_journal_ref)s, %(publication_date)s,
                  %(dispatch_date)s, %(original_language)s, %(source_country)s)
        ON CONFLICT (doc_id) DO NOTHING
        """
        cur.execute(sql, data)

    def _insert_contracting_body(self, cur, doc_id, data):
        """Insert contracting body and return ID."""
        data['ted_doc_id'] = doc_id
        sql = """
        INSERT INTO contracting_bodies (
            ted_doc_id, official_name, address, town, postal_code, country_code,
            nuts_code, contact_point, phone, email, fax, url_general, url_buyer,
            authority_type_code, main_activity_code
        ) VALUES (%(ted_doc_id)s, %(official_name)s, %(address)s, %(town)s, %(postal_code)s,
                  %(country_code)s, %(nuts_code)s, %(contact_point)s, %(phone)s,
                  %(email)s, %(fax)s, %(url_general)s, %(url_buyer)s,
                  %(authority_type_code)s, %(main_activity_code)s)
        RETURNING id
        """
        cur.execute(sql, data)
        return cur.fetchone()['id']

    def _insert_contract(self, cur, doc_id, cb_id, data):
        """Insert contract and return ID."""
        data.update({'ted_doc_id': doc_id, 'contracting_body_id': cb_id})
        sql = """
        INSERT INTO contracts (
            ted_doc_id, contracting_body_id, title, reference_number, short_description,
            main_cpv_code, contract_nature_code, total_value, total_value_currency,
            procedure_type_code, award_criteria_code
        ) VALUES (%(ted_doc_id)s, %(contracting_body_id)s, %(title)s, %(reference_number)s,
                  %(short_description)s, %(main_cpv_code)s, %(contract_nature_code)s,
                  %(total_value)s, %(total_value_currency)s, %(procedure_type_code)s,
                  %(award_criteria_code)s)
        RETURNING id
        """
        cur.execute(sql, data)
        return cur.fetchone()['id']

    def _insert_award(self, cur, contract_id, data):
        """Insert award and return ID."""
        data['contract_id'] = contract_id
        sql = """
        INSERT INTO awards (
            contract_id, award_title, conclusion_date, contract_number,
            tenders_received, tenders_received_sme, tenders_received_other_eu,
            tenders_received_non_eu, tenders_received_electronic,
            awarded_value, awarded_value_currency,
            subcontracted_value, subcontracted_value_currency, subcontracting_description
        ) VALUES (%(contract_id)s, %(award_title)s, %(conclusion_date)s, %(contract_number)s,
                  %(tenders_received)s, %(tenders_received_sme)s, %(tenders_received_other_eu)s,
                  %(tenders_received_non_eu)s, %(tenders_received_electronic)s,
                  %(awarded_value)s, %(awarded_value_currency)s,
                  %(subcontracted_value)s, %(subcontracted_value_currency)s,
                  %(subcontracting_description)s)
        RETURNING id
        """
        cur.execute(sql, data)
        return cur.fetchone()['id']

    def _insert_contractor(self, cur, data):
        """Insert contractor and return ID, or return existing ID."""
        # First try to find existing contractor by name and country
        cur.execute("""
            SELECT id FROM contractors
            WHERE official_name = %(official_name)s AND country_code = %(country_code)s
        """, data)

        existing = cur.fetchone()
        if existing:
            return existing['id']

        # Insert new contractor
        sql = """
        INSERT INTO contractors (
            official_name, address, town, postal_code, country_code, nuts_code,
            phone, email, fax, url, is_sme
        ) VALUES (%(official_name)s, %(address)s, %(town)s, %(postal_code)s,
                  %(country_code)s, %(nuts_code)s, %(phone)s, %(email)s,
                  %(fax)s, %(url)s, %(is_sme)s)
        RETURNING id
        """
        cur.execute(sql, data)
        return cur.fetchone()['id']

    def _link_award_contractor(self, cur, award_id, contractor_id):
        """Link award to contractor."""
        sql = """
        INSERT INTO award_contractors (award_id, contractor_id)
        VALUES (%s, %s)
        ON CONFLICT (award_id, contractor_id) DO NOTHING
        """
        cur.execute(sql, (award_id, contractor_id))

    def _collect_reference_data(self, data: TedAwardDataModel):
        """Collect reference data from award data into cache."""
        # Languages
        if data.document.form_language:
            self._reference_data_cache['languages'].add(data.document.form_language)
        if data.document.original_language:
            self._reference_data_cache['languages'].add(data.document.original_language)
        self._reference_data_cache['languages'].add('')  # Empty case

        # Countries
        if data.document.source_country:
            self._reference_data_cache['countries'].add(data.document.source_country)
        if data.contracting_body.country_code:
            self._reference_data_cache['countries'].add(data.contracting_body.country_code)

        for award_data in data.awards:
            for contractor in award_data.contractors:
                if contractor.country_code:
                    self._reference_data_cache['countries'].add(contractor.country_code)

        self._reference_data_cache['countries'].add('')  # Empty case

        # NUTS codes
        if data.contracting_body.nuts_code:
            self._reference_data_cache['nuts_codes'].add(data.contracting_body.nuts_code)
        if data.contract.performance_nuts_code:
            self._reference_data_cache['nuts_codes'].add(data.contract.performance_nuts_code)

        for award_data in data.awards:
            for contractor in award_data.contractors:
                if contractor.nuts_code:
                    self._reference_data_cache['nuts_codes'].add(contractor.nuts_code)

        self._reference_data_cache['nuts_codes'].add('')  # Empty case

        # Currencies
        if data.contract.total_value_currency:
            self._reference_data_cache['currencies'].add(data.contract.total_value_currency)

        for award_data in data.awards:
            if award_data.awarded_value_currency:
                self._reference_data_cache['currencies'].add(award_data.awarded_value_currency)
            if award_data.subcontracted_value_currency:
                self._reference_data_cache['currencies'].add(award_data.subcontracted_value_currency)

        self._reference_data_cache['currencies'].add('')  # Empty case

        # CPV codes
        if data.contract.main_cpv_code:
            self._reference_data_cache['cpv_codes'].add(data.contract.main_cpv_code)
        self._reference_data_cache['cpv_codes'].add('')  # Empty case

    def _flush_reference_data(self, cur):
        """Batch insert all collected reference data."""
        # Languages
        if self._reference_data_cache['languages']:
            lang_data = [(lang, lang if lang else 'Unknown') for lang in self._reference_data_cache['languages']]
            execute_values(
                cur,
                "INSERT INTO languages (code, name) VALUES %s ON CONFLICT (code) DO NOTHING",
                lang_data
            )

        # Countries
        if self._reference_data_cache['countries']:
            country_data = [(country, country if country else 'Unknown') for country in self._reference_data_cache['countries']]
            execute_values(
                cur,
                "INSERT INTO countries (code, name) VALUES %s ON CONFLICT (code) DO NOTHING",
                country_data
            )

        # NUTS codes
        if self._reference_data_cache['nuts_codes']:
            nuts_data = [(nuts, nuts if nuts else 'Unknown', len(nuts) if nuts else 0) for nuts in self._reference_data_cache['nuts_codes']]
            execute_values(
                cur,
                "INSERT INTO nuts_codes (code, name, level) VALUES %s ON CONFLICT (code) DO NOTHING",
                nuts_data
            )

        # Currencies
        if self._reference_data_cache['currencies']:
            currency_data = [(currency, currency if currency else 'Unknown') for currency in self._reference_data_cache['currencies']]
            execute_values(
                cur,
                "INSERT INTO currencies (code, name) VALUES %s ON CONFLICT (code) DO NOTHING",
                currency_data
            )

        # CPV codes
        if self._reference_data_cache['cpv_codes']:
            cpv_data = [(cpv, cpv if cpv else 'Unknown') for cpv in self._reference_data_cache['cpv_codes']]
            execute_values(
                cur,
                "INSERT INTO cpv_codes (code, description) VALUES %s ON CONFLICT (code) DO NOTHING",
                cpv_data
            )

        # Clear cache after flush
        for key in self._reference_data_cache:
            self._reference_data_cache[key].clear()

    def save_award_data_batch(self, awards: List[TedAwardDataModel]):
        """Save multiple award data records efficiently in batch."""
        if not awards:
            return

        try:
            with self.conn.cursor() as cur:
                # Collect all reference data first
                for award in awards:
                    self._collect_reference_data(award)

                # Batch insert reference data
                self._flush_reference_data(cur)

                # Process each award
                for award in awards:
                    # Save document
                    self._insert_document(cur, award.document.dict())

                    # Save contracting body
                    cb_id = self._insert_contracting_body(cur, award.document.doc_id, award.contracting_body.dict())

                    # Save contract
                    contract_id = self._insert_contract(cur, award.document.doc_id, cb_id, award.contract.dict())

                    # Save awards
                    for award_data in award.awards:
                        award_id = self._insert_award(cur, contract_id, award_data.dict())

                        for contractor_data in award_data.contractors:
                            contractor_id = self._insert_contractor(cur, contractor_data.dict())
                            self._link_award_contractor(cur, award_id, contractor_id)

            self.conn.commit()
            logger.info(f"Saved batch of {len(awards)} award documents")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error saving award data batch: {e}")
            raise

    def _ensure_schema_exists(self):
        """Ensure database schema exists by running schema.sql if needed."""
        try:
            # Check if main table exists
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'ted_documents'
                    );
                """)

                result = cur.fetchone()
                logger.debug(f"Schema check result: {result}")
                if result and result['exists']:
                    logger.debug("Database schema already exists")
                    return

            # Schema doesn't exist, create it
            logger.info("Creating database schema")
            schema_path = Path(__file__).parent.parent / 'schema.sql'

            if not schema_path.exists():
                raise FileNotFoundError(f"Schema file not found: {schema_path}")

            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            with self.conn.cursor() as cur:
                cur.execute(schema_sql)

            self.conn.commit()
            logger.info("Database schema created successfully")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating database schema: {e}")
            raise