"""
Reference data management for database operations.
Handles collection and batch insertion of reference data like countries, languages, etc.
"""

import logging
from typing import Dict, List, Set
from psycopg2.extras import execute_values
from .schema import TedAwardDataModel

logger = logging.getLogger(__name__)


class ReferenceDataManager:
    """Manages collection and batch insertion of reference data."""

    def __init__(self):
        self._cache = {
            'languages': set(),
            'countries': set(),
            'nuts_codes': set(),
            'currencies': set(),
            'cpv_codes': set()
        }

    def collect_from_award_data(self, data: TedAwardDataModel):
        """Collect reference data from award data into cache."""
        # Languages
        if data.document.form_language:
            self._cache['languages'].add(data.document.form_language)
        if data.document.original_language:
            self._cache['languages'].add(data.document.original_language)

        # Countries
        if data.document.source_country:
            self._cache['countries'].add(data.document.source_country)
        if data.contracting_body.country_code:
            self._cache['countries'].add(data.contracting_body.country_code)

        for award_data in data.awards:
            for contractor in award_data.contractors:
                if contractor.country_code:
                    self._cache['countries'].add(contractor.country_code)

        # NUTS codes
        if data.contracting_body.nuts_code:
            self._cache['nuts_codes'].add(data.contracting_body.nuts_code)
        if data.contract.performance_nuts_code:
            self._cache['nuts_codes'].add(data.contract.performance_nuts_code)

        for award_data in data.awards:
            for contractor in award_data.contractors:
                if contractor.nuts_code:
                    self._cache['nuts_codes'].add(contractor.nuts_code)

        # Currencies
        if data.contract.total_value_currency:
            self._cache['currencies'].add(data.contract.total_value_currency)

        for award_data in data.awards:
            if award_data.awarded_value_currency:
                self._cache['currencies'].add(award_data.awarded_value_currency)
            if award_data.subcontracted_value_currency:
                self._cache['currencies'].add(award_data.subcontracted_value_currency)

        # CPV codes
        if data.contract.main_cpv_code:
            self._cache['cpv_codes'].add(data.contract.main_cpv_code)

    def flush_to_database(self, cursor):
        """Batch insert all collected reference data."""
        # Languages
        if self._cache['languages']:
            # Remove empty strings to avoid inserting empty language codes
            valid_languages = {lang for lang in self._cache['languages'] if lang}
            if valid_languages:
                lang_data = [(lang, lang) for lang in valid_languages]
                execute_values(
                    cursor,
                    "INSERT INTO languages (code, name) VALUES %s ON CONFLICT (code) DO NOTHING",
                    lang_data
                )

        # Countries
        if self._cache['countries']:
            # Remove empty strings to avoid inserting empty country codes
            valid_countries = {country for country in self._cache['countries'] if country}
            if valid_countries:
                country_data = [(country, country) for country in valid_countries]
                execute_values(
                    cursor,
                    "INSERT INTO countries (code, name) VALUES %s ON CONFLICT (code) DO NOTHING",
                    country_data
                )

        # NUTS codes
        if self._cache['nuts_codes']:
            # Remove empty strings to avoid inserting empty NUTS codes
            valid_nuts = {nuts for nuts in self._cache['nuts_codes'] if nuts}
            if valid_nuts:
                nuts_data = [(nuts, nuts, len(nuts)) for nuts in valid_nuts]
                execute_values(
                    cursor,
                    "INSERT INTO nuts_codes (code, name, level) VALUES %s ON CONFLICT (code) DO NOTHING",
                    nuts_data
                )

        # Currencies
        if self._cache['currencies']:
            # Remove empty strings to avoid inserting empty currency codes
            valid_currencies = {currency for currency in self._cache['currencies'] if currency}
            if valid_currencies:
                currency_data = [(currency, currency) for currency in valid_currencies]
                execute_values(
                    cursor,
                    "INSERT INTO currencies (code, name) VALUES %s ON CONFLICT (code) DO NOTHING",
                    currency_data
                )

        # CPV codes
        if self._cache['cpv_codes']:
            # Remove empty strings to avoid inserting empty CPV codes
            valid_cpvs = {cpv for cpv in self._cache['cpv_codes'] if cpv}
            if valid_cpvs:
                cpv_data = [(cpv, cpv) for cpv in valid_cpvs]
                execute_values(
                    cursor,
                    "INSERT INTO cpv_codes (code, description) VALUES %s ON CONFLICT (code) DO NOTHING",
                    cpv_data
                )

    def clear_cache(self):
        """Clear the reference data cache."""
        for key in self._cache:
            self._cache[key].clear()

    def collect_from_batch(self, awards: List[TedAwardDataModel]):
        """Collect reference data from a batch of award data."""
        for award in awards:
            self.collect_from_award_data(award)