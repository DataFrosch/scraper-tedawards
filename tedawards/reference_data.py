"""
Reference data management for database operations.
Handles collection and batch insertion of reference data like countries, languages, etc.
"""

import logging
from typing import Dict, List, Set

from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .schema import TedAwardDataModel
from .models import Country, Language, Currency, CPVCode, NUTSCode

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

    def flush_to_database(self, session: Session):
        """Batch insert all collected reference data using SQLAlchemy."""
        # Languages
        if self._cache['languages']:
            valid_languages = {lang for lang in self._cache['languages'] if lang}
            if valid_languages:
                for lang in valid_languages:
                    stmt = sqlite_insert(Language).values(code=lang, name=lang)
                    stmt = stmt.on_conflict_do_nothing(index_elements=['code'])
                    session.execute(stmt)

        # Countries
        if self._cache['countries']:
            valid_countries = {country for country in self._cache['countries'] if country}
            if valid_countries:
                for country in valid_countries:
                    stmt = sqlite_insert(Country).values(code=country, name=country)
                    stmt = stmt.on_conflict_do_nothing(index_elements=['code'])
                    session.execute(stmt)

        # NUTS codes
        if self._cache['nuts_codes']:
            valid_nuts = {nuts for nuts in self._cache['nuts_codes'] if nuts}
            if valid_nuts:
                for nuts in valid_nuts:
                    stmt = sqlite_insert(NUTSCode).values(code=nuts, name=nuts, level=len(nuts))
                    stmt = stmt.on_conflict_do_nothing(index_elements=['code'])
                    session.execute(stmt)

        # Currencies
        if self._cache['currencies']:
            valid_currencies = {currency for currency in self._cache['currencies'] if currency}
            if valid_currencies:
                for currency in valid_currencies:
                    stmt = sqlite_insert(Currency).values(code=currency, name=currency)
                    stmt = stmt.on_conflict_do_nothing(index_elements=['code'])
                    session.execute(stmt)

        # CPV codes
        if self._cache['cpv_codes']:
            valid_cpvs = {cpv for cpv in self._cache['cpv_codes'] if cpv}
            if valid_cpvs:
                for cpv in valid_cpvs:
                    stmt = sqlite_insert(CPVCode).values(code=cpv, description=cpv)
                    stmt = stmt.on_conflict_do_nothing(index_elements=['code'])
                    session.execute(stmt)

        session.flush()

    def clear_cache(self):
        """Clear the reference data cache."""
        for key in self._cache:
            self._cache[key].clear()

    def collect_from_batch(self, awards: List[TedAwardDataModel]):
        """Collect reference data from a batch of award data."""
        for award in awards:
            self.collect_from_award_data(award)
