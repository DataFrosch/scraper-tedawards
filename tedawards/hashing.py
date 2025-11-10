"""
Generic entity hashing utilities for deduplication.

Provides deterministic hash generation from entity key fields to handle
messy/incomplete data without relying on multi-column unique constraints.
"""

import hashlib
from typing import List, Any, Optional


def generate_entity_hash(obj: Any, key_fields: List[str]) -> str:
    """Generate deterministic hash from specified fields.

    Args:
        obj: Object with attributes matching key_fields
        key_fields: List of field names to include in hash

    Returns:
        16-character hex string (SHA256 truncated)

    Notes:
        - Excludes None values (handles missing data gracefully)
        - Normalizes strings (uppercase, strip whitespace)
        - Deterministic ordering (sorted field names)
        - Uses '|' as field separator
    """
    parts = []

    # Sort fields for deterministic ordering
    for field in sorted(key_fields):
        value = getattr(obj, field, None)

        # Skip None values - missing data doesn't break hashing
        if value is None:
            continue

        # Normalize strings for consistency
        if isinstance(value, str):
            normalized = value.upper().strip()
            if normalized:  # Skip empty strings
                parts.append(normalized)
        else:
            # Handle other types (dates, numbers, etc)
            parts.append(str(value))

    # Generate hash from concatenated parts
    hash_input = '|'.join(parts)

    # Use SHA256 and truncate to 16 chars (64 bits)
    # Collision probability: ~1 in 10^19 for reasonable dataset sizes
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()[:16]


class HashableMixin:
    """Mixin for Pydantic models that need deterministic hashing.

    Subclasses must define HASH_KEY_FIELDS as a ClassVar.
    """

    # Subclasses must override this (use ClassVar in Pydantic models)
    HASH_KEY_FIELDS: List[str] = []  # type: ignore

    def compute_hash(self) -> str:
        """Generate hash from key fields defined in HASH_KEY_FIELDS."""
        if not self.HASH_KEY_FIELDS:
            raise NotImplementedError(
                f"{self.__class__.__name__} must define HASH_KEY_FIELDS"
            )
        return generate_entity_hash(self, self.HASH_KEY_FIELDS)
