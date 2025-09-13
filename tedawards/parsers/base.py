from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

class BaseParser(ABC):
    """Base class for TED XML parsers."""

    @abstractmethod
    def can_parse(self, xml_file: Path) -> bool:
        """Check if this parser can handle the given XML file."""
        pass

    @abstractmethod
    def parse_xml_file(self, xml_file: Path) -> Optional[Dict]:
        """Parse XML file and return structured data."""
        pass

    @abstractmethod
    def get_format_name(self) -> str:
        """Return human-readable name of the format this parser handles."""
        pass