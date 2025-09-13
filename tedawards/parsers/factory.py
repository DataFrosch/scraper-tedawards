from pathlib import Path
from typing import List, Optional
from .base import BaseParser
from .ted_r209 import TedXmlParser
from .eforms_ubl import EFormsUBLParser

class ParserFactory:
    """Factory for creating appropriate parsers for different XML formats."""

    def __init__(self):
        self.parsers: List[BaseParser] = [
            TedXmlParser(),
            EFormsUBLParser(),
        ]

    def get_parser(self, xml_file: Path) -> Optional[BaseParser]:
        """Get the appropriate parser for the given XML file."""
        for parser in self.parsers:
            if parser.can_parse(xml_file):
                return parser
        return None

    def get_supported_formats(self) -> List[str]:
        """Get list of supported format names."""
        return [parser.get_format_name() for parser in self.parsers]