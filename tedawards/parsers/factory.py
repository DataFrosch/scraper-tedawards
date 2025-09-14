from pathlib import Path
from typing import List, Optional
from .base import BaseParser
from .ted_v2 import TedV2Parser
from .eforms_ubl import EFormsUBLParser
from .ted_meta_xml import TedMetaXmlParser

class ParserFactory:
    """Factory for creating appropriate parsers for different formats."""

    def __init__(self):
        self.parsers: List[BaseParser] = [
            TedMetaXmlParser(),  # Try META XML format first (for legacy 2007-2013 data)
            TedV2Parser(),       # Unified TED 2.0 parser (R2.0.7, R2.0.8, R2.0.9)
            EFormsUBLParser(),   # eForms UBL (2024+)
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