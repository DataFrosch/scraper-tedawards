"""
Tests for scraper.py logic.
"""

import pytest
import tempfile
import tarfile
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from tedawards.scraper import (
    get_day_number,
    download_and_extract,
    process_file,
    save_awards,
    scrape_date,
    get_session
)
from tedawards.models import (
    Base, TEDDocument, ContractingBody, Contract, Award, Contractor
)
from tedawards.schema import (
    TedAwardDataModel, TedParserResultModel, DocumentModel,
    ContractingBodyModel, ContractModel, AwardModel, ContractorModel
)


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_db():
    """Create a temporary in-memory database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    # Patch the module-level engine and SessionLocal
    with patch('tedawards.scraper.engine', engine), \
         patch('tedawards.scraper.SessionLocal', SessionLocal):
        yield engine


@pytest.fixture
def sample_award_data():
    """Create sample award data for testing."""
    return TedAwardDataModel(
        document=DocumentModel(
            doc_id="12345-2024",
            edition="2024/S 001-000001",
            publication_date=date(2024, 1, 1),
            source_country="DE"
        ),
        contracting_body=ContractingBodyModel(
            official_name="Test Contracting Body",
            town="Berlin",
            country_code="DE"
        ),
        contract=ContractModel(
            title="Test Contract",
            reference_number="REF-2024-001",
            main_cpv_code="45000000",
            total_value=100000.0,
            total_value_currency="EUR"
        ),
        awards=[
            AwardModel(
                award_title="Award 1",
                conclusion_date=date(2024, 1, 15),
                awarded_value=50000.0,
                awarded_value_currency="EUR",
                tenders_received=5,
                contractors=[
                    ContractorModel(
                        official_name="Test Contractor GmbH",
                        town="Munich",
                        country_code="DE",
                        is_sme=True
                    )
                ]
            )
        ]
    )


class TestGetDayNumber:
    """Tests for get_day_number function."""

    def test_first_day_of_year(self):
        """Test calculation for first day of year."""
        target = date(2024, 1, 1)
        expected = 2024 * 100000 + 1  # 202400001
        assert get_day_number(target) == expected

    def test_last_day_of_leap_year(self):
        """Test calculation for last day of leap year."""
        target = date(2024, 12, 31)
        expected = 2024 * 100000 + 366  # 202400366
        assert get_day_number(target) == expected

    def test_last_day_of_regular_year(self):
        """Test calculation for last day of regular year."""
        target = date(2023, 12, 31)
        expected = 2023 * 100000 + 365  # 202300365
        assert get_day_number(target) == expected

    def test_mid_year_date(self):
        """Test calculation for mid-year date."""
        target = date(2024, 6, 15)
        # Jan (31) + Feb (29 in leap year) + Mar (31) + Apr (30) + May (31) + 15 = 167
        expected = 2024 * 100000 + 167
        assert get_day_number(target) == expected

    def test_different_years(self):
        """Test that different years produce different day numbers."""
        date_2023 = date(2023, 1, 1)
        date_2024 = date(2024, 1, 1)
        assert get_day_number(date_2023) != get_day_number(date_2024)


class TestDownloadAndExtract:
    """Tests for download_and_extract function."""

    def test_existing_xml_files_reused(self, temp_data_dir):
        """Test that existing XML files are reused without download."""
        target_date = date(2024, 1, 1)
        extract_dir = temp_data_dir / "2024-01-01"
        extract_dir.mkdir()

        # Create existing XML files
        xml_file = extract_dir / "test.xml"
        xml_file.write_text("<test/>")

        with patch('requests.get') as mock_get:
            files = download_and_extract(
                "https://example.com/package",
                target_date,
                temp_data_dir
            )

            # Should not make HTTP request
            mock_get.assert_not_called()
            assert len(files) == 1
            assert files[0] == xml_file

    def test_existing_zip_files_reused(self, temp_data_dir):
        """Test that existing ZIP files are reused without download."""
        target_date = date(2024, 1, 1)
        extract_dir = temp_data_dir / "2024-01-01"
        extract_dir.mkdir()

        # Create existing ZIP file
        zip_file = extract_dir / "test.zip"
        zip_file.write_bytes(b"PK")  # ZIP magic bytes

        with patch('requests.get') as mock_get:
            files = download_and_extract(
                "https://example.com/package",
                target_date,
                temp_data_dir
            )

            mock_get.assert_not_called()
            assert len(files) == 1
            assert files[0] == zip_file

    def test_case_insensitive_file_detection(self, temp_data_dir):
        """Test that uppercase extensions are also detected."""
        target_date = date(2024, 1, 1)
        extract_dir = temp_data_dir / "2024-01-01"
        extract_dir.mkdir()

        # Create files with uppercase extensions
        xml_file = extract_dir / "test.XML"
        zip_file = extract_dir / "test.ZIP"
        xml_file.write_text("<test/>")
        zip_file.write_bytes(b"PK")

        with patch('requests.get') as mock_get:
            files = download_and_extract(
                "https://example.com/package",
                target_date,
                temp_data_dir
            )

            mock_get.assert_not_called()
            assert len(files) == 2
            assert xml_file in files
            assert zip_file in files

    def test_download_and_extract_tar_gz(self, temp_data_dir):
        """Test downloading and extracting tar.gz archive."""
        target_date = date(2024, 1, 1)

        # Create a mock tar.gz archive with actual content
        tar_path = temp_data_dir / "test.tar.gz"
        with tarfile.open(tar_path, 'w:gz') as tar:
            # Create a temporary XML file to add
            xml_file = temp_data_dir / "temp_test.xml"
            xml_file.write_text("<test/>")
            tar.add(xml_file, arcname="test.xml")
            xml_file.unlink()

        tar_data = tar_path.read_bytes()
        tar_path.unlink()

        # Mock HTTP response
        mock_response = Mock()
        mock_response.content = tar_data
        mock_response.raise_for_status = Mock()

        with patch('requests.get', return_value=mock_response):
            files = download_and_extract(
                "https://example.com/package",
                target_date,
                temp_data_dir
            )

            # Should extract XML file from archive
            assert len(files) == 1
            assert files[0].name == "test.xml"

            # Archive should be cleaned up
            archive_path = temp_data_dir / "2024-01-01.tar.gz"
            assert not archive_path.exists()

    def test_http_error_raises_exception(self, temp_data_dir):
        """Test that HTTP errors are properly raised."""
        target_date = date(2024, 1, 1)

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(Exception, match="404 Not Found"):
                download_and_extract(
                    "https://example.com/package",
                    target_date,
                    temp_data_dir
                )


class TestProcessFile:
    """Tests for process_file function."""

    def test_process_file_with_valid_parser(self, temp_data_dir, sample_award_data):
        """Test processing file with valid parser."""
        xml_file = temp_data_dir / "test.xml"
        xml_file.write_text("<test/>")

        # Mock parser
        mock_parser = Mock()
        mock_parser.parse_xml_file.return_value = TedParserResultModel(
            awards=[sample_award_data]
        )
        mock_parser.get_format_name.return_value = "Test Parser"

        # Mock parser factory
        with patch('tedawards.scraper.parser_factory.get_parser', return_value=mock_parser):
            result = process_file(xml_file)

            assert result is not None
            assert len(result.awards) == 1
            assert result.awards[0].document.doc_id == "12345-2024"
            mock_parser.parse_xml_file.assert_called_once_with(xml_file)

    def test_process_file_no_parser_available(self, temp_data_dir):
        """Test processing file when no parser is available."""
        xml_file = temp_data_dir / "test.xml"
        xml_file.write_text("<test/>")

        with patch('tedawards.scraper.parser_factory.get_parser', return_value=None):
            result = process_file(xml_file)
            assert result is None

    def test_process_file_parser_returns_none(self, temp_data_dir):
        """Test processing file when parser returns None."""
        xml_file = temp_data_dir / "test.xml"
        xml_file.write_text("<test/>")

        mock_parser = Mock()
        mock_parser.parse_xml_file.return_value = None
        mock_parser.get_format_name.return_value = "Test Parser"

        with patch('tedawards.scraper.parser_factory.get_parser', return_value=mock_parser):
            result = process_file(xml_file)
            assert result is None

    def test_process_file_parser_raises_exception(self, temp_data_dir):
        """Test that parser exceptions are propagated."""
        xml_file = temp_data_dir / "test.xml"
        xml_file.write_text("<test/>")

        mock_parser = Mock()
        mock_parser.parse_xml_file.side_effect = ValueError("Invalid XML")

        with patch('tedawards.scraper.parser_factory.get_parser', return_value=mock_parser):
            with pytest.raises(ValueError, match="Invalid XML"):
                process_file(xml_file)


class TestSaveAwards:
    """Tests for save_awards function."""

    def test_save_single_award(self, test_db, sample_award_data):
        """Test saving a single award to database."""
        from tedawards.scraper import SessionLocal

        session = SessionLocal()
        try:
            count = save_awards(session, [sample_award_data])
            session.commit()

            assert count == 1

            # Verify document was saved
            doc = session.execute(
                select(TEDDocument).where(TEDDocument.doc_id == "12345-2024")
            ).scalar_one()
            assert doc.edition == "2024/S 001-000001"

            # Verify contracting body was saved
            cb = session.execute(
                select(ContractingBody).where(ContractingBody.ted_doc_id == "12345-2024")
            ).scalar_one()
            assert cb.official_name == "Test Contracting Body"

            # Verify contract was saved
            contract = session.execute(
                select(Contract).where(Contract.ted_doc_id == "12345-2024")
            ).scalar_one()
            assert contract.title == "Test Contract"
            assert contract.total_value == Decimal("100000.00")

            # Verify award was saved
            award = session.execute(
                select(Award).where(Award.contract_id == contract.id)
            ).scalar_one()
            assert award.awarded_value == Decimal("50000.00")
            assert award.tenders_received == 5

            # Verify contractor was saved
            contractor = session.execute(
                select(Contractor).where(Contractor.official_name == "Test Contractor GmbH")
            ).scalar_one()
            assert contractor.country_code == "DE"
            assert contractor.is_sme == True  # SQLite stores bool as 0/1

            # Verify relationship
            assert contractor in award.contractors

        finally:
            session.close()

    def test_save_duplicate_document_ignored(self, test_db, sample_award_data):
        """Test that duplicate documents are handled via INSERT OR IGNORE."""
        from tedawards.scraper import SessionLocal

        session = SessionLocal()
        try:
            # Save first time
            count1 = save_awards(session, [sample_award_data])
            session.commit()
            assert count1 == 1

            # Save again with same doc_id
            count2 = save_awards(session, [sample_award_data])
            session.commit()
            assert count2 == 1

            # Verify only one document exists
            docs = session.execute(
                select(TEDDocument).where(TEDDocument.doc_id == "12345-2024")
            ).all()
            assert len(docs) == 1

        finally:
            session.close()

    def test_save_duplicate_contractor_deduplicated(self, test_db):
        """Test that duplicate contractors are deduplicated by name+country."""
        from tedawards.scraper import SessionLocal

        # Create two awards with same contractor
        award_data_1 = TedAwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                edition="2024/S 001-000001",
                publication_date=date(2024, 1, 1),
                source_country="DE"
            ),
            contracting_body=ContractingBodyModel(
                official_name="Test Body 1",
                country_code="DE"
            ),
            contract=ContractModel(
                title="Contract 1",
                main_cpv_code="45000000"
            ),
            awards=[
                AwardModel(
                    contractors=[
                        ContractorModel(
                            official_name="Shared Contractor Ltd",
                            country_code="GB"
                        )
                    ]
                )
            ]
        )

        award_data_2 = TedAwardDataModel(
            document=DocumentModel(
                doc_id="67890-2024",
                edition="2024/S 001-000002",
                publication_date=date(2024, 1, 2),
                source_country="FR"
            ),
            contracting_body=ContractingBodyModel(
                official_name="Test Body 2",
                country_code="FR"
            ),
            contract=ContractModel(
                title="Contract 2",
                main_cpv_code="45000000"
            ),
            awards=[
                AwardModel(
                    contractors=[
                        ContractorModel(
                            official_name="Shared Contractor Ltd",
                            country_code="GB"
                        )
                    ]
                )
            ]
        )

        session = SessionLocal()
        try:
            save_awards(session, [award_data_1, award_data_2])
            session.commit()

            # Verify only one contractor exists
            contractors = session.execute(
                select(Contractor).where(Contractor.official_name == "Shared Contractor Ltd")
            ).all()
            assert len(contractors) == 1

        finally:
            session.close()

    def test_save_multiple_awards_same_contract(self, test_db):
        """Test saving multiple awards for same contract."""
        from tedawards.scraper import SessionLocal

        award_data = TedAwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                edition="2024/S 001-000001",
                publication_date=date(2024, 1, 1),
                source_country="DE"
            ),
            contracting_body=ContractingBodyModel(
                official_name="Test Body",
                country_code="DE"
            ),
            contract=ContractModel(
                title="Multi-lot Contract",
                main_cpv_code="45000000"
            ),
            awards=[
                AwardModel(
                    award_title="Lot 1",
                    awarded_value=10000.0,
                    awarded_value_currency="EUR",
                    contractors=[
                        ContractorModel(
                            official_name="Contractor A",
                            country_code="DE"
                        )
                    ]
                ),
                AwardModel(
                    award_title="Lot 2",
                    awarded_value=20000.0,
                    awarded_value_currency="EUR",
                    contractors=[
                        ContractorModel(
                            official_name="Contractor B",
                            country_code="FR"
                        )
                    ]
                )
            ]
        )

        session = SessionLocal()
        try:
            count = save_awards(session, [award_data])
            session.commit()
            assert count == 1

            # Verify both awards were saved
            awards = session.execute(select(Award)).all()
            assert len(awards) == 2

            # Verify different contractors
            contractors = session.execute(select(Contractor)).all()
            assert len(contractors) == 2

        finally:
            session.close()

    def test_save_award_validation_error_propagates(self, test_db):
        """Test that validation errors are propagated."""
        from tedawards.scraper import SessionLocal
        from unittest.mock import patch

        # Create valid award data but force an exception during save
        valid_data = TedAwardDataModel(
            document=DocumentModel(
                doc_id="12345-2024",
                edition="2024/S 001-000001",
                publication_date=date(2024, 1, 1),
                source_country="DE"
            ),
            contracting_body=ContractingBodyModel(
                official_name="Test Body"
            ),
            contract=ContractModel(
                title="Test Contract"
            ),
            awards=[AwardModel()]
        )

        session = SessionLocal()
        try:
            # Mock session.execute to raise an exception
            with patch.object(session, 'execute', side_effect=Exception("Database error")):
                with pytest.raises(Exception, match="Database error"):
                    save_awards(session, [valid_data])
        finally:
            session.close()


class TestScrapeDate:
    """Integration tests for scrape_date function."""

    def test_scrape_date_no_files(self, test_db, temp_data_dir):
        """Test scraping when no files are found."""
        target_date = date(2024, 1, 1)

        with patch('tedawards.scraper.download_and_extract', return_value=[]):
            scrape_date(target_date, temp_data_dir)
            # Should complete without error

    def test_scrape_date_no_awards_found(self, test_db, temp_data_dir):
        """Test scraping when files exist but no awards found."""
        target_date = date(2024, 1, 1)
        xml_file = temp_data_dir / "test.xml"

        with patch('tedawards.scraper.download_and_extract', return_value=[xml_file]), \
             patch('tedawards.scraper.process_file', return_value=None):
            scrape_date(target_date, temp_data_dir)
            # Should complete without error

    def test_scrape_date_with_awards(self, test_db, temp_data_dir, sample_award_data):
        """Test successful scraping with awards."""
        target_date = date(2024, 1, 1)
        xml_file = temp_data_dir / "test.xml"

        parser_result = TedParserResultModel(awards=[sample_award_data])

        with patch('tedawards.scraper.download_and_extract', return_value=[xml_file]), \
             patch('tedawards.scraper.process_file', return_value=parser_result):
            scrape_date(target_date, temp_data_dir)

            # Verify data was saved
            from tedawards.scraper import SessionLocal
            session = SessionLocal()
            try:
                doc = session.execute(
                    select(TEDDocument).where(TEDDocument.doc_id == "12345-2024")
                ).scalar_one_or_none()
                assert doc is not None
            finally:
                session.close()

    def test_scrape_date_calculates_correct_url(self, test_db, temp_data_dir):
        """Test that scrape_date calculates correct package URL."""
        target_date = date(2024, 1, 15)
        expected_day_number = 202400015
        expected_url = f"https://ted.europa.eu/packages/daily/{expected_day_number:09d}"

        with patch('tedawards.scraper.download_and_extract', return_value=[]) as mock_download:
            scrape_date(target_date, temp_data_dir)

            # Verify correct URL was passed
            mock_download.assert_called_once_with(
                expected_url,
                target_date,
                temp_data_dir
            )


class TestGetSession:
    """Tests for get_session context manager."""

    def test_session_commits_on_success(self, test_db):
        """Test that session commits when no exception occurs."""
        from tedawards.scraper import get_session

        with get_session() as session:
            doc = TEDDocument(
                doc_id="test-doc",
                edition="2024/S 001-000001",
                publication_date=date(2024, 1, 1)
            )
            session.add(doc)

        # Verify document was committed
        from tedawards.scraper import SessionLocal
        verify_session = SessionLocal()
        try:
            result = verify_session.execute(
                select(TEDDocument).where(TEDDocument.doc_id == "test-doc")
            ).scalar_one_or_none()
            assert result is not None
        finally:
            verify_session.close()

    def test_session_rolls_back_on_exception(self, test_db):
        """Test that session rolls back when exception occurs."""
        from tedawards.scraper import get_session

        with pytest.raises(ValueError):
            with get_session() as session:
                doc = TEDDocument(
                    doc_id="test-doc",
                    edition="2024/S 001-000001",
                    publication_date=date(2024, 1, 1)
                )
                session.add(doc)
                raise ValueError("Test error")

        # Verify document was NOT committed
        from tedawards.scraper import SessionLocal
        verify_session = SessionLocal()
        try:
            result = verify_session.execute(
                select(TEDDocument).where(TEDDocument.doc_id == "test-doc")
            ).scalar_one_or_none()
            assert result is None
        finally:
            verify_session.close()
