"""
Tests for portals/ted/portal.py — TED Europa download and import logic.
"""

import pytest
import tempfile
import tarfile
from pathlib import Path
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from awards.portals.ted import (
    download_package,
    get_package_files,
    get_downloaded_packages,
    get_package_number,
    download_year,
    import_year,
)
from awards.models import Base

TEST_DATABASE_URL = "postgresql://awards:awards@localhost:5433/awards_test"


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_db():
    """Create a PostgreSQL test database with fresh tables for each test."""
    engine = create_engine(TEST_DATABASE_URL, echo=False)
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with (
        patch("awards.db.engine", engine),
        patch("awards.db.SessionLocal", SessionLocal),
    ):
        yield engine

    engine.dispose()


class TestDownloadPackage:
    """Tests for download_package function."""

    def test_existing_files_skipped(self, temp_data_dir):
        """Test that existing files are skipped without download."""
        package_number = 202400001
        extract_dir = temp_data_dir / "202400001"
        extract_dir.mkdir()

        # Create existing XML files
        xml_file = extract_dir / "test.xml"
        xml_file.write_text("<test/>")

        with patch("requests.get") as mock_get:
            result = download_package(package_number, temp_data_dir)

            # Should not make HTTP request
            mock_get.assert_not_called()
            assert result is True

    def test_download_and_extract_tar_gz(self, temp_data_dir):
        """Test downloading and extracting tar.gz archive."""
        package_number = 202400001

        # Create a mock tar.gz archive with actual content
        tar_path = temp_data_dir / "test.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
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

        with patch("requests.get", return_value=mock_response):
            result = download_package(package_number, temp_data_dir)

            assert result is True

            # Verify files were extracted
            files = get_package_files(package_number, temp_data_dir)
            assert len(files) == 1
            assert files[0].name == "test.xml"

            # Archive should be cleaned up
            archive_path = temp_data_dir / "202400001.tar.gz"
            assert not archive_path.exists()

    def test_404_returns_false(self, temp_data_dir):
        """Test that 404 returns False."""
        package_number = 202400001

        import requests

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            response=mock_response
        )

        with patch("requests.get", return_value=mock_response):
            result = download_package(package_number, temp_data_dir)
            assert result is False

    def test_http_error_raises_exception(self, temp_data_dir):
        """Test that non-404 HTTP errors are properly raised."""
        package_number = 202400001

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("500 Server Error")

        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception, match="500 Server Error"):
                download_package(package_number, temp_data_dir)


class TestGetPackageFiles:
    """Tests for get_package_files function."""

    def test_returns_files_from_package(self, temp_data_dir):
        """Test that files are returned from package directory."""
        package_number = 202400001
        extract_dir = temp_data_dir / "202400001"
        extract_dir.mkdir()

        xml_file = extract_dir / "test.xml"
        xml_file.write_text("<test/>")

        files = get_package_files(package_number, temp_data_dir)
        assert len(files) == 1
        assert files[0] == xml_file

    def test_returns_none_if_not_downloaded(self, temp_data_dir):
        """Test that None is returned if package not downloaded."""
        files = get_package_files(202400001, temp_data_dir)
        assert files is None

    def test_returns_none_if_directory_empty(self, temp_data_dir):
        """Test that None is returned if directory exists but is empty."""
        package_number = 202400001
        extract_dir = temp_data_dir / "202400001"
        extract_dir.mkdir()

        files = get_package_files(package_number, temp_data_dir)
        assert files is None

    def test_case_insensitive_file_detection(self, temp_data_dir):
        """Test that uppercase extensions are also detected."""
        package_number = 202400001
        extract_dir = temp_data_dir / "202400001"
        extract_dir.mkdir()

        xml_file = extract_dir / "test.XML"
        zip_file = extract_dir / "test.ZIP"
        xml_file.write_text("<test/>")
        zip_file.write_bytes(b"PK")

        files = get_package_files(package_number, temp_data_dir)
        assert len(files) == 2
        assert xml_file in files
        assert zip_file in files


class TestGetPackageNumber:
    """Tests for get_package_number function."""

    def test_first_issue_of_year(self):
        """Test calculation for first issue of year."""
        assert get_package_number(2024, 1) == 202400001

    def test_last_issue_of_year(self):
        """Test calculation for a high issue number."""
        assert get_package_number(2024, 253) == 202400253

    def test_different_years(self):
        """Test that different years produce different package numbers."""
        assert get_package_number(2023, 100) == 202300100
        assert get_package_number(2024, 100) == 202400100
        assert get_package_number(2023, 100) != get_package_number(2024, 100)


class TestDownloadPackageResumeBehavior:
    """Tests for download_package resume behavior."""

    def test_uses_existing_extracted_data(self, temp_data_dir):
        """Test that existing extracted data is reused without re-download."""
        package_number = 202400001
        extract_dir = temp_data_dir / "202400001"
        extract_dir.mkdir()

        # Create existing XML files
        xml_file = extract_dir / "test.xml"
        xml_file.write_text("<test/>")

        with patch("requests.get") as mock_get:
            result = download_package(package_number, temp_data_dir)

            # Should NOT make HTTP request
            mock_get.assert_not_called()
            assert result is True

    def test_downloads_if_directory_missing(self, temp_data_dir):
        """Test that package is downloaded if directory doesn't exist."""
        package_number = 202400001

        # Create mock tar.gz archive
        tar_path = temp_data_dir / "test.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
            xml_file = temp_data_dir / "temp.xml"
            xml_file.write_text("<test/>")
            tar.add(xml_file, arcname="test.xml")
            xml_file.unlink()

        tar_data = tar_path.read_bytes()
        tar_path.unlink()

        mock_response = Mock()
        mock_response.content = tar_data
        mock_response.raise_for_status = Mock()

        with patch("requests.get", return_value=mock_response):
            result = download_package(package_number, temp_data_dir)

            assert result is True
            files = get_package_files(package_number, temp_data_dir)
            assert len(files) == 1
            assert files[0].name == "test.xml"

    def test_downloads_if_directory_empty(self, temp_data_dir):
        """Test that package is downloaded if directory exists but is empty."""
        package_number = 202400001
        extract_dir = temp_data_dir / "202400001"
        extract_dir.mkdir()
        # Directory exists but has no files

        # Create mock tar.gz archive
        tar_path = temp_data_dir / "test.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
            xml_file = temp_data_dir / "temp.xml"
            xml_file.write_text("<test/>")
            tar.add(xml_file, arcname="test.xml")
            xml_file.unlink()

        tar_data = tar_path.read_bytes()
        tar_path.unlink()

        mock_response = Mock()
        mock_response.content = tar_data
        mock_response.raise_for_status = Mock()

        with patch("requests.get", return_value=mock_response):
            result = download_package(package_number, temp_data_dir)

            # Should download since directory was empty
            assert result is True
            files = get_package_files(package_number, temp_data_dir)
            assert len(files) == 1


class TestDownloadYear:
    """Tests for download_year function."""

    def test_starts_from_issue_1(self, test_db, temp_data_dir):
        """Test that download always starts from issue 1."""
        # test_db is required: download_year calls init_db() and reads
        # processed_packages (get_imported_package_numbers) before fetching.
        requested_issues = []

        def mock_download(package_num, data_dir):
            issue = package_num % 100000
            requested_issues.append(issue)
            return False

        with patch(
            "awards.portals.ted.portal.download_package", side_effect=mock_download
        ):
            download_year(2024, max_issue=20, data_dir=temp_data_dir)

        assert requested_issues[0] == 1


class TestGetDownloadedPackages:
    """Tests for get_downloaded_packages function."""

    def test_no_packages(self, temp_data_dir):
        """Test when no packages exist."""
        packages = get_downloaded_packages(2024, temp_data_dir)
        assert packages == []

    def test_returns_sorted_packages(self, temp_data_dir):
        """Test that packages are returned sorted."""
        (temp_data_dir / "202400010").mkdir()
        (temp_data_dir / "202400005").mkdir()
        (temp_data_dir / "202400015").mkdir()

        packages = get_downloaded_packages(2024, temp_data_dir)
        assert packages == [202400005, 202400010, 202400015]

    def test_filters_by_year(self, temp_data_dir):
        """Test that only packages for requested year are returned."""
        (temp_data_dir / "202300010").mkdir()
        (temp_data_dir / "202400005").mkdir()
        (temp_data_dir / "202500001").mkdir()

        packages = get_downloaded_packages(2024, temp_data_dir)
        assert packages == [202400005]


class TestImportYear:
    """Tests for import_year function."""

    def test_imports_all_downloaded_packages(self, test_db, temp_data_dir):
        """Test that import_year processes all downloaded packages."""
        # Create package directories with files
        for issue in [1, 2, 3]:
            pkg_dir = temp_data_dir / f"20240000{issue}"
            pkg_dir.mkdir()
            (pkg_dir / "test.xml").write_text("<test/>")

        imported_packages = []

        def mock_import(package_num, data_dir, executor=None):
            imported_packages.append(package_num)
            return 0

        with patch("awards.portals.ted.portal.import_package", side_effect=mock_import):
            import_year(2024, temp_data_dir)

        assert imported_packages == [202400001, 202400002, 202400003]
