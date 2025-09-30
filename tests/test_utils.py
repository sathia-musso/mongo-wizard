"""
Test utility functions
"""

import pytest
from mongo_wizard.utils import (
    parse_collection_selection,
    format_document_count,
    build_connection_uri
)


class TestUtils:
    """Test utility functions"""

    def test_parse_collection_selection_single(self):
        """Test parsing single collection selection"""
        result = parse_collection_selection("3", 10)
        assert result == [2]  # 0-based index

    def test_parse_collection_selection_multiple(self):
        """Test parsing multiple collection selection"""
        result = parse_collection_selection("1,3,5", 10)
        assert result == [0, 2, 4]

    def test_parse_collection_selection_range(self):
        """Test parsing range selection"""
        result = parse_collection_selection("2-5", 10)
        assert result == [1, 2, 3, 4]

    def test_parse_collection_selection_mixed(self):
        """Test parsing mixed selection"""
        result = parse_collection_selection("1,3-5,7", 10)
        assert result == [0, 2, 3, 4, 6]

    def test_parse_collection_selection_all(self):
        """Test parsing ALL selection"""
        result = parse_collection_selection("ALL", 5)
        assert result == [0, 1, 2, 3, 4]

    def test_parse_collection_selection_invalid(self):
        """Test invalid selections are ignored"""
        result = parse_collection_selection("1,invalid,3", 10)
        assert result == [0, 2]

    def test_parse_collection_selection_out_of_range(self):
        """Test out of range values are ignored"""
        result = parse_collection_selection("1,15,3", 5)
        assert result == [0, 2]  # 15 is out of range for max_value=5

    def test_parse_collection_selection_duplicates(self):
        """Test duplicates are removed"""
        result = parse_collection_selection("1,1,2,2,3", 10)
        assert result == [0, 1, 2]

    def test_format_document_count_small(self):
        """Test formatting small document counts"""
        assert format_document_count(42) == "42"
        assert format_document_count(999) == "999"

    def test_format_document_count_thousands(self):
        """Test formatting thousands"""
        assert format_document_count(1500) == "1.5K"
        assert format_document_count(15000) == "15.0K"
        assert format_document_count(999999) == "1000.0K"

    def test_format_document_count_millions(self):
        """Test formatting millions"""
        assert format_document_count(1500000) == "1.5M"
        assert format_document_count(15000000) == "15.0M"

    def test_build_connection_uri_simple(self):
        """Test building simple connection URI"""
        uri = build_connection_uri("localhost")
        assert uri == "mongodb://localhost:27017/"

    def test_build_connection_uri_with_port(self):
        """Test building URI with custom port"""
        uri = build_connection_uri("localhost", port=27018)
        assert uri == "mongodb://localhost:27018/"

    def test_build_connection_uri_with_auth(self):
        """Test building URI with authentication"""
        uri = build_connection_uri(
            "localhost",
            username="admin",
            password="secret"
        )
        assert uri == "mongodb://admin:secret@localhost:27017/"

    def test_build_connection_uri_with_auth_db(self):
        """Test building URI with auth database"""
        uri = build_connection_uri(
            "localhost",
            username="admin",
            password="secret",
            auth_db="admin"
        )
        assert uri == "mongodb://admin:secret@localhost:27017/admin"

    def test_build_connection_uri_complete(self):
        """Test building complete URI"""
        uri = build_connection_uri(
            "prod.server.com",
            port=27020,
            username="user",
            password="pass",
            auth_db="authdb"
        )
        assert uri == "mongodb://user:pass@prod.server.com:27020/authdb"