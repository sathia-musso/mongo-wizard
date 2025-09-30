"""
Tests for formatting utilities
"""

import pytest
from mongo_wizard.formatting import format_number, format_size, format_docs


class TestFormatNumber:
    """Test number formatting with underscore separators"""

    def test_small_numbers(self):
        """Test numbers under 1000 remain unchanged"""
        assert format_number(0) == "0"
        assert format_number(42) == "42"
        assert format_number(999) == "999"

    def test_thousands(self):
        """Test thousands get underscore separators"""
        assert format_number(1000) == "1_000"
        assert format_number(1234) == "1_234"
        assert format_number(999999) == "999_999"

    def test_millions(self):
        """Test millions get proper separators"""
        assert format_number(1000000) == "1_000_000"
        assert format_number(1234567) == "1_234_567"
        assert format_number(999999999) == "999_999_999"


class TestFormatSize:
    """Test byte size formatting"""

    def test_bytes(self):
        """Test byte formatting"""
        assert format_size(0) == "0.0 B"
        assert format_size(512) == "512 B"  # No decimal for whole numbers >= 100
        assert format_size(1023) == "1_023.0 B"

    def test_kilobytes(self):
        """Test KB formatting"""
        assert format_size(1024) == "1.0 KB"
        assert format_size(1536) == "1.5 KB"
        assert format_size(10240) == "10.0 KB"
        assert format_size(1048576 - 1) == "1_024.0 KB"

    def test_megabytes(self):
        """Test MB formatting"""
        assert format_size(1048576) == "1.0 MB"
        assert format_size(1572864) == "1.5 MB"
        assert format_size(15728640) == "15.0 MB"
        assert format_size(1073741824 - 1) == "1_024.0 MB"

    def test_gigabytes(self):
        """Test GB formatting"""
        assert format_size(1073741824) == "1.0 GB"
        assert format_size(1610612736) == "1.5 GB"
        assert format_size(10737418240) == "10.0 GB"

    def test_terabytes(self):
        """Test TB formatting"""
        assert format_size(1099511627776) == "1.0 TB"
        assert format_size(1649267441664) == "1.5 TB"


class TestFormatDocs:
    """Test document count formatting"""

    def test_small_counts(self):
        """Test counts under 1000 remain unchanged"""
        assert format_docs(0) == "0"
        assert format_docs(42) == "42"
        assert format_docs(999) == "999"

    def test_thousands(self):
        """Test thousands get K suffix"""
        assert format_docs(1000) == "1.0K"
        assert format_docs(1500) == "1.5K"
        assert format_docs(12345) == "12.3K"
        assert format_docs(999999) == "1000.0K"  # format_docs doesn't add underscore until >= 1000

    def test_millions(self):
        """Test millions get M suffix"""
        assert format_docs(1000000) == "1.0M"
        assert format_docs(1500000) == "1.5M"
        assert format_docs(1234567) == "1.2M"
        assert format_docs(999999999) == "1000.0M"  # format_docs doesn't add underscore until >= 1000