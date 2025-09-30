"""
Test MongoDB connections and basic operations
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from mongo_wizard.core import MongoAdvancedCopier
from mongo_wizard.utils import check_mongodb_tools
from mongo_wizard.utils import test_connection as verify_connection  # Renamed to avoid pytest confusion
from pymongo.errors import ConnectionFailure


class TestConnection:
    """Test connection functionality"""

    def test_check_mongodb_tools(self):
        """Test that we can check for MongoDB tools"""
        tools = check_mongodb_tools()
        assert isinstance(tools, dict)
        assert 'mongodump' in tools
        assert 'mongorestore' in tools
        assert 'mongo' in tools
        assert 'mongosh' in tools

    @patch('mongo_wizard.utils.MongoClient')
    def test_connection_success(self, mock_client):
        """Test successful connection"""
        # Mock successful connection
        mock_instance = Mock()
        mock_instance.admin.command.return_value = {'ok': 1}
        mock_instance.list_database_names.return_value = ['db1', 'db2', 'db3']
        mock_client.return_value = mock_instance

        success, message = verify_connection("mongodb://localhost:27017")

        assert success is True
        assert "OK" in message
        assert "3 databases" in message
        mock_instance.close.assert_called_once()

    @patch('mongo_wizard.utils.MongoClient')
    def test_connection_failure(self, mock_client):
        """Test connection failure"""
        # Mock connection failure
        mock_client.side_effect = ConnectionFailure("Connection failed")

        success, message = verify_connection("mongodb://invalid:27017")

        assert success is False
        assert "Connection failed" in message

    @patch('mongo_wizard.core.connect_mongo')
    def test_copier_connect_success(self, mock_connect):
        """Test MongoAdvancedCopier connection"""
        # Setup mocks
        mock_source = MagicMock()
        mock_target = MagicMock()
        mock_connect.side_effect = [mock_source, mock_target]

        copier = MongoAdvancedCopier(
            "mongodb://source:27017",
            "mongodb://target:27017"
        )

        result = copier.connect()

        assert result == copier
        assert copier.source_client == mock_source
        assert copier.target_client == mock_target

    @patch('mongo_wizard.core.console')
    @patch('mongo_wizard.core.connect_mongo')
    def test_copier_connect_failure(self, mock_connect, mock_console):
        """Test MongoAdvancedCopier connection failure"""
        mock_connect.side_effect = ConnectionFailure("Cannot connect")

        copier = MongoAdvancedCopier(
            "mongodb://invalid:27017",
            "mongodb://target:27017"
        )

        with pytest.raises(SystemExit):
            copier.connect()

        # Verify error was printed (but suppressed by mock)
        mock_console.print.assert_called_once()

    def test_copier_close(self):
        """Test closing connections"""
        copier = MongoAdvancedCopier(
            "mongodb://source:27017",
            "mongodb://target:27017"
        )

        # Mock clients
        copier.source_client = Mock()
        copier.target_client = Mock()

        copier.close()

        copier.source_client.close.assert_called_once()
        copier.target_client.close.assert_called_once()