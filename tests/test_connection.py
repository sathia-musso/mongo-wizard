"""
Test database engine connections and basic operations
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from db_wizard.engines.mongo import MongoEngine
from db_wizard.engine import EngineFactory
from pymongo.errors import ConnectionFailure


class TestConnection:
    """Test connection functionality"""

    def test_check_mongodb_tools(self):
        """Test that we can check for MongoDB tools"""
        engine = MongoEngine("mongodb://localhost:27017")
        tools = engine.check_tools()
        assert isinstance(tools, dict)
        assert 'mongodump' in tools
        assert 'mongorestore' in tools
        assert 'mongosh' in tools

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_connection_success(self, mock_client):
        """Test successful connection"""
        mock_instance = Mock()
        mock_instance.admin.command.return_value = {'ok': 1}
        mock_instance.list_database_names.return_value = ['db1', 'db2', 'db3']
        mock_client.return_value = mock_instance

        engine = MongoEngine("mongodb://localhost:27017")
        success, message = engine.test_connection()

        assert success is True
        assert "OK" in message
        assert "3 databases" in message
        mock_instance.close.assert_called_once()

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_connection_failure(self, mock_client):
        """Test connection failure"""
        mock_client.side_effect = ConnectionFailure("Connection failed")

        engine = MongoEngine("mongodb://invalid:27017")
        success, message = engine.test_connection()

        assert success is False
        assert "Connection failed" in message

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_engine_connect_success(self, mock_client):
        """Test MongoEngine.connect()"""
        mock_instance = MagicMock()
        mock_instance.admin.command.return_value = {'ok': 1}
        mock_client.return_value = mock_instance

        engine = MongoEngine("mongodb://source:27017")
        result = engine.connect()

        assert result == engine
        assert engine.client == mock_instance

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_engine_connect_failure(self, mock_client):
        """Test MongoEngine.connect() failure raises exception"""
        mock_instance = MagicMock()
        mock_instance.admin.command.side_effect = ConnectionFailure("Cannot connect")
        mock_client.return_value = mock_instance

        engine = MongoEngine("mongodb://invalid:27017")

        with pytest.raises(ConnectionFailure):
            engine.connect()

    def test_engine_close(self):
        """Test closing connections"""
        engine = MongoEngine("mongodb://source:27017")
        mock_client = Mock()
        engine.client = mock_client

        engine.close()

        mock_client.close.assert_called_once()
        assert engine.client is None

    def test_engine_close_no_client(self):
        """Test closing when no client is connected"""
        engine = MongoEngine("mongodb://source:27017")
        engine.close()

    def test_engine_factory_mongo(self):
        """Test EngineFactory creates MongoEngine for mongodb:// URI"""
        engine = EngineFactory.create("mongodb://localhost:27017")
        assert isinstance(engine, MongoEngine)

    def test_engine_factory_unknown_scheme(self):
        """Test EngineFactory raises for unknown scheme"""
        with pytest.raises(ValueError, match="Unsupported"):
            EngineFactory.create("postgres://localhost:5432")
