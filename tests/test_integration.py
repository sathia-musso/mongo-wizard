"""
Integration tests for db-wizard
Requires a local MongoDB instance running on localhost:27017
"""

import pytest
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from db_wizard.engines.mongo import MongoEngine
from db_wizard.settings import SettingsManager
import tempfile
from pathlib import Path


# Skip integration tests if MongoDB is not available
try:
    test_client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=1000)
    test_client.admin.command('ping')
    test_client.close()
    MONGODB_AVAILABLE = True
except (ConnectionFailure, Exception):
    MONGODB_AVAILABLE = False


@pytest.mark.skipif(not MONGODB_AVAILABLE, reason="MongoDB not available on localhost:27017")
class TestIntegration:
    """Integration tests with real MongoDB"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test databases"""
        self.client = MongoClient("mongodb://localhost:27017")

        # Clean test databases
        self.client.drop_database("test_source")
        self.client.drop_database("test_target")

        # Create test data
        self.source_db = self.client["test_source"]
        self.target_db = self.client["test_target"]

        # Insert test documents
        self.test_docs = [
            {"_id": i, "name": f"Document {i}", "value": i * 10}
            for i in range(100)
        ]
        self.source_db.test_collection.insert_many(self.test_docs)

        # Create indexes
        self.source_db.test_collection.create_index("name")
        self.source_db.test_collection.create_index([("value", -1)])

        yield

        # Cleanup
        self.client.drop_database("test_source")
        self.client.drop_database("test_target")
        self.client.close()

    def test_copy_collection_with_indexes(self):
        """Test copying a collection with indexes"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db="test_source", source_table="test_collection",
                target_db="test_target", target_table="test_collection",
                drop_target=True,
                force=True,
                force_python=True
            )

            assert result['documents_copied'] == 100
            assert result['indexes_created'] == 2

            target_docs = list(self.target_db.test_collection.find().sort("_id"))
            assert len(target_docs) == 100
            assert target_docs[0]["name"] == "Document 0"

            indexes = list(self.target_db.test_collection.list_indexes())
            index_names = [idx['name'] for idx in indexes]
            assert 'name_1' in index_names
            assert 'value_-1' in index_names

        finally:
            source_engine.close()
            target_engine.close()

    def test_copy_with_verification(self):
        """Test copy with verification"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            target_engine.copy(
                source_engine=source_engine,
                source_db="test_source", source_table="test_collection",
                target_db="test_target", target_table="verified_collection",
                force=True,
                force_python=True
            )

            verification = source_engine.verify_copy(
                "test_source", "test_collection",
                "test_target", "verified_collection",
                target_engine=target_engine,
                sample_size=10
            )

            assert verification['count_match'] is True
            assert verification['source_count'] == 100
            assert verification['target_count'] == 100
            assert verification['index_match'] is True
            assert verification['sample_match'] is True
            assert len(verification['sample_errors']) == 0

        finally:
            source_engine.close()
            target_engine.close()

    def test_backup_before_copy(self):
        """Test creating backup before copy"""
        self.target_db.important.insert_many([
            {"_id": i, "data": f"Important {i}"}
            for i in range(10)
        ])

        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            backup_name = target_engine.backup_before_copy("test_target", "important")
            assert backup_name.startswith("important_backup_")

            backup_coll = self.target_db[backup_name]
            assert backup_coll.count_documents({}) == 10

            target_engine.copy(
                source_engine=source_engine,
                source_db="test_source", source_table="test_collection",
                target_db="test_target", target_table="important",
                drop_target=True,
                force=True,
                force_python=True
            )

            assert self.target_db.important.count_documents({}) == 100
            assert backup_coll.count_documents({}) == 10

        finally:
            source_engine.close()
            target_engine.close()

    def test_copy_multiple_collections(self):
        """Test copying multiple collections"""
        self.source_db.users.insert_many([{"_id": i, "user": f"user{i}"} for i in range(50)])
        self.source_db.products.insert_many([{"_id": i, "product": f"prod{i}"} for i in range(30)])

        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            results = {}
            for coll_name in ["test_collection", "users", "products"]:
                result = target_engine.copy(
                    source_engine=source_engine,
                    source_db="test_source", source_table=coll_name,
                    target_db="test_target", target_table=coll_name,
                    drop_target=True,
                    force=True,
                    force_python=True
                )
                results[coll_name] = result

            assert len(results) == 3
            assert results['test_collection']['documents_copied'] == 100
            assert results['users']['documents_copied'] == 50
            assert results['products']['documents_copied'] == 30

            assert self.target_db.test_collection.count_documents({}) == 100
            assert self.target_db.users.count_documents({}) == 50
            assert self.target_db.products.count_documents({}) == 30

        finally:
            source_engine.close()
            target_engine.close()

    def test_settings_manager_with_temp_file(self):
        """Test SettingsManager with temporary config file"""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            manager = SettingsManager()
            manager.config_file = tmp_path
            manager.settings = {"hosts": {}, "tasks": {}}

            manager.add_host("test", "mongodb://test:27017")
            manager.add_task("test_task", {
                "source_uri": "mongodb://source",
                "target_uri": "mongodb://target",
                "source_db": "test",
                "target_db": "test_backup"
            })

            manager.save_settings()

            new_manager = SettingsManager()
            new_manager.config_file = tmp_path
            new_manager.settings = new_manager.load_settings()

            assert new_manager.get_host("test") == "mongodb://test:27017"
            assert new_manager.get_task("test_task")["source_db"] == "test"

        finally:
            if tmp_path.exists():
                tmp_path.unlink()
