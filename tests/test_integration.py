"""
Integration tests for MongoDB Wizard
Requires a local MongoDB instance running on localhost:27017
"""

import pytest
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from mongo_wizard.core import MongoAdvancedCopier
from mongo_wizard.settings import SettingsManager
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
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            result = copier.copy_collection_with_indexes(
                "test_source", "test_collection",
                "test_target", "test_collection",
                drop_target=True,
                force=True,
                force_python=True  # Force Python for predictable testing
            )

            # Check results
            assert result['documents_copied'] == 100
            assert result['indexes_created'] == 2  # name and value indexes (not _id)

            # Verify data
            target_docs = list(self.target_db.test_collection.find().sort("_id"))
            assert len(target_docs) == 100
            assert target_docs[0]["name"] == "Document 0"

            # Verify indexes
            indexes = list(self.target_db.test_collection.list_indexes())
            index_names = [idx['name'] for idx in indexes]
            assert 'name_1' in index_names
            assert 'value_-1' in index_names

        finally:
            copier.close()

    def test_copy_with_verification(self):
        """Test copy with verification"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            # Copy
            copier.copy_collection_with_indexes(
                "test_source", "test_collection",
                "test_target", "verified_collection",
                force=True,
                force_python=True  # Force Python for predictable testing
            )

            # Verify
            verification = copier.verify_copy(
                "test_source", "test_collection",
                "test_target", "verified_collection",
                sample_size=10
            )

            assert verification['count_match'] is True
            assert verification['source_count'] == 100
            assert verification['target_count'] == 100
            assert verification['index_match'] is True
            assert verification['sample_match'] is True
            assert len(verification['sample_errors']) == 0

        finally:
            copier.close()

    def test_backup_before_copy(self):
        """Test creating backup before copy"""
        # Create initial target data
        self.target_db.important.insert_many([
            {"_id": i, "data": f"Important {i}"}
            for i in range(10)
        ])

        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            # Create backup
            backup_name = copier.backup_before_copy("test_target", "important")
            assert backup_name.startswith("important_backup_")

            # Verify backup exists
            backup_coll = self.target_db[backup_name]
            assert backup_coll.count_documents({}) == 10

            # Now copy new data
            copier.copy_collection_with_indexes(
                "test_source", "test_collection",
                "test_target", "important",
                drop_target=True,
                force=True,
                force_python=True  # Force Python for predictable testing
            )

            # Original collection should have new data
            assert self.target_db.important.count_documents({}) == 100

            # Backup should still have old data
            assert backup_coll.count_documents({}) == 10

        finally:
            copier.close()

    def test_copy_multiple_collections(self):
        """Test copying multiple collections"""
        # Create more collections
        self.source_db.users.insert_many([{"_id": i, "user": f"user{i}"} for i in range(50)])
        self.source_db.products.insert_many([{"_id": i, "product": f"prod{i}"} for i in range(30)])

        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            results = copier.copy_multiple_collections(
                "test_source", "test_target",
                ["test_collection", "users", "products"],
                drop_target=True,
                force=True,
                force_python=True  # Force Python for predictable testing
            )

            assert len(results) == 3
            assert results['test_collection']['documents_copied'] == 100
            assert results['users']['documents_copied'] == 50
            assert results['products']['documents_copied'] == 30

            # Verify in target
            assert self.target_db.test_collection.count_documents({}) == 100
            assert self.target_db.users.count_documents({}) == 50
            assert self.target_db.products.count_documents({}) == 30

        finally:
            copier.close()

    def test_settings_manager_with_temp_file(self):
        """Test SettingsManager with temporary config file"""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            # Create manager with temp file
            manager = SettingsManager()
            manager.config_file = tmp_path
            manager.settings = {"hosts": {}, "tasks": {}}  # Initialize properly

            # Test operations
            manager.add_host("test", "mongodb://test:27017")
            manager.add_task("test_task", {
                "source_uri": "mongodb://source",
                "target_uri": "mongodb://target",
                "source_db": "test",
                "target_db": "test_backup"
            })

            # Save and reload
            manager.save_settings()

            # Create new manager to test loading
            new_manager = SettingsManager()
            new_manager.config_file = tmp_path
            new_manager.settings = new_manager.load_settings()

            assert new_manager.get_host("test") == "mongodb://test:27017"
            assert new_manager.get_task("test_task")["source_db"] == "test"

        finally:
            # Cleanup
            if tmp_path.exists():
                tmp_path.unlink()