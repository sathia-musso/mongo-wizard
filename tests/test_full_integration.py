#!/usr/bin/env python
"""
Full Integration Test Suite for mongo-wizard
Tests all features with real MongoDB operations

Requirements:
- MongoDB running on localhost:27017
- mongodump and mongorestore installed (optional, will test fallback if not)

Run with: pytest tests/test_full_integration.py -v
"""

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch

import pytest
from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.errors import ConnectionFailure

# Import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))
from mongo_wizard.core import MongoAdvancedCopier
from mongo_wizard.settings import SettingsManager
from mongo_wizard.wizard import MongoWizard
from mongo_wizard.utils import check_mongodb_tools


# Check if MongoDB is available
def mongodb_available():
    """Check if MongoDB is running and accessible"""
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        client.close()
        return True
    except (ConnectionFailure, Exception):
        return False


# Skip all tests if MongoDB not available
pytestmark = pytest.mark.skipif(
    not mongodb_available(),
    reason="MongoDB not available on localhost:27017"
)


class TestFullIntegration:
    """Complete integration test suite testing all features"""

    def setup_method(self):
        """Setup test environment before each test method"""
        self.test_db_prefix = "mw_test_"
        self.source_db_name = f"{self.test_db_prefix}source"
        self.target_db_name = f"{self.test_db_prefix}target"
        self.backup_db_name = f"{self.test_db_prefix}backup"

        # Create client
        self.client = MongoClient("mongodb://localhost:27017")

        # Store MongoDB tools availability
        self.tools = check_mongodb_tools()

        # Clean and setup test data
        self.cleanup_test_databases()
        self.create_test_data()

    def teardown_method(self):
        """Cleanup after each test method"""
        self.cleanup_test_databases()
        self.client.close()

    def cleanup_test_databases(self):
        """Remove all test databases"""
        for db_name in self.client.list_database_names():
            if db_name.startswith(self.test_db_prefix):
                self.client.drop_database(db_name)
                print(f"Cleaned up test database: {db_name}")


    def create_test_data(self):
        """Create comprehensive test data with various scenarios"""
        source_db = self.client[self.source_db_name]

        # 1. Small collection (10 documents)
        small_docs = [
            {"_id": i, "type": "small", "value": i, "name": f"Small Doc {i}"}
            for i in range(10)
        ]
        source_db.small_collection.insert_many(small_docs)
        source_db.small_collection.create_index("name")

        # 2. Medium collection (1000 documents)
        medium_docs = [
            {
                "_id": i,
                "type": "medium",
                "value": i * 10,
                "name": f"Medium Doc {i}",
                "category": f"cat_{i % 10}",
                "tags": [f"tag_{j}" for j in range(i % 5)]
            }
            for i in range(1000)
        ]
        source_db.medium_collection.insert_many(medium_docs)
        source_db.medium_collection.create_index([("category", ASCENDING), ("value", -1)])
        source_db.medium_collection.create_index("tags")

        # 3. Collection with complex documents
        complex_docs = [
            {
                "_id": f"complex_{i}",
                "user": {
                    "name": f"User {i}",
                    "email": f"user{i}@example.com",
                    "profile": {
                        "age": 20 + i,
                        "location": {
                            "city": ["New York", "London", "Tokyo"][i % 3],
                            "country": ["USA", "UK", "Japan"][i % 3]
                        }
                    }
                },
                "posts": [
                    {
                        "title": f"Post {j}",
                        "content": f"Content for post {j}" * 10,
                        "likes": j * i
                    }
                    for j in range(3)
                ],
                "created_at": datetime.now(),
                "metadata": {"version": i, "active": i % 2 == 0}
            }
            for i in range(100)
        ]
        source_db.complex_collection.insert_many(complex_docs)

        # 4. Collection with text index
        text_docs = [
            {
                "_id": i,
                "title": f"Article {i}",
                "content": f"This is the content of article {i}. It contains searchable text.",
                "author": f"Author {i % 10}",
                "published": i % 2 == 0
            }
            for i in range(50)
        ]
        source_db.text_collection.insert_many(text_docs)
        source_db.text_collection.create_index([("title", TEXT), ("content", TEXT)])

        # 5. Collection with special characters in name
        special_docs = [{"_id": i, "data": f"Special {i}"} for i in range(5)]
        source_db["special-collection.with.dots"].insert_many(special_docs)

        # 6. Empty collection (for edge case testing)
        source_db.create_collection("empty_collection")

        print(f"Created test data in {self.source_db_name}:")
        for coll in source_db.list_collection_names():
            count = source_db[coll].count_documents({})
            print(f"  - {coll}: {count} documents")

    # ========== CORE COPY OPERATIONS TESTS ==========

    def test_copy_single_collection(self):
        """Test copying a single collection with indexes"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            result = copier.copy_collection_with_indexes(
                self.source_db_name, "small_collection",
                self.target_db_name, "small_collection_copy",
                drop_target=True
            )

            # Verify results
            assert result['documents_copied'] == 10
            assert result['indexes_created'] > 0

            # Verify data
            target_coll = self.client[self.target_db_name]["small_collection_copy"]
            assert target_coll.count_documents({}) == 10

            # Verify indexes were copied
            indexes = list(target_coll.list_indexes())
            assert len(indexes) > 1  # Should have _id index + custom indexes

        finally:
            copier.close()

    def test_copy_entire_database(self):
        """Test copying an entire database"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            results = copier.copy_entire_database(
                self.source_db_name,
                self.target_db_name,
                drop_target=True,
                force=True  # Skip confirmation in tests
            )

            # Verify all collections were copied
            source_colls = set(self.client[self.source_db_name].list_collection_names())
            target_colls = set(self.client[self.target_db_name].list_collection_names())

            assert source_colls == target_colls

            # Verify document counts match
            for coll_name in source_colls:
                source_count = self.client[self.source_db_name][coll_name].count_documents({})
                target_count = self.client[self.target_db_name][coll_name].count_documents({})
                assert source_count == target_count, f"Count mismatch for {coll_name}"

        finally:
            copier.close()

    def test_copy_multiple_collections(self):
        """Test copying multiple specific collections"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            collections_to_copy = ["small_collection", "medium_collection", "text_collection"]

            results = copier.copy_multiple_collections(
                self.source_db_name,
                self.target_db_name,
                collections_to_copy,
                drop_target=True
            )

            # Verify only specified collections were copied
            target_colls = list(self.client[self.target_db_name].list_collection_names())
            assert set(target_colls) == set(collections_to_copy)

            # Verify each collection
            for coll_name in collections_to_copy:
                assert coll_name in results
                source_count = self.client[self.source_db_name][coll_name].count_documents({})
                assert results[coll_name]['documents_copied'] == source_count

        finally:
            copier.close()

    def test_backup_before_drop(self):
        """Test automatic backup creation before dropping target"""
        # First create some data in target
        target_db = self.client[self.target_db_name]
        target_db.existing_collection.insert_many([
            {"_id": i, "data": f"Existing {i}"} for i in range(20)
        ])

        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            # Enable backup
            backup_name = copier.backup_before_copy(
                self.target_db_name,
                "existing_collection"
            )

            # Verify backup was created
            assert backup_name is not None
            backup_coll = self.client[self.target_db_name][backup_name]
            assert backup_coll.count_documents({}) == 20

            # Now copy with drop
            copier.copy_collection_with_indexes(
                self.source_db_name, "small_collection",
                self.target_db_name, "existing_collection",
                drop_target=True,
                force=True  # Skip confirmation in tests
            )

            # Verify original was replaced
            assert target_db.existing_collection.count_documents({}) == 10

            # Verify backup still exists
            assert backup_coll.count_documents({}) == 20

        finally:
            copier.close()

    def test_verify_copy_integrity(self):
        """Test copy verification functionality"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            # Copy a collection
            copier.copy_collection_with_indexes(
                self.source_db_name, "medium_collection",
                self.target_db_name, "medium_collection",
                drop_target=True
            )

            # Verify the copy
            verification = copier.verify_copy(
                self.source_db_name, "medium_collection",
                self.target_db_name, "medium_collection"
            )

            assert verification['count_match'] is True
            assert verification['source_count'] == 1000
            assert verification['target_count'] == 1000
            assert verification['index_match'] is True

        finally:
            copier.close()

    @pytest.mark.skipif(
        not check_mongodb_tools()['mongodump'],
        reason="mongodump not available"
    )
    def test_mongodump_method(self):
        """Test copy using mongodump/mongorestore"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            # Force mongodump method
            result = copier.copy_collection_with_indexes(
                self.source_db_name, "medium_collection",
                self.target_db_name, "medium_dump_copy",
                drop_target=True,
                force_python=False  # Use mongodump if available
            )

            # Check method used
            assert result.get('method') == 'mongodump'

            # Verify copy
            target_coll = self.client[self.target_db_name]["medium_dump_copy"]
            assert target_coll.count_documents({}) == 1000

        finally:
            copier.close()

    def test_python_fallback_method(self):
        """Test copy using Python fallback method"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            # Force Python method
            result = copier.copy_collection_with_indexes(
                self.source_db_name, "small_collection",
                self.target_db_name, "small_python_copy",
                drop_target=True,
                force_python=True  # Force Python method
            )

            # Verify copy
            target_coll = self.client[self.target_db_name]["small_python_copy"]
            assert target_coll.count_documents({}) == 10

        finally:
            copier.close()

    # ========== SETTINGS MANAGEMENT TESTS ==========

    def test_settings_manager_complete(self):
        """Test all settings manager operations"""
        with tempfile.TemporaryDirectory() as tmpdir:
            settings_file = Path(tmpdir) / "test_settings.json"

            # Create manager with custom file
            manager = SettingsManager()
            manager.config_file = settings_file

            # Test adding hosts
            manager.add_host("local", "mongodb://localhost:27017")
            manager.add_host("remote", "mongodb://remote.server:27017")

            # Test adding tasks
            task_config = {
                "source_uri": "mongodb://localhost:27017",
                "target_uri": "mongodb://remote:27017",
                "source_db": "test",
                "target_db": "test_copy",
                "source_collection": ["coll1", "coll2"],  # Test with array
                "drop_target": True,
                "verify": True
            }
            manager.add_task("daily_backup", task_config)

            # Save and reload
            manager.save_settings()

            # Create new manager to test loading
            new_manager = SettingsManager()
            new_manager.config_file = settings_file
            new_manager.settings = new_manager.load_settings()  # Reload settings
            new_settings = new_manager.settings

            # Verify hosts
            assert "local" in new_settings['hosts']
            assert new_settings['hosts']['local'] == "mongodb://localhost:27017"

            # Verify tasks
            assert "daily_backup" in new_settings['tasks']
            loaded_task = new_settings['tasks']['daily_backup']
            assert loaded_task['source_db'] == "test"
            assert isinstance(loaded_task['source_collection'], list)
            assert len(loaded_task['source_collection']) == 2

            # Test get operations
            assert new_manager.get_host("local") == "mongodb://localhost:27017"
            assert new_manager.get_task("daily_backup") is not None

            # Test delete operations
            assert new_manager.delete_host("remote") is True
            assert new_manager.delete_task("daily_backup") is True

            # Verify deletions
            assert new_manager.get_host("remote") is None
            assert new_manager.get_task("daily_backup") is None

    # ========== EDGE CASES AND ERROR HANDLING ==========

    def test_empty_collection_copy(self):
        """Test copying an empty collection"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            result = copier.copy_collection_with_indexes(
                self.source_db_name, "empty_collection",
                self.target_db_name, "empty_copy",
                drop_target=True
            )

            assert result['documents_copied'] == 0

            # Verify collection exists but is empty
            assert "empty_copy" in self.client[self.target_db_name].list_collection_names()
            assert self.client[self.target_db_name]["empty_copy"].count_documents({}) == 0

        finally:
            copier.close()

    def test_special_characters_collection(self):
        """Test copying collection with special characters in name"""
        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            result = copier.copy_collection_with_indexes(
                self.source_db_name, "special-collection.with.dots",
                self.target_db_name, "special-collection.with.dots",
                drop_target=True
            )

            assert result['documents_copied'] == 5

            # Verify
            target_coll = self.client[self.target_db_name]["special-collection.with.dots"]
            assert target_coll.count_documents({}) == 5

        finally:
            copier.close()

    def test_copy_with_existing_target_no_drop(self):
        """Test copying to existing collection without dropping"""
        # Create existing data in target
        target_db = self.client[self.target_db_name]
        target_db.merged_collection.insert_many([
            {"_id": f"existing_{i}", "data": f"Existing {i}"}
            for i in range(5)
        ])

        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            # Copy without drop (should merge)
            result = copier.copy_collection_with_indexes(
                self.source_db_name, "small_collection",
                self.target_db_name, "merged_collection",
                drop_target=False  # Don't drop, merge instead
            )

            # Should have original 5 + new 10 = 15 documents
            assert target_db.merged_collection.count_documents({}) == 15

        finally:
            copier.close()

    def test_copy_large_documents(self):
        """Test copying collection with large documents"""
        source_db = self.client[self.source_db_name]

        # Create large documents (but under 16MB limit)
        large_docs = [
            {
                "_id": i,
                "data": "x" * (1024 * 1024),  # 1MB of data
                "metadata": {"index": i}
            }
            for i in range(5)
        ]
        source_db.large_collection.insert_many(large_docs)

        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            result = copier.copy_collection_with_indexes(
                self.source_db_name, "large_collection",
                self.target_db_name, "large_copy",
                drop_target=True
            )

            assert result['documents_copied'] == 5

            # Verify large documents were copied correctly
            target_doc = self.client[self.target_db_name]["large_copy"].find_one({"_id": 0})
            assert len(target_doc['data']) == 1024 * 1024

        finally:
            copier.close()

    # ========== PERFORMANCE TESTS ==========

    @pytest.mark.skipif(
        not check_mongodb_tools()['mongodump'],
        reason="mongodump not available for comparison"
    )
    def test_performance_comparison(self):
        """Compare performance of mongodump vs Python copy"""
        # Create a decent sized collection for testing
        source_db = self.client[self.source_db_name]
        perf_docs = [
            {
                "_id": i,
                "data": f"Document {i}",
                "value": i * 3.14,
                "tags": [f"tag{j}" for j in range(10)]
            }
            for i in range(5000)
        ]
        source_db.perf_test.insert_many(perf_docs)

        copier = MongoAdvancedCopier(
            "mongodb://localhost:27017",
            "mongodb://localhost:27017"
        )
        copier.connect()

        try:
            # Test with mongodump
            start_dump = time.time()
            result_dump = copier.copy_collection_with_indexes(
                self.source_db_name, "perf_test",
                self.target_db_name, "perf_mongodump",
                drop_target=True,
                force_python=False
            )
            time_mongodump = time.time() - start_dump

            # Test with Python
            start_python = time.time()
            result_python = copier.copy_collection_with_indexes(
                self.source_db_name, "perf_test",
                self.target_db_name, "perf_python",
                drop_target=True,
                force_python=True
            )
            time_python = time.time() - start_python

            # Both should copy same amount
            assert result_dump['documents_copied'] == 5000
            assert result_python['documents_copied'] == 5000

            # Log performance (mongodump should be faster for larger sets)
            print(f"\nPerformance Test Results (5000 docs):")
            print(f"  mongodump: {time_mongodump:.2f}s")
            print(f"  Python:    {time_python:.2f}s")
            print(f"  Ratio:     {time_python/time_mongodump:.2f}x")

            # mongodump should generally be faster for this size
            # But we won't assert this as it depends on the system

        finally:
            copier.close()

    # ========== CLI TESTS ==========

    def test_cli_list_commands(self):
        """Test CLI list commands work correctly"""
        # We'll test the actual commands work, not mock settings
        # Since CLI uses real settings file, we just test it runs without error

        # Test list-hosts command
        result = subprocess.run(
            [sys.executable, "-m", "mongo_wizard", "--list-hosts"],
            capture_output=True,
            text=True
        )

        # Should run without error
        assert result.returncode == 0
        # Should have some output (either hosts or "No saved hosts")
        assert len(result.stdout) > 0

        # Test list-tasks command
        result = subprocess.run(
            [sys.executable, "-m", "mongo_wizard", "--list-tasks"],
            capture_output=True,
            text=True
        )

        # Should run without error
        assert result.returncode == 0
        # Should have some output
        assert len(result.stdout) > 0

    def test_wizard_operations(self):
        """Test wizard class operations"""
        from mongo_wizard.utils import test_connection
        wizard = MongoWizard()

        # Test connection testing
        is_online, msg = test_connection("mongodb://localhost:27017", timeout=1000)
        assert is_online is True

        # Test with invalid connection
        is_online, msg = test_connection("mongodb://invalid.host:27017", timeout=100)
        assert is_online is False

    # ========== CLEANUP ==========

    def test_backup_and_restore_local(self):
        """Test backup and restore operations with local storage"""
        import tempfile
        import os
        from mongo_wizard.backup import BackupManager

        # Create backup
        with tempfile.TemporaryDirectory() as temp_dir:
            backup_mgr = BackupManager(
                f"mongodb://localhost:27017",
                temp_dir
            )

            # Perform backup
            result = backup_mgr.backup_database(
                self.source_db_name,
                collections=['small_collection', 'medium_collection']
            )

            assert result['success'] is True
            assert result['documents'] == 1010  # 10 + 1000
            assert result['collections'] == 2
            assert 'filename' in result

            backup_file = os.path.join(temp_dir, result['filename'])
            assert os.path.exists(backup_file)

            # Clean target database
            self.client.drop_database(self.target_db_name)

            # Perform restore
            restore_mgr = BackupManager(
                f"mongodb://localhost:27017",
                temp_dir
            )

            restore_result = restore_mgr.restore_database(
                backup_file,
                target_database=self.target_db_name,
                drop_target=True
            )

            assert restore_result['success'] is True
            assert restore_result['database'] == self.target_db_name
            assert restore_result['documents'] >= 1010  # Should have restored documents

            # Verify restored data
            client = MongoClient("mongodb://localhost:27017")
            db = client[self.target_db_name]

            assert 'small_collection' in db.list_collection_names()
            assert 'medium_collection' in db.list_collection_names()
            assert db['small_collection'].count_documents({}) == 10
            assert db['medium_collection'].count_documents({}) == 1000

            client.close()
            backup_mgr.close()
            restore_mgr.close()

            print(f"✅ Backup/Restore test passed - {result['documents']} documents")

        self.client.drop_database(self.source_db_name)
        self.client.drop_database(self.target_db_name)

    def test_backup_entire_database(self):
        """Test backing up entire database"""
        import tempfile
        from mongo_wizard.backup import BackupManager

        with tempfile.TemporaryDirectory() as temp_dir:
            backup_mgr = BackupManager(
                f"mongodb://localhost:27017",
                temp_dir
            )

            # Backup entire database
            result = backup_mgr.backup_database(self.source_db_name)

            assert result['success'] is True
            assert result['collections'] == 6  # All test collections
            assert result['documents'] == 1165  # Total documents
            assert 'size_human' in result

            backup_mgr.close()
            print(f"✅ Full database backup - {result['size_human']}")

        self.client.drop_database(self.source_db_name)

    def test_cli_backup_restore_commands(self):
        """Test CLI backup and restore commands"""
        import tempfile
        import glob

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test backup command
            backup_cmd = [
                sys.executable, '-m', 'mongo_wizard.cli',
                '--backup', f'mongodb://localhost:27017/{self.source_db_name}',
                '--backup-to', temp_dir,
                '-y'
            ]

            result = subprocess.run(backup_cmd, capture_output=True, text=True)
            assert result.returncode == 0
            assert 'Backup completed' in result.stdout

            # Find backup file
            backups = glob.glob(os.path.join(temp_dir, '*.tar.gz'))
            assert len(backups) == 1

            # Clean target
            self.client.drop_database(self.target_db_name)

            # Test restore command
            restore_cmd = [
                sys.executable, '-m', 'mongo_wizard.cli',
                '--restore', backups[0],
                '--restore-to', f'mongodb://localhost:27017',
                '--drop-target',
                '-y'
            ]

            result = subprocess.run(restore_cmd, capture_output=True, text=True)
            assert result.returncode == 0
            assert 'Restore completed' in result.stdout

        self.client.drop_database(self.source_db_name)
        self.client.drop_database(self.target_db_name)

    def test_zzz_final_cleanup(self):
        """Final test to ensure all test databases are cleaned up"""
        self.cleanup_test_databases()

        # Verify no test databases remain
        for db_name in self.client.list_database_names():
            assert not db_name.startswith(self.test_db_prefix)


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "--tb=short"])