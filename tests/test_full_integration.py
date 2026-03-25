#!/usr/bin/env python
"""
Full Integration Test Suite for db-wizard
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

from db_wizard.engines.mongo import MongoEngine
from db_wizard.settings import SettingsManager
from db_wizard.wizard import DbWizard


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

        self.client = MongoClient("mongodb://localhost:27017")

        self.tools = MongoEngine("mongodb://localhost:27017").check_tools()

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

    def create_test_data(self):
        """Create comprehensive test data with various scenarios"""
        source_db = self.client[self.source_db_name]

        small_docs = [
            {"_id": i, "type": "small", "value": i, "name": f"Small Doc {i}"}
            for i in range(10)
        ]
        source_db.small_collection.insert_many(small_docs)
        source_db.small_collection.create_index("name")

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

        special_docs = [{"_id": i, "data": f"Special {i}"} for i in range(5)]
        source_db["special-collection.with.dots"].insert_many(special_docs)

        source_db.create_collection("empty_collection")

    # ========== CORE COPY OPERATIONS TESTS ==========

    def test_copy_single_collection(self):
        """Test copying a single collection with indexes"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="small_collection",
                target_db=self.target_db_name, target_table="small_collection_copy",
                drop_target=True
            )

            assert result['documents_copied'] == 10
            assert result['indexes_created'] > 0

            target_coll = self.client[self.target_db_name]["small_collection_copy"]
            assert target_coll.count_documents({}) == 10

            indexes = list(target_coll.list_indexes())
            assert len(indexes) > 1

        finally:
            source_engine.close()
            target_engine.close()

    def test_copy_entire_database(self):
        """Test copying an entire database"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table=None,
                target_db=self.target_db_name, target_table=None,
                drop_target=True,
                force=True
            )

            source_colls = set(self.client[self.source_db_name].list_collection_names())
            target_colls = set(self.client[self.target_db_name].list_collection_names())
            assert source_colls == target_colls

            for coll_name in source_colls:
                source_count = self.client[self.source_db_name][coll_name].count_documents({})
                target_count = self.client[self.target_db_name][coll_name].count_documents({})
                assert source_count == target_count, f"Count mismatch for {coll_name}"

        finally:
            source_engine.close()
            target_engine.close()

    def test_copy_multiple_collections(self):
        """Test copying multiple specific collections"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            collections_to_copy = ["small_collection", "medium_collection", "text_collection"]
            results = {}

            for coll_name in collections_to_copy:
                result = target_engine.copy(
                    source_engine=source_engine,
                    source_db=self.source_db_name, source_table=coll_name,
                    target_db=self.target_db_name, target_table=coll_name,
                    drop_target=True
                )
                results[coll_name] = result

            target_colls = list(self.client[self.target_db_name].list_collection_names())
            assert set(target_colls) == set(collections_to_copy)

            for coll_name in collections_to_copy:
                assert coll_name in results
                source_count = self.client[self.source_db_name][coll_name].count_documents({})
                assert results[coll_name]['documents_copied'] == source_count

        finally:
            source_engine.close()
            target_engine.close()

    def test_backup_before_drop(self):
        """Test automatic backup creation before dropping target"""
        target_db = self.client[self.target_db_name]
        target_db.existing_collection.insert_many([
            {"_id": i, "data": f"Existing {i}"} for i in range(20)
        ])

        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            backup_name = target_engine.backup_before_copy(
                self.target_db_name,
                "existing_collection"
            )

            assert backup_name is not None
            backup_coll = self.client[self.target_db_name][backup_name]
            assert backup_coll.count_documents({}) == 20

            target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="small_collection",
                target_db=self.target_db_name, target_table="existing_collection",
                drop_target=True,
                force=True
            )

            assert target_db.existing_collection.count_documents({}) == 10
            assert backup_coll.count_documents({}) == 20

        finally:
            source_engine.close()
            target_engine.close()

    def test_verify_copy_integrity(self):
        """Test copy verification functionality"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="medium_collection",
                target_db=self.target_db_name, target_table="medium_collection",
                drop_target=True
            )

            verification = source_engine.verify_copy(
                self.source_db_name, "medium_collection",
                self.target_db_name, "medium_collection",
                target_engine=target_engine
            )

            assert verification['count_match'] is True
            assert verification['source_count'] == 1000
            assert verification['target_count'] == 1000
            assert verification['index_match'] is True

        finally:
            source_engine.close()
            target_engine.close()

    @pytest.mark.skipif(
        not MongoEngine("mongodb://localhost:27017").check_tools().get('mongodump', False),
        reason="mongodump not available"
    )
    def test_mongodump_method(self):
        """Test copy using mongodump/mongorestore"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="medium_collection",
                target_db=self.target_db_name, target_table="medium_dump_copy",
                drop_target=True,
                force_python=False
            )

            assert result.get('method') == 'mongodump'

            target_coll = self.client[self.target_db_name]["medium_dump_copy"]
            assert target_coll.count_documents({}) == 1000

        finally:
            source_engine.close()
            target_engine.close()

    def test_python_fallback_method(self):
        """Test copy using Python fallback method"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="small_collection",
                target_db=self.target_db_name, target_table="small_python_copy",
                drop_target=True,
                force_python=True
            )

            target_coll = self.client[self.target_db_name]["small_python_copy"]
            assert target_coll.count_documents({}) == 10

        finally:
            source_engine.close()
            target_engine.close()

    # ========== SETTINGS MANAGEMENT TESTS ==========

    def test_settings_manager_complete(self):
        """Test all settings manager operations"""
        with tempfile.TemporaryDirectory() as tmpdir:
            settings_file = Path(tmpdir) / "test_settings.json"

            manager = SettingsManager()
            manager.config_file = settings_file

            manager.add_host("local", "mongodb://localhost:27017")
            manager.add_host("remote", "mongodb://remote.server:27017")

            task_config = {
                "source_uri": "mongodb://localhost:27017",
                "target_uri": "mongodb://remote:27017",
                "source_db": "test",
                "target_db": "test_copy",
                "source_collection": ["coll1", "coll2"],
                "drop_target": True,
                "verify": True
            }
            manager.add_task("daily_backup", task_config)

            manager.save_settings()

            new_manager = SettingsManager()
            new_manager.config_file = settings_file
            new_manager.settings = new_manager.load_settings()
            new_settings = new_manager.settings

            assert "local" in new_settings['hosts']
            assert new_settings['hosts']['local'] == "mongodb://localhost:27017"

            assert "daily_backup" in new_settings['tasks']
            loaded_task = new_settings['tasks']['daily_backup']
            assert loaded_task['source_db'] == "test"
            assert isinstance(loaded_task['source_collection'], list)
            assert len(loaded_task['source_collection']) == 2

            assert new_manager.get_host("local") == "mongodb://localhost:27017"
            assert new_manager.get_task("daily_backup") is not None

            assert new_manager.delete_host("remote") is True
            assert new_manager.delete_task("daily_backup") is True

            assert new_manager.get_host("remote") is None
            assert new_manager.get_task("daily_backup") is None

    # ========== EDGE CASES AND ERROR HANDLING ==========

    def test_empty_collection_copy(self):
        """Test copying an empty collection"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="empty_collection",
                target_db=self.target_db_name, target_table="empty_copy",
                drop_target=True
            )

            assert result['documents_copied'] == 0
            assert "empty_copy" in self.client[self.target_db_name].list_collection_names()
            assert self.client[self.target_db_name]["empty_copy"].count_documents({}) == 0

        finally:
            source_engine.close()
            target_engine.close()

    def test_special_characters_collection(self):
        """Test copying collection with special characters in name"""
        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="special-collection.with.dots",
                target_db=self.target_db_name, target_table="special-collection.with.dots",
                drop_target=True
            )

            assert result['documents_copied'] == 5

            target_coll = self.client[self.target_db_name]["special-collection.with.dots"]
            assert target_coll.count_documents({}) == 5

        finally:
            source_engine.close()
            target_engine.close()

    def test_copy_with_existing_target_no_drop(self):
        """Test copying to existing collection without dropping"""
        target_db = self.client[self.target_db_name]
        target_db.merged_collection.insert_many([
            {"_id": f"existing_{i}", "data": f"Existing {i}"}
            for i in range(5)
        ])

        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="small_collection",
                target_db=self.target_db_name, target_table="merged_collection",
                drop_target=False
            )

            assert target_db.merged_collection.count_documents({}) == 15

        finally:
            source_engine.close()
            target_engine.close()

    def test_copy_large_documents(self):
        """Test copying collection with large documents"""
        source_db = self.client[self.source_db_name]

        large_docs = [
            {
                "_id": i,
                "data": "x" * (1024 * 1024),
                "metadata": {"index": i}
            }
            for i in range(5)
        ]
        source_db.large_collection.insert_many(large_docs)

        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="large_collection",
                target_db=self.target_db_name, target_table="large_copy",
                drop_target=True
            )

            assert result['documents_copied'] == 5

            target_doc = self.client[self.target_db_name]["large_copy"].find_one({"_id": 0})
            assert len(target_doc['data']) == 1024 * 1024

        finally:
            source_engine.close()
            target_engine.close()

    # ========== PERFORMANCE TESTS ==========

    @pytest.mark.skipif(
        not MongoEngine("mongodb://localhost:27017").check_tools().get('mongodump', False),
        reason="mongodump not available for comparison"
    )
    def test_performance_comparison(self):
        """Compare performance of mongodump vs Python copy"""
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

        source_engine = MongoEngine("mongodb://localhost:27017")
        target_engine = MongoEngine("mongodb://localhost:27017")
        source_engine.connect()
        target_engine.connect()

        try:
            start_dump = time.time()
            result_dump = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="perf_test",
                target_db=self.target_db_name, target_table="perf_mongodump",
                drop_target=True,
                force_python=False
            )
            time_mongodump = time.time() - start_dump

            start_python = time.time()
            result_python = target_engine.copy(
                source_engine=source_engine,
                source_db=self.source_db_name, source_table="perf_test",
                target_db=self.target_db_name, target_table="perf_python",
                drop_target=True,
                force_python=True
            )
            time_python = time.time() - start_python

            assert result_dump['documents_copied'] == 5000
            assert result_python['documents_copied'] == 5000

            print(f"\nPerformance Test Results (5000 docs):")
            print(f"  mongodump: {time_mongodump:.2f}s")
            print(f"  Python:    {time_python:.2f}s")
            print(f"  Ratio:     {time_python/time_mongodump:.2f}x")

        finally:
            source_engine.close()
            target_engine.close()

    # ========== CLI TESTS ==========

    def test_cli_list_commands(self):
        """Test CLI list commands work correctly"""
        result = subprocess.run(
            [sys.executable, "-m", "db_wizard", "--list-hosts"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
        assert len(result.stdout) > 0

        result = subprocess.run(
            [sys.executable, "-m", "db_wizard", "--list-tasks"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
        assert len(result.stdout) > 0

    def test_engine_connection_testing(self):
        """Test engine connection testing"""
        engine = MongoEngine("mongodb://localhost:27017")
        is_online, msg = engine.test_connection()
        assert is_online is True

        bad_engine = MongoEngine("mongodb://invalid.host:27017")
        is_online, msg = bad_engine.test_connection(timeout=100)
        assert is_online is False

    # ========== BACKUP / RESTORE ==========

    def test_backup_and_restore_local(self):
        """Test backup and restore operations with local storage"""
        from db_wizard.backup import BackupManager

        with tempfile.TemporaryDirectory() as temp_dir:
            backup_mgr = BackupManager(
                "mongodb://localhost:27017",
                temp_dir
            )

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

            self.client.drop_database(self.target_db_name)

            restore_mgr = BackupManager(
                "mongodb://localhost:27017",
                temp_dir
            )

            restore_result = restore_mgr.restore_database(
                backup_file,
                target_database=self.target_db_name,
                drop_target=True
            )

            assert restore_result['success'] is True
            assert restore_result['database'] == self.target_db_name
            assert restore_result['documents'] >= 1010

            client = MongoClient("mongodb://localhost:27017")
            db = client[self.target_db_name]

            assert 'small_collection' in db.list_collection_names()
            assert 'medium_collection' in db.list_collection_names()
            assert db['small_collection'].count_documents({}) == 10
            assert db['medium_collection'].count_documents({}) == 1000

            client.close()
            backup_mgr.close()
            restore_mgr.close()

        self.client.drop_database(self.source_db_name)
        self.client.drop_database(self.target_db_name)

    def test_backup_entire_database(self):
        """Test backing up entire database"""
        from db_wizard.backup import BackupManager

        with tempfile.TemporaryDirectory() as temp_dir:
            backup_mgr = BackupManager(
                "mongodb://localhost:27017",
                temp_dir
            )

            result = backup_mgr.backup_database(self.source_db_name)

            assert result['success'] is True
            assert result['collections'] == 6
            assert result['documents'] == 1165
            assert 'size_human' in result

            backup_mgr.close()

        self.client.drop_database(self.source_db_name)

    def test_zzz_final_cleanup(self):
        """Final test to ensure all test databases are cleaned up"""
        self.cleanup_test_databases()

        for db_name in self.client.list_database_names():
            assert not db_name.startswith(self.test_db_prefix)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
