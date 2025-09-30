"""
Tests for backup and restore functionality
"""

import os
import tempfile
import tarfile
from unittest.mock import Mock, patch, MagicMock, call
import pytest

from mongo_wizard.backup import BackupManager, BackupTask


class TestBackupManager:
    """Test backup manager functionality"""

    @patch('mongo_wizard.backup.connect_mongo')
    def test_connect(self, mock_connect):
        """Test MongoDB connection"""
        mock_instance = MagicMock()
        mock_connect.return_value = mock_instance

        mgr = BackupManager("mongodb://localhost", "/tmp")
        result = mgr.connect()

        assert result is True
        mock_connect.assert_called_with("mongodb://localhost", timeout=5000)

    @patch('mongo_wizard.backup.connect_mongo')
    def test_connect_failure(self, mock_connect):
        """Test MongoDB connection failure"""
        mock_connect.side_effect = Exception("Connection failed")

        mgr = BackupManager("mongodb://localhost", "/tmp")
        result = mgr.connect()

        assert result is False

    @patch('tempfile.TemporaryDirectory')
    @patch('subprocess.run')
    @patch('mongo_wizard.backup.connect_mongo')
    @patch('mongo_wizard.backup.check_mongodb_tools')
    def test_backup_database(self, mock_tools, mock_connect, mock_run, mock_tempdir):
        """Test database backup process"""
        # Mock MongoDB tools check
        mock_tools.return_value = {'mongodump': True, 'mongorestore': True}

        # Mock MongoDB client
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ['users', 'posts']
        mock_db['users'].estimated_document_count.return_value = 1000
        mock_db['posts'].estimated_document_count.return_value = 500

        mock_instance = MagicMock()
        mock_instance.__getitem__.return_value = mock_db
        mock_connect.return_value = mock_instance

        # Mock subprocess for mongodump
        mock_run.return_value = MagicMock(returncode=0)

        # Create real temp directory for testing
        with tempfile.TemporaryDirectory() as real_tmpdir:
            # Mock the TemporaryDirectory context manager
            mock_temp_context = MagicMock()
            mock_temp_context.__enter__ = MagicMock(return_value=real_tmpdir)
            mock_temp_context.__exit__ = MagicMock(return_value=None)
            mock_tempdir.return_value = mock_temp_context

            # Create the dump directory that mongodump would create
            dump_path = os.path.join(real_tmpdir, "dump", "testdb")
            os.makedirs(dump_path, exist_ok=True)
            # Create fake BSON files
            with open(os.path.join(dump_path, "users.bson"), "wb") as f:
                f.write(b"fake bson")
            with open(os.path.join(dump_path, "posts.bson"), "wb") as f:
                f.write(b"fake bson")

            mgr = BackupManager("mongodb://localhost", "/tmp/storage")

            # Mock the storage upload
            mgr.storage.upload = MagicMock(return_value=True)

            result = mgr.backup_database("testdb")

            assert result['success'] is True
            assert result['database'] == "testdb"
            assert result['documents'] == 1000  # 500 + 500
            assert result['collections'] == 2
            assert 'filename' in result
            assert '.tar.gz' in result['filename']

    @patch('tempfile.TemporaryDirectory')
    @patch('subprocess.run')
    @patch('mongo_wizard.backup.connect_mongo')
    @patch('mongo_wizard.backup.check_mongodb_tools')
    def test_backup_specific_collections(self, mock_tools, mock_connect, mock_run, mock_tempdir):
        """Test backing up specific collections"""
        mock_tools.return_value = {'mongodump': True, 'mongorestore': True}

        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ['users', 'posts', 'comments']
        mock_db['users'].estimated_document_count.return_value = 1000
        mock_db['posts'].estimated_document_count.return_value = 500

        mock_instance = MagicMock()
        mock_instance.__getitem__.return_value = mock_db
        mock_connect.return_value = mock_instance

        mock_run.return_value = MagicMock(returncode=0)

        # Create real temp directory for testing
        with tempfile.TemporaryDirectory() as real_tmpdir:
            # Mock the TemporaryDirectory context manager
            mock_temp_context = MagicMock()
            mock_temp_context.__enter__ = MagicMock(return_value=real_tmpdir)
            mock_temp_context.__exit__ = MagicMock(return_value=None)
            mock_tempdir.return_value = mock_temp_context

            # Create the dump directory that mongodump would create
            dump_path = os.path.join(real_tmpdir, "dump", "testdb")
            os.makedirs(dump_path, exist_ok=True)
            # Create fake BSON files
            with open(os.path.join(dump_path, "users.bson"), "wb") as f:
                f.write(b"fake bson")
            with open(os.path.join(dump_path, "posts.bson"), "wb") as f:
                f.write(b"fake bson")

            mgr = BackupManager("mongodb://localhost", "/tmp/storage")
            mgr.storage.upload = MagicMock(return_value=True)

            result = mgr.backup_database("testdb", collections=['users', 'posts'])

            assert result['success'] is True
            assert result['collections'] == 2
            assert result['documents'] == 1000  # 500 + 500

    @patch('subprocess.run')
    @patch('mongo_wizard.backup.connect_mongo')
    @patch('mongo_wizard.backup.check_mongodb_tools')
    def test_restore_database(self, mock_tools, mock_connect, mock_run):
        """Test database restore process"""
        mock_tools.return_value = {'mongodump': True, 'mongorestore': True}

        # Create a mock backup file
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a fake backup structure
            backup_dir = os.path.join(tmpdir, "dump", "testdb")
            os.makedirs(backup_dir)

            # Create fake BSON files
            for coll in ['users.bson', 'posts.bson']:
                with open(os.path.join(backup_dir, coll), 'wb') as f:
                    f.write(b"fake bson data")

            # Create tar.gz archive
            backup_file = os.path.join(tmpdir, "backup.tar.gz")
            with tarfile.open(backup_file, 'w:gz') as tar:
                tar.add(os.path.join(tmpdir, "dump"), arcname="dump")

            # Mock MongoDB client
            mock_db = MagicMock()
            mock_db.list_collection_names.return_value = ['users', 'posts']
            mock_db['users'].estimated_document_count.return_value = 1000
            mock_db['posts'].estimated_document_count.return_value = 500

            mock_instance = MagicMock()
            mock_instance.__getitem__.return_value = mock_db
            mock_connect.return_value = mock_instance

            # Mock subprocess for mongorestore
            mock_run.return_value = MagicMock(returncode=0)

            mgr = BackupManager("mongodb://localhost", tmpdir)
            mgr.storage.download = MagicMock(side_effect=lambda src, dst: os.link(backup_file, dst) or True)

            result = mgr.restore_database(backup_file, target_database="restored_db")

            assert result['success'] is True
            assert result['database'] == "restored_db"
            assert result['collections'] == 2

    @patch('mongo_wizard.backup.StorageFactory')
    def test_list_backups(self, mock_factory):
        """Test listing available backups"""
        mock_storage = MagicMock()
        mock_storage.list_files.return_value = [
            {
                'name': 'backup1.tar.gz',
                'path': '/tmp/backup1.tar.gz',
                'size': 1024,
                'size_human': '1.0 KB',
                'modified': 'Today'
            },
            {
                'name': 'backup2.tar.gz',
                'path': '/tmp/backup2.tar.gz',
                'size': 2048,
                'size_human': '2.0 KB',
                'modified': 'Yesterday'
            }
        ]
        mock_factory.create.return_value = mock_storage

        mgr = BackupManager("mongodb://localhost", "/tmp")
        backups = mgr.list_backups()

        assert len(backups) == 2
        assert backups[0]['name'] == 'backup1.tar.gz'

    @patch('mongo_wizard.backup.StorageFactory')
    def test_list_backups_with_filter(self, mock_factory):
        """Test listing backups with database filter"""
        mock_storage = MagicMock()
        mock_storage.list_files.return_value = [
            {
                'name': '2024_01_01-testdb.tar.gz',
                'path': '/tmp/2024_01_01-testdb.tar.gz',
                'size': 1024,
                'size_human': '1.0 KB',
                'modified': 'Today'
            },
            {
                'name': '2024_01_01-otherdb.tar.gz',
                'path': '/tmp/2024_01_01-otherdb.tar.gz',
                'size': 2048,
                'size_human': '2.0 KB',
                'modified': 'Yesterday'
            }
        ]
        mock_factory.create.return_value = mock_storage

        mgr = BackupManager("mongodb://localhost", "/tmp")
        backups = mgr.list_backups(database_filter="testdb")

        assert len(backups) == 1
        assert 'testdb' in backups[0]['name']


class TestBackupTask:
    """Test backup task configuration"""

    def test_create_backup_task(self):
        """Test creating backup task config"""
        task = BackupTask.create_backup_task(
            name="daily_backup",
            mongo_uri="mongodb://localhost",
            database="production",
            collections=['users', 'posts'],
            storage_url="ssh://backup@server:/backups"
        )

        assert task['type'] == 'backup'
        assert task['name'] == 'daily_backup'
        assert task['database'] == 'production'
        assert task['collections'] == ['users', 'posts']
        assert 'created' in task

    def test_create_restore_task(self):
        """Test creating restore task config"""
        task = BackupTask.create_restore_task(
            name="emergency_restore",
            mongo_uri="mongodb://localhost",
            backup_file="/backups/latest.tar.gz",
            target_database="restored_db",
            storage_url="/backups",
            drop_target=True
        )

        assert task['type'] == 'restore'
        assert task['name'] == 'emergency_restore'
        assert task['drop_target'] is True
        assert task['target_database'] == 'restored_db'