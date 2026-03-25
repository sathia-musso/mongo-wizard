"""
Tests for backup and restore functionality
"""

import os
import tempfile
import tarfile
from unittest.mock import Mock, patch, MagicMock, call
import pytest

from db_wizard.backup import BackupManager, BackupTask


class TestBackupManager:
    """Test backup manager functionality"""

    @patch('db_wizard.backup.EngineFactory')
    def test_connect(self, mock_factory):
        """Test database connection via engine"""
        mock_engine = MagicMock()
        mock_engine.client = MagicMock()
        mock_factory.create.return_value = mock_engine

        mgr = BackupManager("mongodb://localhost", "/tmp")
        result = mgr.connect()

        assert result is True
        mock_factory.create.assert_called_with("mongodb://localhost")
        mock_engine.connect.assert_called_once()

    @patch('db_wizard.backup.EngineFactory')
    def test_connect_failure(self, mock_factory):
        """Test database connection failure"""
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("Connection failed")
        mock_factory.create.return_value = mock_engine

        mgr = BackupManager("mongodb://localhost", "/tmp")
        result = mgr.connect()

        assert result is False

    @patch('subprocess.run')
    @patch('db_wizard.backup.EngineFactory')
    def test_backup_database(self, mock_factory, mock_run, tmp_path):
        """Test database backup process"""
        mock_engine = MagicMock()
        mock_engine.client = MagicMock()
        mock_engine.list_tables.return_value = [
            {'name': 'users', 'rows': 1000, 'indexes': 2},
            {'name': 'posts', 'rows': 500, 'indexes': 1},
        ]
        mock_engine.count_rows.side_effect = lambda db, t: {'users': 500, 'posts': 500}[t]
        mock_engine.check_tools.return_value = {'mongodump': True, 'mongorestore': True}
        mock_engine.dump.return_value = True
        mock_factory.create.return_value = mock_engine

        mock_run.return_value = MagicMock(returncode=0)

        real_tmpdir = str(tmp_path)
        with patch('tempfile.TemporaryDirectory') as mock_tempdir:
            mock_temp_context = MagicMock()
            mock_temp_context.__enter__ = MagicMock(return_value=real_tmpdir)
            mock_temp_context.__exit__ = MagicMock(return_value=None)
            mock_tempdir.return_value = mock_temp_context

            dump_path = os.path.join(real_tmpdir, "dump", "testdb")
            os.makedirs(dump_path, exist_ok=True)
            with open(os.path.join(dump_path, "users.bson"), "wb") as f:
                f.write(b"fake bson")
            with open(os.path.join(dump_path, "posts.bson"), "wb") as f:
                f.write(b"fake bson")

            mgr = BackupManager("mongodb://localhost", "/tmp/storage")
            mgr.storage.upload = MagicMock(return_value=True)

            result = mgr.backup_database("testdb")

            assert result['success'] is True
            assert result['database'] == "testdb"
            assert result['documents'] == 1000  # 500 + 500
            assert result['collections'] == 2
            assert 'filename' in result
            assert '.tar.gz' in result['filename']

    @patch('subprocess.run')
    @patch('db_wizard.backup.EngineFactory')
    def test_backup_specific_collections(self, mock_factory, mock_run, tmp_path):
        """Test backing up specific collections"""
        mock_engine = MagicMock()
        mock_engine.client = MagicMock()
        mock_engine.list_tables.return_value = [
            {'name': 'users', 'rows': 1000, 'indexes': 2},
            {'name': 'posts', 'rows': 500, 'indexes': 1},
            {'name': 'comments', 'rows': 200, 'indexes': 0},
        ]
        mock_engine.count_rows.side_effect = lambda db, t: {'users': 500, 'posts': 500}[t]
        mock_engine.check_tools.return_value = {'mongodump': True, 'mongorestore': True}
        mock_engine.dump.return_value = True
        mock_factory.create.return_value = mock_engine

        mock_run.return_value = MagicMock(returncode=0)

        real_tmpdir = str(tmp_path)
        with patch('tempfile.TemporaryDirectory') as mock_tempdir:
            mock_temp_context = MagicMock()
            mock_temp_context.__enter__ = MagicMock(return_value=real_tmpdir)
            mock_temp_context.__exit__ = MagicMock(return_value=None)
            mock_tempdir.return_value = mock_temp_context

            dump_path = os.path.join(real_tmpdir, "dump", "testdb")
            os.makedirs(dump_path, exist_ok=True)
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
    @patch('db_wizard.backup.EngineFactory')
    def test_restore_database(self, mock_factory, mock_run):
        """Test database restore process"""
        mock_engine = MagicMock()
        mock_engine.client = MagicMock()
        mock_engine.check_tools.return_value = {'mongodump': True, 'mongorestore': True}
        mock_engine.restore.return_value = True

        mock_users_coll = MagicMock()
        mock_users_coll.count_documents.return_value = 1000
        mock_posts_coll = MagicMock()
        mock_posts_coll.count_documents.return_value = 500

        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ['users', 'posts']
        mock_db.__getitem__.side_effect = lambda name: {'users': mock_users_coll, 'posts': mock_posts_coll}[name]
        mock_engine.client.__getitem__.return_value = mock_db

        mock_factory.create.return_value = mock_engine

        with tempfile.TemporaryDirectory() as tmpdir:
            backup_dir = os.path.join(tmpdir, "dump", "testdb")
            os.makedirs(backup_dir)

            for coll in ['users.bson', 'posts.bson']:
                with open(os.path.join(backup_dir, coll), 'wb') as f:
                    f.write(b"fake bson data")

            backup_file = os.path.join(tmpdir, "backup.tar.gz")
            with tarfile.open(backup_file, 'w:gz') as tar:
                tar.add(os.path.join(tmpdir, "dump"), arcname="dump")

            mock_run.return_value = MagicMock(returncode=0)

            mgr = BackupManager("mongodb://localhost", tmpdir)
            mgr.storage.download = MagicMock(side_effect=lambda src, dst: os.link(backup_file, dst) or True)

            result = mgr.restore_database(backup_file, target_database="restored_db")

            assert result['success'] is True
            assert result['database'] == "restored_db"
            assert result['collections'] == 2

    @patch('db_wizard.backup.StorageFactory')
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

    @patch('db_wizard.backup.StorageFactory')
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
            db_uri="mongodb://localhost",
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
            db_uri="mongodb://localhost",
            backup_file="/backups/latest.tar.gz",
            target_database="restored_db",
            storage_url="/backups",
            drop_target=True
        )

        assert task['type'] == 'restore'
        assert task['name'] == 'emergency_restore'
        assert task['drop_target'] is True
        assert task['target_database'] == 'restored_db'
