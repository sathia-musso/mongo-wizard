"""
Tests for storage module
"""

import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
import pytest

from mongo_wizard.storage import (
    LocalStorage,
    SSHStorage,
    FTPStorage,
    StorageFactory,
    LocalStorageConfig,
    SSHStorageConfig,
    FTPStorageConfig
)


class TestLocalStorage:
    """Test local filesystem storage"""

    def test_list_files(self):
        """Test listing local files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            for i in range(3):
                test_file = os.path.join(tmpdir, f"backup_{i}.tar.gz")
                with open(test_file, 'w') as f:
                    f.write(f"test data {i}")

            storage = LocalStorage()
            files = storage.list_files(tmpdir)

            assert len(files) == 3
            assert all('backup_' in f['name'] for f in files)
            assert all(f['name'].endswith('.tar.gz') for f in files)

    def test_upload(self):
        """Test local file copy"""
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source.txt")
            target = os.path.join(tmpdir, "subdir", "target.txt")

            with open(source, 'w') as f:
                f.write("test data")

            storage = LocalStorage()
            result = storage.upload(source, target)

            assert result is True
            assert os.path.exists(target)
            with open(target, 'r') as f:
                assert f.read() == "test data"

    def test_download(self):
        """Test local file copy (download)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source.txt")
            target = os.path.join(tmpdir, "target.txt")

            with open(source, 'w') as f:
                f.write("test data")

            storage = LocalStorage()
            result = storage.download(source, target)

            assert result is True
            assert os.path.exists(target)

    def test_delete(self):
        """Test file deletion"""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, 'w') as f:
                f.write("test")

            storage = LocalStorage()
            result = storage.delete(test_file)

            assert result is True
            assert not os.path.exists(test_file)


class TestSSHStorage:
    """Test SSH storage backend"""

    @patch('subprocess.run')
    def test_build_ssh_command(self, mock_run):
        """Test SSH command building with shlex"""
        storage = SSHStorage(
            host="server.com",
            user="testuser",
            port=2222,
            key_path="/home/user/.ssh/id_rsa"
        )

        cmd = storage._build_ssh_command()

        assert isinstance(cmd, list)
        assert 'ssh' in cmd
        assert '-p' in cmd
        assert '2222' in cmd
        assert '-i' in cmd
        assert '/home/user/.ssh/id_rsa' in cmd
        assert 'testuser@server.com' in cmd

    @patch('subprocess.run')
    def test_build_scp_command(self, mock_run):
        """Test SCP command building"""
        storage = SSHStorage(
            host="server.com",
            user="testuser",
            port=2222
        )

        cmd = storage._build_scp_command()

        assert isinstance(cmd, list)
        assert 'scp' in cmd
        assert '-P' in cmd  # Note: capital P for SCP
        assert '2222' in cmd

    @patch('subprocess.run')
    def test_test_connection(self, mock_run):
        """Test SSH connection testing"""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="OK",
            stderr=""
        )

        storage = SSHStorage(
            host="server.com",
            user="testuser"
        )

        success, msg = storage.test_connection()

        assert success is True
        assert "successful" in msg

    @patch('subprocess.run')
    def test_upload_with_verification(self, mock_run):
        """Test SSH upload with verification"""
        # Mock successful upload and verification
        mock_run.side_effect = [
            MagicMock(returncode=0),  # mkdir
            MagicMock(returncode=0),  # scp
            MagicMock(returncode=0, stdout="-rw-r--r-- 1 user group 1024 Jan 1 00:00 /remote/file.tar.gz"),  # ls verification
        ]

        storage = SSHStorage(
            host="server.com",
            user="testuser"
        )

        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b"x" * 1024)
            tmp.flush()

            result = storage.upload(tmp.name, "/remote/file.tar.gz")

            # Should succeed with verification
            assert result is True


class TestStorageFactory:
    """Test storage factory"""

    def test_create_local_from_path(self):
        """Test creating local storage from path"""
        storage = StorageFactory.create("/path/to/local")
        assert isinstance(storage, LocalStorage)

    def test_create_ssh_from_url(self):
        """Test creating SSH storage from URL"""
        storage = StorageFactory.create("ssh://user@host:2222/path")
        assert isinstance(storage, SSHStorage)
        assert storage.user == "user"
        assert storage.host == "host"
        assert storage.port == 2222

    def test_create_ftp_from_url(self):
        """Test creating FTP storage from URL"""
        storage = StorageFactory.create("ftp://user:pass@host:2121/path")
        assert isinstance(storage, FTPStorage)
        assert storage.user == "user"
        assert storage.password == "pass"
        assert storage.host == "host"
        assert storage.port == 2121

    def test_create_from_config_dict(self):
        """Test creating storage from config dictionary"""
        config = {
            "type": "ssh",
            "host": "server.com",
            "user": "backup",
            "port": 22,
            "path": "/backups"
        }

        storage = StorageFactory.create(config)
        assert isinstance(storage, SSHStorage)
        assert storage.host == "server.com"
        assert storage.user == "backup"

    def test_create_config_from_url(self):
        """Test creating config object from URL"""
        config = StorageFactory.create_config(
            "ssh://user@host:2222/backups",
            "my_backup"
        )

        assert isinstance(config, SSHStorageConfig)
        assert config.name == "my_backup"
        assert config.host == "host"
        assert config.user == "user"
        assert config.port == 2222
        assert config.path == "/backups"


class TestStorageConfigs:
    """Test storage configuration dataclasses"""

    def test_local_config(self):
        """Test local storage config"""
        config = LocalStorageConfig(
            name="local_backup",
            path="/var/backups"
        )

        assert config.type == "local"
        assert config.name == "local_backup"
        assert config.path == "/var/backups"

        dict_repr = config.to_dict()
        assert dict_repr["type"] == "local"
        assert dict_repr["name"] == "local_backup"

    def test_ssh_config(self):
        """Test SSH storage config"""
        config = SSHStorageConfig(
            name="remote_backup",
            host="server.com",
            user="backup",
            path="/backups",
            port=2222,
            key_path="/home/user/.ssh/id_rsa"
        )

        assert config.type == "ssh"
        assert config.port == 2222
        assert config.key_path == "/home/user/.ssh/id_rsa"

    def test_ftp_config(self):
        """Test FTP storage config"""
        config = FTPStorageConfig(
            name="ftp_backup",
            host="ftp.server.com",
            user="ftpuser",
            password="secret",
            path="/backups"
        )

        assert config.type == "ftp"
        assert config.password == "secret"