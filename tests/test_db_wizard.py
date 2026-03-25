"""
Tests for db_wizard new components:
- EngineFactory (create, detect_scheme, check_same_engine)
- MongoEngine ABC compliance
- Settings migration + tunnel host support
- SSH tunnel module
- mask_password utility
"""

import os
import json
import shutil
import tempfile
import subprocess
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock

import pytest


# ===========================================================================
# EngineFactory
# ===========================================================================

class TestEngineFactory:
    """Test EngineFactory auto-detection and cross-engine check"""

    def test_create_mongo_from_uri(self):
        from db_wizard.engine import EngineFactory
        from db_wizard.engines.mongo import MongoEngine
        engine = EngineFactory.create("mongodb://localhost:27017")
        assert isinstance(engine, MongoEngine)

    def test_create_mongo_srv(self):
        from db_wizard.engine import EngineFactory
        from db_wizard.engines.mongo import MongoEngine
        engine = EngineFactory.create("mongodb+srv://user:pass@cluster.example.com")
        assert isinstance(engine, MongoEngine)

    def test_create_mysql_from_uri(self):
        from db_wizard.engine import EngineFactory
        from db_wizard.engines.mysql import MySQLEngine
        engine = EngineFactory.create("mysql://root@localhost/db")
        assert isinstance(engine, MySQLEngine)

    def test_create_unknown_raises(self):
        from db_wizard.engine import EngineFactory
        with pytest.raises(ValueError, match="Unsupported"):
            EngineFactory.create("unknown://localhost/db")

    def test_detect_scheme_mongodb(self):
        from db_wizard.engine import EngineFactory
        assert EngineFactory.detect_scheme("mongodb://localhost") == "mongodb"
        assert EngineFactory.detect_scheme("mongodb+srv://cluster.com") == "mongodb"

    def test_detect_scheme_mysql(self):
        from db_wizard.engine import EngineFactory
        assert EngineFactory.detect_scheme("mysql://root@localhost") == "mysql"

    def test_detect_scheme_unknown_raises(self):
        from db_wizard.engine import EngineFactory
        with pytest.raises(ValueError):
            EngineFactory.detect_scheme("unknown://localhost")

    def test_check_same_engine_ok(self):
        from db_wizard.engine import EngineFactory
        source = EngineFactory.create("mongodb://localhost")
        target = EngineFactory.create("mongodb://remote")
        # Should not raise
        EngineFactory.check_same_engine(source, target)

    def test_check_same_engine_mysql_ok(self):
        from db_wizard.engine import EngineFactory
        source = EngineFactory.create("mysql://root@host1/db")
        target = EngineFactory.create("mysql://root@host2/db")
        # Should not raise
        EngineFactory.check_same_engine(source, target)

    def test_check_same_engine_cross_raises(self):
        from db_wizard.engine import EngineFactory
        source = EngineFactory.create("mongodb://localhost")
        target = EngineFactory.create("mysql://root@localhost/db")
        with pytest.raises(ValueError, match="Cross-engine"):
            EngineFactory.check_same_engine(source, target)

    def test_check_same_engine_cross_reverse(self):
        from db_wizard.engine import EngineFactory
        source = EngineFactory.create("mysql://root@localhost/db")
        target = EngineFactory.create("mongodb://localhost")
        with pytest.raises(ValueError, match="Cross-engine"):
            EngineFactory.check_same_engine(source, target)


# ===========================================================================
# MongoEngine ABC compliance
# ===========================================================================

class TestMongoEngineABC:
    """Test MongoEngine implements DatabaseEngine correctly"""

    def test_is_subclass(self):
        from db_wizard.engine import DatabaseEngine
        from db_wizard.engines.mongo import MongoEngine
        assert issubclass(MongoEngine, DatabaseEngine)

    def test_properties(self):
        from db_wizard.engines.mongo import MongoEngine
        engine = MongoEngine("mongodb://localhost:27017")
        assert engine.table_term == "collection"
        assert engine.table_term_plural == "collections"
        assert engine.scheme == "mongodb"

    def test_has_all_abc_methods(self):
        """MongoEngine must implement every abstract method"""
        import inspect
        from db_wizard.engine import DatabaseEngine
        from db_wizard.engines.mongo import MongoEngine

        abstract = {
            name for name, method in inspect.getmembers(DatabaseEngine)
            if getattr(method, '__isabstractmethod__', False)
        }
        implemented = set(dir(MongoEngine))
        missing = abstract - implemented
        assert not missing, f"MongoEngine missing abstract methods: {missing}"

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_connect_returns_self(self, mock_client_cls):
        from db_wizard.engines.mongo import MongoEngine
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        engine = MongoEngine("mongodb://localhost:27017")
        result = engine.connect()
        assert result is engine

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_close(self, mock_client_cls):
        from db_wizard.engines.mongo import MongoEngine
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        engine = MongoEngine("mongodb://localhost:27017")
        engine.connect()
        engine.close()
        mock_client.close.assert_called_once()
        assert engine.client is None

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_test_connection_success(self, mock_client_cls):
        from db_wizard.engines.mongo import MongoEngine
        mock_client = MagicMock()
        mock_client.list_database_names.return_value = ['db1', 'db2']
        mock_client_cls.return_value = mock_client
        engine = MongoEngine("mongodb://localhost:27017")
        ok, msg = engine.test_connection()
        assert ok is True
        assert "2" in msg

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_test_connection_failure(self, mock_client_cls):
        from db_wizard.engines.mongo import MongoEngine
        mock_client_cls.side_effect = Exception("Connection refused")
        engine = MongoEngine("mongodb://localhost:27017")
        ok, msg = engine.test_connection()
        assert ok is False
        assert "refused" in msg.lower()

    def test_build_connection_uri(self):
        from db_wizard.engines.mongo import MongoEngine
        uri = MongoEngine.build_connection_uri("server.com", port=27018, username="admin", password="pass")
        assert uri == "mongodb://admin:pass@server.com:27018/"

    @patch('db_wizard.engines.mongo.MongoClient')
    def test_check_tools(self, mock_client_cls):
        from db_wizard.engines.mongo import MongoEngine
        engine = MongoEngine("mongodb://localhost:27017")
        tools = engine.check_tools()
        assert 'mongodump' in tools
        assert 'mongorestore' in tools
        assert 'mongosh' in tools


# ===========================================================================
# Settings migration + tunnel host support
# ===========================================================================

class TestSettingsMigration:
    """Test auto-migration from old config path"""

    def test_migration_copies_old_to_new(self):
        from db_wizard.settings import SettingsManager

        with tempfile.TemporaryDirectory() as tmpdir:
            old_path = Path(tmpdir) / '.mongo_wizard_settings.json'
            new_path = Path(tmpdir) / '.db_wizard_settings.json'

            # Create old config
            old_data = {"hosts": {"prod": "mongodb://prod:27017"}, "tasks": {}, "storages": {}}
            old_path.write_text(json.dumps(old_data))

            # Patch paths and create manager
            with patch('db_wizard.settings.CONFIG_FILE', new_path), \
                 patch('db_wizard.settings.OLD_CONFIG_FILE', old_path):
                mgr = SettingsManager()

            # New file should exist with old data
            assert new_path.exists()
            assert mgr.settings['hosts']['prod'] == "mongodb://prod:27017"

    def test_migration_skips_if_new_exists(self):
        from db_wizard.settings import SettingsManager

        with tempfile.TemporaryDirectory() as tmpdir:
            old_path = Path(tmpdir) / '.mongo_wizard_settings.json'
            new_path = Path(tmpdir) / '.db_wizard_settings.json'

            old_data = {"hosts": {"old": "mongodb://old"}, "tasks": {}, "storages": {}}
            new_data = {"hosts": {"new": "mongodb://new"}, "tasks": {}, "storages": {}}
            old_path.write_text(json.dumps(old_data))
            new_path.write_text(json.dumps(new_data))

            with patch('db_wizard.settings.CONFIG_FILE', new_path), \
                 patch('db_wizard.settings.OLD_CONFIG_FILE', old_path):
                mgr = SettingsManager()

            # Should use new, not overwrite with old
            assert mgr.settings['hosts'].get('new') == "mongodb://new"
            assert 'old' not in mgr.settings['hosts']

    def test_migration_does_nothing_if_no_old(self):
        from db_wizard.settings import SettingsManager

        with tempfile.TemporaryDirectory() as tmpdir:
            old_path = Path(tmpdir) / '.mongo_wizard_settings.json'
            new_path = Path(tmpdir) / '.db_wizard_settings.json'

            with patch('db_wizard.settings.CONFIG_FILE', new_path), \
                 patch('db_wizard.settings.OLD_CONFIG_FILE', old_path):
                mgr = SettingsManager()

            # Fresh empty settings
            assert mgr.settings == {"hosts": {}, "tasks": {}, "storages": {}}


class TestSettingsTunnelHost:
    """Test host storage with SSH tunnel config"""

    def _make_manager(self):
        from db_wizard.settings import SettingsManager
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'settings.json'
            with patch('db_wizard.settings.CONFIG_FILE', path), \
                 patch('db_wizard.settings.OLD_CONFIG_FILE', Path(tmpdir) / 'old.json'):
                mgr = SettingsManager()
                mgr._tmpdir = tmpdir  # Keep ref so dir doesn't get deleted
                return mgr, path

    def test_add_host_simple_string(self):
        mgr, _ = self._make_manager()
        mgr.add_host("local", "mongodb://localhost:27017")
        assert mgr.get_host("local") == "mongodb://localhost:27017"
        assert mgr.get_host_uri("local") == "mongodb://localhost:27017"
        assert mgr.get_host_tunnel("local") is None

    def test_add_host_with_string_tunnel(self):
        mgr, _ = self._make_manager()
        mgr.add_host("remote", "mysql://user:pass@localhost:3306/db", ssh_tunnel="myserver")
        host = mgr.get_host("remote")
        assert isinstance(host, dict)
        assert host['uri'] == "mysql://user:pass@localhost:3306/db"
        assert host['ssh_tunnel'] == "myserver"
        assert mgr.get_host_uri("remote") == "mysql://user:pass@localhost:3306/db"
        assert mgr.get_host_tunnel("remote") == "myserver"

    def test_add_host_with_dict_tunnel(self):
        mgr, _ = self._make_manager()
        tunnel_config = {"host": "server.com", "user": "deploy", "port": 22}
        mgr.add_host("remote", "mysql://user:pass@localhost:3306/db", ssh_tunnel=tunnel_config)
        assert mgr.get_host_tunnel("remote") == tunnel_config

    def test_get_host_uri_from_dict(self):
        mgr, _ = self._make_manager()
        mgr.add_host("x", "mysql://a@b/c", ssh_tunnel="jump")
        assert mgr.get_host_uri("x") == "mysql://a@b/c"

    def test_get_host_nonexistent(self):
        mgr, _ = self._make_manager()
        assert mgr.get_host("nope") is None
        assert mgr.get_host_uri("nope") is None
        assert mgr.get_host_tunnel("nope") is None

    def test_list_hosts_mixed(self):
        mgr, _ = self._make_manager()
        mgr.add_host("a", "mongodb://localhost")
        mgr.add_host("b", "mysql://root@host/db", ssh_tunnel="jump")
        hosts = mgr.list_hosts()
        assert len(hosts) == 2
        assert isinstance(hosts['a'], str)
        assert isinstance(hosts['b'], dict)


# ===========================================================================
# SSH Tunnel
# ===========================================================================

class TestSSHTunnel:
    """Test tunnel.py - mocked, no real SSH needed"""

    def test_default_port_for_scheme(self):
        from db_wizard.tunnel import _default_port_for_scheme
        assert _default_port_for_scheme("mongodb") == 27017
        assert _default_port_for_scheme("mysql") == 3306
        assert _default_port_for_scheme("unknown") == 0

    def test_find_free_port(self):
        from db_wizard.tunnel import _find_free_port
        port = _find_free_port()
        assert 1024 < port < 65536

    @patch('subprocess.Popen')
    @patch('socket.create_connection')
    def test_open_tunnel_simple_hostname(self, mock_socket, mock_popen):
        from db_wizard.tunnel import open_tunnel, _active_tunnels

        # Mock SSH process that exits successfully (forked to background)
        mock_proc = MagicMock()
        mock_proc.wait.return_value = None
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc
        mock_socket.return_value.__enter__ = MagicMock()
        mock_socket.return_value.__exit__ = MagicMock()

        result = open_tunnel("mysql://user:pass@localhost:3306/db", "myserver")

        # Should return a rewritten URI with 127.0.0.1 and a random port
        assert "127.0.0.1" in result
        assert "mysql://" in result
        # SSH command should include the hostname
        cmd = mock_popen.call_args[0][0]
        assert "myserver" in cmd
        assert "-L" in cmd

    @patch('subprocess.Popen')
    @patch('socket.create_connection')
    def test_open_tunnel_dict_config(self, mock_socket, mock_popen):
        from db_wizard.tunnel import open_tunnel

        mock_proc = MagicMock()
        mock_proc.wait.return_value = None
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc
        mock_socket.return_value.__enter__ = MagicMock()
        mock_socket.return_value.__exit__ = MagicMock()

        config = {"host": "server.com", "user": "deploy", "port": 2222, "key_path": "/path/to/key"}
        result = open_tunnel("mysql://user:pass@localhost:3306/db", config)

        assert "127.0.0.1" in result
        cmd = mock_popen.call_args[0][0]
        assert "deploy@server.com" in cmd
        assert "-p" in cmd
        assert "2222" in cmd
        assert "-i" in cmd
        assert "/path/to/key" in cmd

    @patch('subprocess.Popen')
    def test_open_tunnel_ssh_failure(self, mock_popen):
        from db_wizard.tunnel import open_tunnel

        mock_proc = MagicMock()
        mock_proc.wait.return_value = None
        mock_proc.returncode = 1
        mock_proc.stderr.read.return_value = b"Permission denied"
        mock_popen.return_value = mock_proc

        with pytest.raises(ConnectionError, match="Permission denied"):
            open_tunnel("mysql://user:pass@localhost:3306/db", "badhost")

    def test_close_tunnels(self):
        from db_wizard.tunnel import _active_tunnels, close_tunnels
        mock_proc = MagicMock()
        _active_tunnels.append(mock_proc)
        close_tunnels()
        mock_proc.kill.assert_called_once()
        assert len(_active_tunnels) == 0


# ===========================================================================
# mask_password
# ===========================================================================

class TestMaskPassword:
    """Test URI password masking"""

    def test_mysql_uri(self):
        from db_wizard.utils import mask_password
        assert mask_password("mysql://user:secret@host:3306/db") == "mysql://user:****@host:3306/db"

    def test_mongodb_uri(self):
        from db_wizard.utils import mask_password
        assert mask_password("mongodb://admin:s3cret@prod:27017/auth") == "mongodb://admin:****@prod:27017/auth"

    def test_no_password(self):
        from db_wizard.utils import mask_password
        assert mask_password("mongodb://localhost:27017") == "mongodb://localhost:27017"

    def test_no_at_sign(self):
        from db_wizard.utils import mask_password
        assert mask_password("mongodb://localhost") == "mongodb://localhost"

    def test_empty_password(self):
        from db_wizard.utils import mask_password
        # user:@host -> still has : before @
        result = mask_password("mysql://root:@localhost/db")
        assert "root:****@" in result

    def test_complex_password(self):
        from db_wizard.utils import mask_password
        result = mask_password("mysql://user:p%40ss%23w0rd!@host/db")
        assert result == "mysql://user:****@host/db"
        assert "p%40ss" not in result
