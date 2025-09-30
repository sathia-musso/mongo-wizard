"""
Test settings manager functionality
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from mongo_wizard.settings import SettingsManager


class TestSettingsManager:
    """Test SettingsManager class"""

    @patch('mongo_wizard.settings.Path.exists')
    @patch('mongo_wizard.settings.open', new_callable=mock_open, read_data='{"hosts": {}, "tasks": {}, "storages": {}}')
    def test_load_settings_existing(self, mock_file, mock_exists):
        """Test loading existing settings file"""
        mock_exists.return_value = True

        manager = SettingsManager()

        assert manager.settings == {"hosts": {}, "tasks": {}, "storages": {}}
        mock_file.assert_called()

    @patch('mongo_wizard.settings.Path.exists')
    def test_load_settings_new_file(self, mock_exists):
        """Test creating new settings when file doesn't exist"""
        mock_exists.return_value = False

        manager = SettingsManager()

        assert manager.settings == {"hosts": {}, "tasks": {}, "storages": {}}

    @patch('mongo_wizard.settings.open', new_callable=mock_open)
    @patch('mongo_wizard.settings.Path.exists')
    def test_save_settings(self, mock_exists, mock_file):
        """Test saving settings to file"""
        mock_exists.return_value = False
        manager = SettingsManager()
        manager.settings = {"hosts": {"test": "mongodb://test"}}

        manager.save_settings()

        mock_file.assert_called()
        handle = mock_file()
        written = ''.join(call.args[0] for call in handle.write.call_args_list)
        data = json.loads(written)
        assert data == {"hosts": {"test": "mongodb://test"}}

    @patch('mongo_wizard.settings.Path.exists')
    def test_add_host(self, mock_exists):
        """Test adding a host"""
        mock_exists.return_value = False
        manager = SettingsManager()

        with patch.object(manager, 'save_settings') as mock_save:
            manager.add_host("production", "mongodb://prod:27017")

            assert manager.settings['hosts']['production'] == "mongodb://prod:27017"
            mock_save.assert_called_once()

    @patch('mongo_wizard.settings.Path.exists')
    def test_get_host(self, mock_exists):
        """Test getting a host"""
        mock_exists.return_value = False
        manager = SettingsManager()
        manager.settings = {"hosts": {"test": "mongodb://test:27017"}}

        result = manager.get_host("test")
        assert result == "mongodb://test:27017"

        result = manager.get_host("nonexistent")
        assert result is None

    @patch('mongo_wizard.settings.Path.exists')
    def test_delete_host(self, mock_exists):
        """Test deleting a host"""
        mock_exists.return_value = False
        manager = SettingsManager()
        manager.settings = {"hosts": {"test": "mongodb://test:27017"}}

        with patch.object(manager, 'save_settings') as mock_save:
            result = manager.delete_host("test")

            assert result is True
            assert "test" not in manager.settings['hosts']
            mock_save.assert_called_once()

            result = manager.delete_host("nonexistent")
            assert result is False

    @patch('mongo_wizard.settings.Path.exists')
    def test_add_task(self, mock_exists):
        """Test adding a task"""
        mock_exists.return_value = False
        manager = SettingsManager()

        task_config = {
            "source_uri": "mongodb://source:27017",
            "target_uri": "mongodb://target:27017",
            "source_db": "test",
            "target_db": "test_backup"
        }

        with patch.object(manager, 'save_settings') as mock_save:
            manager.add_task("daily_backup", task_config)

            assert manager.settings['tasks']['daily_backup'] == task_config
            mock_save.assert_called_once()

    @patch('mongo_wizard.settings.Path.exists')
    def test_get_task(self, mock_exists):
        """Test getting a task"""
        mock_exists.return_value = False
        manager = SettingsManager()

        task_config = {"source_db": "test"}
        manager.settings = {"tasks": {"test_task": task_config}}

        result = manager.get_task("test_task")
        assert result == task_config

        result = manager.get_task("nonexistent")
        assert result is None

    @patch('mongo_wizard.settings.Path.exists')
    def test_list_hosts(self, mock_exists):
        """Test listing all hosts"""
        mock_exists.return_value = False
        manager = SettingsManager()

        hosts = {
            "local": "mongodb://localhost:27017",
            "prod": "mongodb://prod:27017"
        }
        manager.settings = {"hosts": hosts}

        result = manager.list_hosts()
        assert result == hosts

    @patch('mongo_wizard.settings.Path.exists')
    def test_list_tasks(self, mock_exists):
        """Test listing all tasks"""
        mock_exists.return_value = False
        manager = SettingsManager()

        tasks = {
            "backup": {"source_db": "test"},
            "sync": {"source_db": "prod"}
        }
        manager.settings = {"tasks": tasks}

        result = manager.list_tasks()
        assert result == tasks