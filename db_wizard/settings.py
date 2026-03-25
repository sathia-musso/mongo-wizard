"""
Settings manager for db-wizard
Handles saved hosts, tasks, and storage configurations.
"""

import json
import os
import shutil
import stat
from pathlib import Path

from rich.console import Console

console = Console()

# Config file paths
CONFIG_FILE = Path.home() / '.db_wizard_settings.json'
OLD_CONFIG_FILE = Path.home() / '.mongo_wizard_settings.json'


class SettingsManager:
    """Manages saved hosts, tasks, and storage configurations."""

    def __init__(self):
        self.config_file = CONFIG_FILE
        # Auto-migrate from old config path
        self._migrate_old_config()
        self.settings = self.load_settings()

    def _migrate_old_config(self):
        """One-time migration from mongo_wizard settings to db_wizard."""
        if OLD_CONFIG_FILE.exists() and not self.config_file.exists():
            try:
                shutil.copy2(OLD_CONFIG_FILE, self.config_file)
                console.print(f"[dim]Migrated settings from {OLD_CONFIG_FILE} to {self.config_file}[/dim]")
            except Exception as e:
                console.print(f"[yellow]⚠ Could not migrate old settings: {e}[/yellow]")

    def load_settings(self) -> dict:
        """Load settings from file"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                console.print(f"[yellow]⚠ Error loading settings: {e}[/yellow]")
                return {"hosts": {}, "tasks": {}, "storages": {}}
        return {"hosts": {}, "tasks": {}, "storages": {}}

    def save_settings(self):
        """Save settings to file with restricted permissions (600)
        because it may contain passwords and connection URIs."""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.settings, f, indent=2)
            # Restrict permissions: owner read/write only (like ~/.ssh/config)
            os.chmod(self.config_file, stat.S_IRUSR | stat.S_IWUSR)
        except Exception as e:
            console.print(f"[red]❌ Error saving settings: {e}[/red]")

    def add_host(self, name: str, uri: str, ssh_tunnel: str | dict | None = None) -> bool:
        """Add or update a saved host.

        Args:
            name: Host display name
            uri: Database URI (mongodb:// or mysql://)
            ssh_tunnel: Optional SSH tunnel config. Either:
                - A string hostname (uses ~/.ssh/config, e.g. "myserver")
                - A dict with {host, user, port, key_path}
        """
        if ssh_tunnel:
            self.settings['hosts'][name] = {'uri': uri, 'ssh_tunnel': ssh_tunnel}
        else:
            self.settings['hosts'][name] = uri
        self.save_settings()
        return True

    def get_host(self, name: str) -> str | dict | None:
        """Get a saved host. Returns URI string or dict with {uri, ssh_tunnel}."""
        return self.settings.get('hosts', {}).get(name)

    def get_host_uri(self, name: str) -> str | None:
        """Get just the URI for a host (unwrap dict if needed)."""
        host = self.get_host(name)
        if host is None:
            return None
        if isinstance(host, dict):
            return host.get('uri')
        return host

    def get_host_tunnel(self, name: str) -> str | dict | None:
        """Get SSH tunnel config for a host, or None."""
        host = self.get_host(name)
        if isinstance(host, dict):
            return host.get('ssh_tunnel')
        return None

    def list_hosts(self) -> dict:
        """Get all saved hosts. Values can be str or dict."""
        return self.settings.get('hosts', {})

    def delete_host(self, name: str) -> bool:
        """Delete a saved host"""
        if name in self.settings.get('hosts', {}):
            del self.settings['hosts'][name]
            self.save_settings()
            return True
        return False

    def add_task(self, name: str, config: dict) -> bool:
        """Add or update a saved task"""
        if 'tasks' not in self.settings:
            self.settings['tasks'] = {}
        self.settings['tasks'][name] = config
        self.save_settings()
        return True

    def get_task(self, name: str) -> dict | None:
        """Get a saved task configuration"""
        return self.settings.get('tasks', {}).get(name)

    def list_tasks(self) -> dict[str, dict]:
        """Get all saved tasks"""
        return self.settings.get('tasks', {})

    def delete_task(self, name: str) -> bool:
        """Delete a saved task"""
        if name in self.settings.get('tasks', {}):
            del self.settings['tasks'][name]
            self.save_settings()
            return True
        return False

    # Storage management
    def add_storage(self, name: str, config: dict) -> bool:
        """Add or update a storage configuration"""
        if 'storages' not in self.settings:
            self.settings['storages'] = {}
        self.settings['storages'][name] = config
        self.save_settings()
        return True

    def get_storage(self, name: str) -> dict | None:
        """Get a storage configuration by name"""
        return self.settings.get('storages', {}).get(name)

    def list_storages(self) -> dict[str, dict]:
        """Get all storage configurations"""
        return self.settings.get('storages', {})

    def delete_storage(self, name: str) -> bool:
        """Delete a storage configuration"""
        if name in self.settings.get('storages', {}):
            del self.settings['storages'][name]
            self.save_settings()
            return True
        return False
