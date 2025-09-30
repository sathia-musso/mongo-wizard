"""
Settings manager for MongoDB Wizard
"""

import json
from pathlib import Path
# No typing imports needed anymore with Python 3.10+

from rich.console import Console

console = Console()

# Config file path
CONFIG_FILE = Path.home() / '.mongo_wizard_settings.json'


class SettingsManager:
    """Manages saved hosts and tasks"""

    def __init__(self):
        self.config_file = CONFIG_FILE
        self.settings = self.load_settings()

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
        """Save settings to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.settings, f, indent=2)
        except Exception as e:
            console.print(f"[red]❌ Error saving settings: {e}[/red]")

    def add_host(self, name: str, uri: str):
        """Add or update a saved host"""
        self.settings['hosts'][name] = uri
        self.save_settings()

    def get_host(self, name: str) -> str | None:
        """Get a saved host URI"""
        return self.settings.get('hosts', {}).get(name)

    def list_hosts(self) -> dict[str, str]:
        """Get all saved hosts"""
        return self.settings.get('hosts', {})

    def delete_host(self, name: str) -> bool:
        """Delete a saved host"""
        if name in self.settings.get('hosts', {}):
            del self.settings['hosts'][name]
            self.save_settings()
            return True
        return False

    def add_task(self, name: str, config: dict):
        """Add or update a saved task"""
        if 'tasks' not in self.settings:
            self.settings['tasks'] = {}
        self.settings['tasks'][name] = config
        self.save_settings()

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
    def add_storage(self, name: str, config: dict):
        """Add or update a storage configuration"""
        if 'storages' not in self.settings:
            self.settings['storages'] = {}
        self.settings['storages'][name] = config
        self.save_settings()

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