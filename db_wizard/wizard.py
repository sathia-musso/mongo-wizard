#!/usr/bin/env python
"""
db-wizard - Interactive tool with saved hosts
Supports MongoDB and MySQL via engine abstraction.
"""

import os
from pathlib import Path
from typing import Any

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt, IntPrompt
from rich.table import Table

from .settings import SettingsManager
from .flows._common import GoHome

console = Console()


def check_system_requirements():
    from .system_checks import check_system_requirements as _check_system_requirements
    _check_system_requirements()


class DbWizard:
    """Interactive wizard for database operations (MongoDB, MySQL)"""

    def __init__(self):
        self.settings_manager = SettingsManager()
        self.source_uri = None
        self.target_uri = None

    def clear_screen(self):
        """Clear console screen"""
        console.clear()

    def show_banner(self):
        """Show cool banner"""
        banner = """
╔═══════════════════════════════════════════════════════╗
║                🚀 DB-WIZARD 🚀                        ║
║        Advanced Database Copy & Migration             ║
║ MongoDB | MySQL | PostgreSQL | Redis (via CLI)        ║
╚═══════════════════════════════════════════════════════╝
        """
        console.print(banner)

    def main_menu(self) -> str:
        """Show main menu and get choice"""
        self.clear_screen()
        self.show_banner()

        console.print("\n[bold yellow]MAIN MENU:[/bold yellow]\n")
        console.print("  [cyan]1.[/cyan] 📋 Copy Collection/Database")
        console.print("  [cyan]2.[/cyan] 🚀 Run Saved Task")
        console.print("  [cyan]3.[/cyan] 💾 Manage Saved Hosts")
        console.print("  [cyan]4.[/cyan] ⚙️ Manage Saved Tasks")
        console.print("  [cyan]5.[/cyan] 🗄️ Manage Storage Configs")
        console.print("  [cyan]6.[/cyan] 🔍 Browse Database")
        console.print("  [cyan]7.[/cyan] 📦 Backup Database")
        console.print("  [cyan]8.[/cyan] 📥 Restore Database")
        console.print("  [cyan]9.[/cyan] 🔧 Check System Requirements")
        console.print("  [cyan]0.[/cyan] ❌ Exit")

        choice = Prompt.ask("\n[bold]Choose option", choices=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"])
        return choice

    def _resolve_host(self, host_value: str | dict) -> str:
        """Resolve a host config to a usable URI, opening SSH tunnel if needed."""
        if isinstance(host_value, dict):
            uri = host_value['uri']
            tunnel_config = host_value.get('ssh_tunnel')
            if tunnel_config:
                from .tunnel import open_tunnel
                return open_tunnel(uri, tunnel_config)
            return uri
        return host_value

    def _host_display(self, name: str, host_value: str | dict) -> str:
        """Format host for display in the list."""
        if isinstance(host_value, dict):
            uri = host_value['uri']
            tunnel = host_value.get('ssh_tunnel', '')
            # Detect engine type
            scheme = "MySQL" if 'mysql://' in uri else "PostgreSQL" if 'postgres' in uri else "Redis" if 'redis' in uri else "MongoDB"
            tunnel_label = f" via SSH:{tunnel}" if isinstance(tunnel, str) else f" via SSH:{tunnel.get('host', '?')}" if tunnel else ""
            return f"{name} [dim]({scheme}{tunnel_label})[/dim]"
        else:
            uri = host_value
            scheme = "MySQL" if 'mysql://' in uri else "PostgreSQL" if 'postgres' in uri else "Redis" if 'redis' in uri else "MongoDB"
            return f"{name} [dim]({scheme})[/dim]"

    def _get_host_uri(self, host_value: str | dict) -> str:
        """Extract raw URI from a host value (string or dict)."""
        if isinstance(host_value, dict):
            return host_value.get('uri', '')
        return host_value

    def select_or_add_host(self, purpose: str = "source", filter_scheme: str | None = None) -> str:
        """Select saved host or add new one. Returns a usable URI."""
        from .flows.selection import SelectionFlow
        return SelectionFlow(self).select_or_add_host(purpose, filter_scheme)

    def _add_new_host(self) -> str:
        """Interactive flow to add a new host to settings"""
        from .flows.selection import SelectionFlow
        return SelectionFlow(self).add_new_host()

    def select_database(self, engine, purpose: str = "source") -> str:
        """Select database from list. Accepts a DatabaseEngine."""
        from .flows.selection import SelectionFlow
        return SelectionFlow(self).select_database(engine, purpose)

    def select_collection(self, engine, database: str, purpose: str = "source", allow_all: bool = True,
                          allow_multiple: bool = False) -> Any | None:
        """Select collection/table from database. Accepts a DatabaseEngine."""
        from .flows.selection import SelectionFlow
        return SelectionFlow(self).select_collection(engine, database, purpose, allow_all, allow_multiple)

    def copy_wizard(self):
        """Interactive copy wizard"""
        from .flows.copy_flow import CopyWizardFlow
        flow = CopyWizardFlow(self)
        flow.run()

    def manage_hosts(self):
        """Manage saved hosts"""
        from .flows.manage_hosts import ManageHostsFlow
        flow = ManageHostsFlow(self)
        flow.run()

    def manage_storages(self):
        """Manage saved storage configurations"""
        from .flows.manage_storages import ManageStoragesFlow
        flow = ManageStoragesFlow(self)
        flow.run()

    def browse_database(self):
        """Browse database interactively"""
        from .flows.browse_flow import BrowseWizardFlow
        flow = BrowseWizardFlow(self)
        flow.run()

    def run_saved_task(self, task_name: str = None):
        """Run a saved task (interactive selector + confirmation, then delegates to task_runner)"""
        from .task_runner import run_task, display_task_summary

        if not task_name:
            # Show task selector
            self.clear_screen()
            console.print(Panel("[bold cyan]⚙️  RUN SAVED TASK[/bold cyan]", style="cyan"))

            saved_tasks_dict = self.settings_manager.list_tasks()
            saved_tasks_list = list(saved_tasks_dict.items())

            if not saved_tasks_list:
                console.print("[yellow]No saved tasks yet![/yellow]")
                Prompt.ask("Press Enter to continue")
                return

            # Display tasks
            from .utils import format_task_table_row
            table = Table(title="Saved Tasks", box=box.ROUNDED)
            table.add_column("#", style="cyan", width=4)
            table.add_column("Name", style="green")
            table.add_column("Source → Target", style="yellow")
            table.add_column("Collection", style="magenta")

            for i, (name, config) in enumerate(saved_tasks_list, 1):
                _, source_target, coll_display = format_task_table_row(name, config)
                table.add_row(str(i), name, source_target, coll_display)

            console.print(table)

            while True:
                choice_str = Prompt.ask("\nSelect task to run (or 'q' to quit)")
                if choice_str.lower() == 'q':
                    return
                try:
                    choice = int(choice_str)
                    if 1 <= choice <= len(saved_tasks_list):
                        task_name = saved_tasks_list[choice - 1][0]
                        task_config = saved_tasks_list[choice - 1][1]
                        break
                    console.print(f"[red]Please enter a number between 1 and {len(saved_tasks_list)}[/red]")
                except ValueError:
                    console.print("[red]Please enter a valid number[/red]")
        else:
            task_config = self.settings_manager.get_task(task_name)
            if not task_config:
                console.print(f"[red]❌ Task '{task_name}' not found![/red]")
                return

        # Show summary and confirm
        console.print(f"\n[cyan]🚀 Task: {task_name}[/cyan]")
        display_task_summary(task_config)

        if not Confirm.ask("\n[yellow]Execute this task?[/yellow]"):
            return

        # Execute via centralized task runner
        success = run_task(task_config)

        if success:
            console.print("[green]✅ Task completed![/green]")

        Prompt.ask("Press Enter to continue")

    def manage_tasks(self):
        """Manage saved tasks"""
        from .flows.manage_tasks import ManageTasksFlow
        flow = ManageTasksFlow(self)
        flow.run()

    def backup_wizard(self):
        """Wizard for database backup"""
        from .flows.backup_flow import BackupWizardFlow
        flow = BackupWizardFlow(self)
        flow.run()

    def restore_wizard(self):
        """Wizard for database restore"""
        from .flows.restore_flow import RestoreWizardFlow
        flow = RestoreWizardFlow(self)
        flow.run()

    def _prompt_new_storage(self):
        """Prompt for new storage configuration"""
        console.print("  [cyan]1.[/cyan] 💾 Local directory")
        console.print("  [cyan]2.[/cyan] 🌐 SSH/SCP remote server")
        console.print("  [cyan]3.[/cyan] 📡 FTP server")

        storage_choice = Prompt.ask("\nChoose storage type", choices=["1", "2", "3"])

        if storage_choice == "1":
            # Local directory
            default_dir = os.path.join(os.path.expanduser("~"), "db_backups")
            storage_path = Prompt.ask("Backup directory", default=default_dir)
            Path(storage_path).mkdir(parents=True, exist_ok=True)

            # Ask to save config
            if Confirm.ask("Save this storage configuration?"):
                name = Prompt.ask("Configuration name")
                config = {
                    "type": "local",
                    "name": name,
                    "path": storage_path
                }
                self.settings_manager.add_storage(name, config)
                console.print(f"[green]✓ Saved storage config '{name}'[/green]")
                return config
            return storage_path

        elif storage_choice == "2":
            # SSH/SCP
            host = Prompt.ask("SSH host")
            user = Prompt.ask("SSH user", default="root")
            port = IntPrompt.ask("SSH port", default=22)
            path = Prompt.ask("Remote path", default="/backups")
            key_path = Prompt.ask("SSH key path (optional, press Enter to skip)", default="")

            # Ask to save config
            if Confirm.ask("Save this storage configuration?"):
                name = Prompt.ask("Configuration name")
                config = {
                    "type": "ssh",
                    "name": name,
                    "host": host,
                    "user": user,
                    "port": port,
                    "path": path
                }
                if key_path:
                    config["key_path"] = key_path
                self.settings_manager.add_storage(name, config)
                console.print(f"[green]✓ Saved storage config '{name}'[/green]")
                return config

            storage_url = f"ssh://{user}@{host}:{port}{path}"
            return storage_url

        elif storage_choice == "3":
            # FTP
            host = Prompt.ask("FTP host")
            user = Prompt.ask("FTP user")
            password = Prompt.ask("FTP password", password=True)
            port = IntPrompt.ask("FTP port", default=21)
            path = Prompt.ask("Remote path", default="/")

            # Ask to save config
            if Confirm.ask("Save this storage configuration?"):
                name = Prompt.ask("Configuration name")
                config = {
                    "type": "ftp",
                    "name": name,
                    "host": host,
                    "user": user,
                    "password": password,
                    "port": port,
                    "path": path
                }
                self.settings_manager.add_storage(name, config)
                console.print(f"[green]✓ Saved storage config '{name}'[/green]")
                return config

            storage_url = f"ftp://{user}:{password}@{host}:{port}{path}"
            return storage_url

    def run(self):
        """Main wizard loop. GoHome exception from any sub-menu returns here."""
        while True:
            try:
                choice = self.main_menu()

                if choice == "1":
                    self.copy_wizard()
                elif choice == "2":
                    self.run_saved_task()
                elif choice == "3":
                    self.manage_hosts()
                elif choice == "4":
                    self.manage_tasks()
                elif choice == "5":
                    self.manage_storages()
                elif choice == "6":
                    self.browse_database()
                elif choice == "7":
                    self.backup_wizard()
                elif choice == "8":
                    self.restore_wizard()
                elif choice == "9":
                    self.clear_screen()
                    check_system_requirements()
                    Prompt.ask("\nPress Enter to continue")
                elif choice == "0":
                    console.print("\n[bold green]👋 Goodbye![/bold green]")
                    break
            except GoHome:
                # User typed 'x' somewhere - silently return to main menu
                continue


