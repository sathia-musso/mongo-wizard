from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm, Prompt, IntPrompt
from rich import box
import os

console = Console()

class ManageStoragesFlow:
    """Flow for managing saved storage configurations"""
    def __init__(self, wizard_context):
        self.wizard = wizard_context

    def run(self):
        while True:
            self.wizard.clear_screen()
            console.print(Panel("[bold cyan]🗄️ MANAGE STORAGE CONFIGURATIONS[/bold cyan]", style="cyan"))

            saved_storages = self.wizard.settings_manager.list_storages()
            storage_list = list(saved_storages.items())

            if not storage_list:
                console.print("[yellow]No saved storage configurations yet![/yellow]")
            else:
                table = Table(title="Saved Storage Configs", box=box.ROUNDED)
                table.add_column("#", style="cyan", width=4)
                table.add_column("Name", style="green")
                table.add_column("Type", style="yellow")
                table.add_column("Details", style="magenta")

                for i, (name, config) in enumerate(storage_list, 1):
                    storage_type = config.get('type', 'unknown')

                    if storage_type == 'ssh':
                        details = f"{config.get('user')}@{config.get('host')}:{config.get('path', '/')}"
                    elif storage_type == 'ftp':
                        details = f"{config.get('user')}@{config.get('host')}:{config.get('path', '/')}"
                    elif storage_type == 'local':
                        details = config.get('path', '/')
                    else:
                        details = "Unknown"

                    table.add_row(str(i), name, storage_type.upper(), details)

                console.print(table)

            console.print("\n[bold]Options:[/bold]")
            console.print("  [cyan]1.[/cyan] Add new storage")
            if storage_list:
                console.print("  [cyan]2.[/cyan] Test storage connection")
                console.print("  [cyan]3.[/cyan] Remove storage")
            console.print("  [cyan]4.[/cyan] Back to main menu")

            choices = ["1", "2", "3", "4"] if storage_list else ["1", "4"]
            choice = Prompt.ask("Choose option", choices=choices)

            if choice == "1":
                self._add_storage()
            elif choice == "2" and storage_list:
                self._test_storage(storage_list)
            elif choice == "3" and storage_list:
                self._remove_storage(storage_list)
            elif choice == "4":
                break

    def _add_storage(self):
        console.print("\n[bold]Storage types:[/bold]")
        console.print("  [cyan]1.[/cyan] Local filesystem")
        console.print("  [cyan]2.[/cyan] SSH/SCP")
        console.print("  [cyan]3.[/cyan] FTP")

        storage_type = Prompt.ask("Choose storage type", choices=["1", "2", "3"])
        name = Prompt.ask("\nStorage configuration name")

        if storage_type == "1":
            path = Prompt.ask("Local path", default="/tmp/backups")
            config = {
                "type": "local",
                "name": name,
                "path": path
            }
        elif storage_type == "2":
            host = Prompt.ask("SSH host")
            user = Prompt.ask("SSH user", default="root")
            port = IntPrompt.ask("SSH port", default=22)
            path = Prompt.ask("Remote path", default="/backups")
            key_path = Prompt.ask("SSH key path (optional, press Enter to skip)", default="")

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
        elif storage_type == "3":
            host = Prompt.ask("FTP host")
            user = Prompt.ask("FTP user")
            password = Prompt.ask("FTP password", password=True)
            port = IntPrompt.ask("FTP port", default=21)
            path = Prompt.ask("Remote path", default="/")

            config = {
                "type": "ftp",
                "name": name,
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "path": path
            }

        self.wizard.settings_manager.add_storage(name, config)
        console.print(f"[green]✓ Added storage configuration '{name}'[/green]")

        if Confirm.ask("Test connection now?"):
            self._test_connection(config)

        Prompt.ask("Press Enter to continue")

    def _test_storage(self, storage_list):
        while True:
            try:
                idx = int(Prompt.ask("Select storage to test"))
                if 1 <= idx <= len(storage_list):
                    break
                console.print(f"[red]Please enter a number between 1 and {len(storage_list)}[/red]")
            except ValueError:
                console.print("[red]Please enter a valid number[/red]")

        name, config = storage_list[idx - 1]
        console.print(f"[yellow]Testing {name}...[/yellow]")
        self._test_connection(config)
        Prompt.ask("Press Enter to continue")

    def _test_connection(self, config):
        from ..storage import StorageFactory
        try:
            storage = StorageFactory.create(config)
            if hasattr(storage, 'test_connection'):
                test_path = config.get('path', '/')
                success, msg = storage.test_connection(test_path)
                if success:
                    console.print(f"[green]✓ {msg}[/green]")
                else:
                    console.print(f"[red]✗ {msg}[/red]")
            else:
                path = config.get('path', '/')
                if os.path.exists(path) and os.access(path, os.W_OK):
                    console.print(f"[green]✓ Local path {path} is accessible[/green]")
                else:
                    console.print(f"[red]✗ Local path {path} not accessible[/red]")
        except Exception as e:
            console.print(f"[red]✗ Error: {e}[/red]")

    def _remove_storage(self, storage_list):
        while True:
            try:
                idx = int(Prompt.ask("Select storage to remove"))
                if 1 <= idx <= len(storage_list):
                    break
                console.print(f"[red]Please enter a number between 1 and {len(storage_list)}[/red]")
            except ValueError:
                console.print("[red]Please enter a valid number[/red]")

        name, _ = storage_list[idx - 1]
        if Confirm.ask(f"Remove storage '{name}'?"):
            if self.wizard.settings_manager.delete_storage(name):
                console.print(f"[green]✓ Removed '{name}'[/green]")
        Prompt.ask("Press Enter to continue")
