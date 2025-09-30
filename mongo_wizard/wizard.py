#!/usr/bin/env python
"""
mongo-wizard - Interactive tool with saved hosts
"""

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import click
from pymongo import MongoClient
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm, Prompt, IntPrompt
from rich.table import Table

from .settings import SettingsManager
from .utils import test_connection, check_mongodb_tools
from .backup import BackupManager, BackupTask
from .formatting import format_number

console = Console()

# Config file path - renamed to settings.json
CONFIG_FILE = Path.home() / '.mongo_wizard_settings.json'


def check_system_requirements():
    """Check system requirements at startup"""
    console.print("[cyan]🔍 Checking system requirements...[/cyan]\n")

    # Check MongoDB tools
    tools = check_mongodb_tools()

    all_good = True
    requirements_table = Table(title="System Requirements", box=box.ROUNDED)
    requirements_table.add_column("Component", style="cyan")
    requirements_table.add_column("Status", style="green")
    requirements_table.add_column("Notes", style="yellow")

    # Check Python packages
    try:
        import pymongo
        requirements_table.add_row("PyMongo", "✅ Installed", f"v{pymongo.version}")
    except ImportError:
        requirements_table.add_row("PyMongo", "❌ Missing", "pip install pymongo")
        all_good = False

    try:
        import rich
        # Rich doesn't have __version__, try to get it from metadata
        try:
            import importlib.metadata
            rich_version = importlib.metadata.version('rich')
            requirements_table.add_row("Rich", "✅ Installed", f"v{rich_version}")
        except Exception:
            requirements_table.add_row("Rich", "✅ Installed", "")
    except ImportError:
        requirements_table.add_row("Rich", "❌ Missing", "pip install rich")
        all_good = False

    # Check MongoDB tools
    if tools['mongodump']:
        try:
            result = subprocess.run(['mongodump', '--version'], capture_output=True, text=True)
            version = result.stdout.split('\n')[0] if result.stdout else "Unknown version"
            requirements_table.add_row("mongodump", "✅ Installed", version[:30])
        except Exception:
            requirements_table.add_row("mongodump", "✅ Installed", "")
    else:
        requirements_table.add_row("mongodump", "⚠️  Missing", "Optional for backup")

    if tools['mongorestore']:
        requirements_table.add_row("mongorestore", "✅ Installed", "")
    else:
        requirements_table.add_row("mongorestore", "⚠️  Missing", "Optional for restore")

    if tools['mongosh']:
        requirements_table.add_row("MongoDB Shell", "✅ Installed", "")
    else:
        requirements_table.add_row("MongoDB Shell", "⚠️  Missing", "Optional")

    console.print(requirements_table)

    # Show warnings if tools missing
    if not tools['mongodump'] or not tools['mongorestore']:
        console.print("\n[yellow]⚠️  MongoDB Database Tools not fully installed![/yellow]")
        console.print("[dim]Some advanced features (mongodump/mongorestore) will not be available.[/dim]")
        console.print("\nTo install MongoDB Database Tools:")

        if sys.platform == "darwin":
            console.print("  [cyan]macOS:[/cyan] brew install mongodb-database-tools")
        elif sys.platform == "linux":
            console.print("  [cyan]Linux:[/cyan] apt-get install mongodb-database-tools")
        else:
            console.print("  [cyan]Download from:[/cyan] https://www.mongodb.com/try/download/database-tools")

        console.print("\n[dim]Note: Basic copy operations will still work using PyMongo.[/dim]")

    if not all_good:
        console.print("\n[red]❌ Missing required Python packages![/red]")
        console.print("Run: [cyan]pip install pymongo rich[/cyan]")
        sys.exit(1)

    console.print("\n[green]✅ All required components are installed![/green]")

    if not tools['mongodump'] or not tools['mongorestore']:
        if not Confirm.ask("\n[yellow]Continue without MongoDB Database Tools?[/yellow]"):
            sys.exit(0)
    else:
        Prompt.ask("\nPress Enter to continue")

    console.clear()


class MongoWizard:
    """Interactive wizard for MongoDB operations"""

    def __init__(self):
        self.settings_manager = SettingsManager()
        self.source_client = None
        self.target_client = None
        self.source_uri = None
        self.target_uri = None

    def clear_screen(self):
        """Clear console screen"""
        console.clear()

    def show_banner(self):
        """Show cool banner"""
        banner = """
╔══════════════════════════════════════════════╗
║         🚀 MONGO-WIZARD 🚀                   ║
║     Advanced MongoDB Copy & Migration        ║
╚══════════════════════════════════════════════╝
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

    def select_or_add_host(self, purpose: str = "source") -> str:
        """Select saved host or add new one"""
        self.clear_screen()
        console.print(f"\n[bold cyan]Select {purpose.upper()} host:[/bold cyan]\n")

        saved_hosts_dict = self.settings_manager.list_hosts()  # Returns Dict[str, str]
        saved_hosts_list = list(saved_hosts_dict.items())  # Convert to list of (name, uri) tuples

        # Add localhost as default option
        console.print("  [green]0.[/green] 🏠 localhost (mongodb://localhost:27017)")

        # Show saved hosts with connection status
        if saved_hosts_list:
            console.print("\n[dim]Checking saved hosts...[/dim]")
            for i, (host_name, host_uri) in enumerate(saved_hosts_list, 1):
                # Quick connection test
                is_online, _ = test_connection(host_uri, timeout=1000)
                status_icon = "🟢" if is_online else "🔴"

                console.print(f"  [green]{i}.[/green] {status_icon} {host_name}")

        console.print(f"\n  [yellow]{len(saved_hosts_list) + 1}.[/yellow] ➕ Add new host")
        console.print(f"  [yellow]{len(saved_hosts_list) + 2}.[/yellow] ✏️  Enter URI manually")

        max_choice = len(saved_hosts_list) + 2
        choice_str = Prompt.ask("\nChoose option", default="0")
        choice = int(choice_str)

        if choice == 0:
            uri = "mongodb://localhost:27017"
            console.print(f"[yellow]Testing localhost connection...[/yellow]")
            is_online, status = test_connection(uri)
            if is_online:
                console.print(f"[green]✅ Connected to localhost: {status}[/green]")
                return uri
            else:
                console.print(f"[red]❌ Cannot connect to localhost: {status}[/red]")
                if not Confirm.ask("Continue anyway?"):
                    return self.select_or_add_host(purpose)
                return uri

        elif choice <= len(saved_hosts_list):
            host_name, host_uri = saved_hosts_list[choice - 1]
            console.print(f"[yellow]Testing connection to {host_name}...[/yellow]")
            is_online, status = test_connection(host_uri)
            if is_online:
                console.print(f"[green]✅ Connected: {status}[/green]")
                return host_uri
            else:
                console.print(f"[red]❌ Cannot connect: {status}[/red]")
                if Confirm.ask("Try again?"):
                    return self.select_or_add_host(purpose)
                elif Confirm.ask("Use anyway?"):
                    return host_uri
                else:
                    return self.select_or_add_host(purpose)

        elif choice == len(saved_hosts_list) + 1:
            # Add new host
            console.print("\n[bold cyan]Add new host:[/bold cyan]")
            name = Prompt.ask("Host name (e.g., 'production', 'backup')")
            uri = Prompt.ask("MongoDB URI", default="mongodb://localhost:27017")

            # Test connection
            console.print("[yellow]Testing connection...[/yellow]")
            is_online, status = test_connection(uri)
            if is_online:
                console.print(f"[green]✅ Connection successful: {status}[/green]")
                self.settings_manager.add_host(name, uri)
                console.print(f"[green]✅ Saved host '{name}'[/green]")
                return uri
            else:
                console.print(f"[red]❌ Connection failed: {status}[/red]")
                if Confirm.ask("Save anyway?"):
                    self.settings_manager.add_host(name, uri)
                    if Confirm.ask("Use this host now?"):
                        return uri
                return self.select_or_add_host(purpose)

        else:
            # Manual URI
            uri = Prompt.ask("Enter MongoDB URI", default="mongodb://localhost:27017")
            console.print("[yellow]Testing connection...[/yellow]")
            is_online, status = test_connection(uri)
            if is_online:
                console.print(f"[green]✅ Connected: {status}[/green]")
            else:
                console.print(f"[red]❌ Cannot connect: {status}[/red]")
                if not Confirm.ask("Use anyway?"):
                    return self.select_or_add_host(purpose)
            return uri

    def select_database(self, client: MongoClient, purpose: str = "source") -> str:
        """Select database from list"""
        self.clear_screen()
        console.print(f"\n[bold cyan]Select {purpose.upper()} database:[/bold cyan]\n")

        databases = []
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            task = progress.add_task("Loading databases...", total=None)
            db_names = client.list_database_names()

            for db_name in db_names:
                if db_name not in ['admin', 'config', 'local']:
                    db = client[db_name]
                    collections = db.list_collection_names()
                    # Get approximate size
                    stats = db.command("dbStats")
                    size_mb = stats.get('dataSize', 0) / (1024 * 1024)
                    databases.append({
                        'name': db_name,
                        'collections': len(collections),
                        'size_mb': size_mb
                    })
            progress.remove_task(task)

        # Display databases in a nice table
        table = Table(title=f"📚 Available Databases", box=box.ROUNDED)
        table.add_column("#", style="cyan", width=4)
        table.add_column("Database", style="green")
        table.add_column("Collections", style="yellow", justify="right")
        table.add_column("Size (MB)", style="magenta", justify="right")

        for i, db in enumerate(databases, 1):
            table.add_row(
                str(i),
                db['name'],
                str(db['collections']),
                f"{db['size_mb']:.1f}"
            )

        console.print(table)

        if not databases:
            console.print("[yellow]No databases found![/yellow]")
            return Prompt.ask("Enter database name manually")

        while True:
            choice_str = Prompt.ask("\nSelect database")
            try:
                choice = int(choice_str)
                if 1 <= choice <= len(databases):
                    return databases[choice - 1]['name']
                else:
                    console.print(f"[red]Please enter a number between 1 and {len(databases)}[/red]")
            except ValueError:
                console.print("[red]Please enter a valid number[/red]")

    def select_collection(self, client: MongoClient, database: str, purpose: str = "source", allow_all: bool = True,
                          allow_multiple: bool = False) -> Any | None:
        """Select collection from database"""
        self.clear_screen()
        console.print(f"\n[bold cyan]Select {purpose.upper()} collection from {database}:[/bold cyan]\n")

        db = client[database]
        collections = []

        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            task = progress.add_task("Loading collections...", total=None)

            for coll_name in db.list_collection_names():
                coll = db[coll_name]
                count = coll.estimated_document_count()
                # Get indexes count
                indexes = list(coll.list_indexes())
                collections.append({
                    'name': coll_name,
                    'documents': count,
                    'indexes': len(indexes)
                })
            progress.remove_task(task)

        # Sort by document count
        collections.sort(key=lambda x: x['documents'], reverse=True)

        # Display collections
        table = Table(title=f"📁 Collections in {database}", box=box.ROUNDED)
        table.add_column("#", style="cyan", width=4)
        table.add_column("Collection", style="green")
        table.add_column("Documents", style="yellow", justify="right")
        table.add_column("Indexes", style="magenta", justify="right")

        if allow_all:
            table.add_row("0", "[bold]ALL COLLECTIONS[/bold]", format_number(sum(c['documents'] for c in collections)), "-")

        for i, coll in enumerate(collections, 1):
            table.add_row(
                str(i),
                coll['name'],
                f"{format_number(coll['documents'])}",
                str(coll['indexes'])
            )

        console.print(table)

        max_val = len(collections)
        min_val = 0 if allow_all else 1

        if allow_multiple:
            console.print("\n[yellow]💡 Multiple selection mode:[/yellow]")
            console.print("[dim]Enter numbers separated by commas (e.g., 1,3,5) or ranges (e.g., 1-5)[/dim]")
            console.print("[dim]Enter 0 for ALL collections, or leave empty to cancel[/dim]")

            selection = Prompt.ask("\nSelect collections")

            if not selection:
                return []

            if selection == "0":
                return None  # All collections

            selected = []
            try:
                # Parse selection (supports: "1,2,3" or "1-5" or "1,3-5,7")
                parts = selection.split(',')
                for part in parts:
                    part = part.strip()
                    if '-' in part:
                        # Range
                        start, end = part.split('-')
                        start, end = int(start.strip()), int(end.strip())
                        for i in range(start, end + 1):
                            if 1 <= i <= len(collections):
                                selected.append(collections[i - 1]['name'])
                    else:
                        # Single number
                        idx = int(part)
                        if 1 <= idx <= len(collections):
                            selected.append(collections[idx - 1]['name'])

                # Remove duplicates while preserving order
                seen = set()
                selected = [x for x in selected if not (x in seen or seen.add(x))]

                if selected:
                    console.print(
                        f"[green]✅ Selected {len(selected)} collections: {', '.join(selected[:3])}{'...' if len(selected) > 3 else ''}[/green]")
                    return selected
                else:
                    console.print("[red]No valid collections selected[/red]")
                    return []

            except Exception as e:
                console.print(f"[red]Invalid selection: {e}[/red]")
                return []

        else:
            # Single selection mode
            while True:
                choice_str = Prompt.ask("\nSelect collection")
                try:
                    choice = int(choice_str)
                    if min_val <= choice <= max_val:
                        if choice == 0:
                            return None  # All collections
                        return collections[choice - 1]['name']
                    else:
                        console.print(f"[red]Please enter a number between {min_val} and {max_val}[/red]")
                except ValueError:
                    console.print("[red]Please enter a valid number[/red]")

    def copy_wizard(self):
        """Interactive copy wizard"""
        self.clear_screen()
        console.print(Panel("[bold cyan]📋 COPY WIZARD[/bold cyan]", style="cyan"))

        # Step 1: Select source
        console.print("\n[bold]STEP 1: SELECT SOURCE[/bold]")
        self.source_uri = self.select_or_add_host("source")
        self.source_client = MongoClient(self.source_uri)

        source_db = self.select_database(self.source_client, "source")

        # Ask if user wants single or multiple collection selection
        if Confirm.ask("\n[cyan]Do you want to select multiple collections?[/cyan]"):
            source_collection = self.select_collection(self.source_client, source_db, "source", allow_all=True,
                                                       allow_multiple=True)
        else:
            source_collection = self.select_collection(self.source_client, source_db, "source", allow_all=True,
                                                       allow_multiple=False)

        # Step 2: Select target
        console.print("\n[bold]STEP 2: SELECT TARGET[/bold]")
        self.target_uri = self.select_or_add_host("target")
        self.target_client = MongoClient(self.target_uri)

        # Ask if want same or different database name
        if Confirm.ask(f"\n[yellow]Use same database name '{source_db}' on target?[/yellow]", default=True):
            target_db = source_db
        else:
            target_db = Prompt.ask("Enter target database name", default=f"{source_db}_copy")

        target_collection = None
        if source_collection:
            if Confirm.ask(f"[yellow]Use same collection name '{source_collection}' on target?[/yellow]", default=True):
                target_collection = source_collection
            else:
                target_collection = Prompt.ask("Enter target collection name", default=f"{source_collection}_copy")

        # Step 3: Options
        console.print("\n[bold]STEP 3: COPY OPTIONS[/bold]")
        drop_target = Confirm.ask("[yellow]Drop target before copying?[/yellow]", default=False)
        create_backup = False
        if drop_target:
            create_backup = Confirm.ask("[cyan]💾 Create backup before dropping?[/cyan]", default=True)
        copy_indexes = Confirm.ask("[cyan]Copy indexes?[/cyan]", default=True)
        verify = Confirm.ask("[green]Verify after copy?[/green]", default=True)

        # Summary
        self.clear_screen()
        console.print(Panel("[bold green]📋 COPY SUMMARY[/bold green]", style="green"))

        summary = Table(box=box.SIMPLE)
        summary.add_column("Property", style="cyan")
        summary.add_column("Value", style="yellow")

        summary.add_row("Source", f"{self.source_uri}")
        summary.add_row("Source DB", source_db)
        if isinstance(source_collection, list):
            summary.add_row("Source Collections", f"{len(source_collection)} selected")
        else:
            summary.add_row("Source Collection", source_collection or "ALL")
        summary.add_row("", "")
        summary.add_row("Target", f"{self.target_uri}")
        summary.add_row("Target DB", target_db)
        if isinstance(source_collection, list):
            summary.add_row("Target Collections", "Same names as source")
        else:
            summary.add_row("Target Collection", target_collection or "ALL")
        summary.add_row("", "")
        summary.add_row("Drop Target", "YES" if drop_target else "NO")
        if drop_target and create_backup:
            summary.add_row("Create Backup", "YES 💾")
        summary.add_row("Copy Indexes", "YES" if copy_indexes else "NO")
        summary.add_row("Verify", "YES" if verify else "NO")

        console.print(summary)

        # Get document counts for warning
        source_count = None
        target_count = None
        try:
            if source_collection:
                source_count = self.source_client[source_db][source_collection].estimated_document_count()
                if target_collection in self.target_client[target_db].list_collection_names():
                    target_count = self.target_client[target_db][target_collection].estimated_document_count()
            else:
                source_colls = [c for c in self.source_client[source_db].list_collection_names() if
                                not c.startswith('system.')]
                source_count = sum(self.source_client[source_db][c].estimated_document_count() for c in source_colls)
                if target_db in self.target_client.list_database_names():
                    target_colls = [c for c in self.target_client[target_db].list_collection_names() if
                                    not c.startswith('system.')]
                    target_count = sum(
                        self.target_client[target_db][c].estimated_document_count() for c in target_colls)
        except Exception:
            # Silently ignore count errors - not critical for confirmation
            pass

        if drop_target:
            console.print("\n[bold red]⚠️  WARNING: This will DELETE existing target data![/bold red]")
            if source_count is not None:
                console.print(f"[cyan]📊 Source has {format_number(source_count)} documents[/cyan]")
            if target_count is not None and target_count > 0:
                console.print(f"[red]🗑️  Target has {format_number(target_count)} documents that will be DELETED![/red]")
        else:
            # Show counts even without drop
            if source_count is not None:
                console.print(f"\n[cyan]📊 Source: {format_number(source_count)} documents[/cyan]")
            if target_count is not None and target_count > 0:
                console.print(f"[yellow]📊 Target: {format_number(target_count)} existing documents (will merge)[/yellow]")

        if not Confirm.ask("\n[bold yellow]Proceed with copy?[/bold yellow]"):
            console.print("[red]✗ Cancelled[/red]")
            return

        # Execute copy using MongoAdvancedCopier
        console.print("\n[cyan]Starting copy operation...[/cyan]")

        # Import and use the advanced copier from package
        from .core import MongoAdvancedCopier

        copier = MongoAdvancedCopier(self.source_uri, self.target_uri)
        copier.connect()

        try:
            if isinstance(source_collection, list):
                # Multiple collections
                console.print(f"[cyan]📋 Copying {len(source_collection)} collections[/cyan]")

                # Create backup if requested
                if create_backup and drop_target:
                    for coll in source_collection:
                        if coll in copier.target_client[target_db].list_collection_names():
                            copier.backup_before_copy(target_db, coll)

                results = copier.copy_multiple_collections(
                    source_db, target_db,
                    source_collection,
                    drop_target=drop_target,
                    create_backup=False  # Already handled above
                )

                total_docs = sum(r['documents_copied'] for r in results.values())
                total_indexes = sum(r['indexes_created'] for r in results.values())
                console.print(f"[green]✓ Copied {len(results)} collections[/green]")
                console.print(f"[green]✓ Total: {format_number(total_docs)} documents, {total_indexes} indexes[/green]")

            elif source_collection:
                # Single collection
                if create_backup and drop_target and target_collection in copier.target_client[
                    target_db].list_collection_names():
                    copier.backup_before_copy(target_db, target_collection)

                result = copier.copy_collection_with_indexes(
                    source_db, source_collection,
                    target_db, target_collection,
                    drop_target=drop_target
                )
                console.print(f"[green]✓ Copied {format_number(result['documents_copied'])} documents[/green]")
                console.print(f"[green]✓ Created {result['indexes_created']} indexes[/green]")

                if verify:
                    verification = copier.verify_copy(
                        source_db, source_collection,
                        target_db, target_collection
                    )
                    if verification['count_match']:
                        console.print("[green]✓ Verification passed![/green]")
                    else:
                        console.print(
                            f"[yellow]⚠ Count mismatch: {verification['source_count']} vs {verification['target_count']}[/yellow]")

            else:
                # Entire database
                results = copier.copy_entire_database(
                    source_db, target_db,
                    drop_target=drop_target,
                    create_backup=create_backup
                )
                total_docs = sum(r['documents_copied'] for r in results.values())
                console.print(f"[green]✓ Copied {len(results)} collections, {format_number(total_docs)} total documents[/green]")

        except Exception as e:
            console.print(f"[red]✗ Error: {e}[/red]")

        finally:
            copier.close()

        console.print("\n[bold green]✅ COPY COMPLETE![/bold green]")

        # Ask if user wants to save this task
        if Confirm.ask("\n[cyan]💾 Would you like to save this task for future use?[/cyan]"):
            task_name = Prompt.ask("Enter a name for this task")

            task_config = {
                'source_uri': self.source_uri,
                'target_uri': self.target_uri,
                'source_db': source_db,
                'target_db': target_db,
                'source_collection': source_collection,
                'target_collection': target_collection,
                'drop_target': drop_target,
                'copy_indexes': copy_indexes,
                'verify': verify
            }

            if self.settings_manager.add_task(task_name, task_config):
                console.print(f"[green]✅ Task '{task_name}' saved![/green]")
                console.print(f"[dim]Run it anytime with: python mongo_wizard.py --task {task_name}[/dim]")

        Prompt.ask("\nPress Enter to continue")

    def manage_hosts(self):
        """Manage saved hosts"""
        while True:
            self.clear_screen()
            console.print(Panel("[bold cyan]💾 MANAGE SAVED HOSTS[/bold cyan]", style="cyan"))

            saved_hosts_dict = self.settings_manager.list_hosts()  # Returns Dict[str, str]
            saved_hosts_list = list(saved_hosts_dict.items())  # Convert to list of (name, uri) tuples

            if not saved_hosts_list:
                console.print("[yellow]No saved hosts yet![/yellow]")
            else:
                table = Table(title="Saved Hosts", box=box.ROUNDED)
                table.add_column("#", style="cyan", width=4)
                table.add_column("Name", style="green")
                table.add_column("URI", style="yellow")

                for i, (host_name, host_uri) in enumerate(saved_hosts_list, 1):
                    table.add_row(
                        str(i),
                        host_name,
                        host_uri[:50] + '...' if len(host_uri) > 50 else host_uri
                    )

                console.print(table)

            console.print("\n[bold]Options:[/bold]")
            console.print("  [cyan]1.[/cyan] Add new host")
            console.print("  [cyan]2.[/cyan] Remove host")
            console.print("  [cyan]3.[/cyan] Test host connection")
            console.print("  [cyan]4.[/cyan] Back to main menu")

            choice = Prompt.ask("Choose option", choices=["1", "2", "3", "4"])

            if choice == "1":
                # Add host
                name = Prompt.ask("\nHost name")
                uri = Prompt.ask("MongoDB URI", default="mongodb://localhost:27017")
                self.settings_manager.add_host(name, uri)
                console.print(f"[green]✓ Added host '{name}'[/green]")
                Prompt.ask("Press Enter to continue")

            elif choice == "2" and saved_hosts_list:
                # Remove host
                while True:
                    try:
                        idx = int(Prompt.ask("Select host to remove"))
                        if 1 <= idx <= len(saved_hosts_list):
                            break
                        console.print(f"[red]Please enter a number between 1 and {len(saved_hosts_list)}[/red]")
                    except ValueError:
                        console.print("[red]Please enter a valid number[/red]")
                host_name, _ = saved_hosts_list[idx - 1]
                if Confirm.ask(f"Remove '{host_name}'?"):
                    if self.settings_manager.delete_host(host_name):
                        console.print(f"[green]✓ Removed '{host_name}'[/green]")
                Prompt.ask("Press Enter to continue")

            elif choice == "3" and saved_hosts_list:
                # Test connection
                while True:
                    try:
                        idx = int(Prompt.ask("Select host to test"))
                        if 1 <= idx <= len(saved_hosts_list):
                            break
                        console.print(f"[red]Please enter a number between 1 and {len(saved_hosts_list)}[/red]")
                    except ValueError:
                        console.print("[red]Please enter a valid number[/red]")
                host_name, host_uri = saved_hosts_list[idx - 1]
                console.print(f"[yellow]Testing {host_name}...[/yellow]")
                try:
                    client = MongoClient(host_uri, serverSelectionTimeoutMS=3000)
                    client.admin.command('ping')
                    dbs = len(client.list_database_names())
                    client.close()
                    console.print(f"[green]✓ Connection OK! Found {dbs} databases[/green]")
                except Exception as e:
                    console.print(f"[red]✗ Connection failed: {e}[/red]")
                Prompt.ask("Press Enter to continue")

            elif choice == "4":
                break

    def manage_storages(self):
        """Manage saved storage configurations"""
        while True:
            self.clear_screen()
            console.print(Panel("[bold cyan]🗄️ MANAGE STORAGE CONFIGURATIONS[/bold cyan]", style="cyan"))

            saved_storages = self.settings_manager.list_storages()
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

                    # Format details based on type
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
            console.print("  [cyan]2.[/cyan] Test storage connection")
            console.print("  [cyan]3.[/cyan] Remove storage")
            console.print("  [cyan]4.[/cyan] Back to main menu")

            choice = Prompt.ask("Choose option", choices=["1", "2", "3", "4"])

            if choice == "1":
                # Add storage
                console.print("\n[bold]Storage types:[/bold]")
                console.print("  [cyan]1.[/cyan] Local filesystem")
                console.print("  [cyan]2.[/cyan] SSH/SCP")
                console.print("  [cyan]3.[/cyan] FTP")

                storage_type = Prompt.ask("Choose storage type", choices=["1", "2", "3"])

                name = Prompt.ask("\nStorage configuration name")

                if storage_type == "1":
                    # Local storage
                    path = Prompt.ask("Local path", default="/tmp/backups")
                    config = {
                        "type": "local",
                        "name": name,
                        "path": path
                    }

                elif storage_type == "2":
                    # SSH storage
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
                    # FTP storage
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

                self.settings_manager.add_storage(name, config)
                console.print(f"[green]✓ Added storage configuration '{name}'[/green]")

                # Test connection
                if Confirm.ask("Test connection now?"):
                    from .storage import StorageFactory
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
                            console.print("[green]✓ Local storage ready[/green]")
                    except Exception as e:
                        console.print(f"[red]✗ Error: {e}[/red]")

                Prompt.ask("Press Enter to continue")

            elif choice == "2" and storage_list:
                # Test storage
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

                from .storage import StorageFactory
                try:
                    storage = StorageFactory.create(config)
                    if hasattr(storage, 'test_connection'):
                        success, msg = storage.test_connection()
                        if success:
                            console.print(f"[green]✓ {msg}[/green]")
                        else:
                            console.print(f"[red]✗ {msg}[/red]")
                    else:
                        # Local storage - test if path exists/writable
                        import os
                        path = config.get('path', '/')
                        if os.path.exists(path) and os.access(path, os.W_OK):
                            console.print(f"[green]✓ Local path {path} is accessible[/green]")
                        else:
                            console.print(f"[red]✗ Local path {path} not accessible[/red]")
                except Exception as e:
                    console.print(f"[red]✗ Error: {e}[/red]")

                Prompt.ask("Press Enter to continue")

            elif choice == "3" and storage_list:
                # Remove storage
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
                    if self.settings_manager.delete_storage(name):
                        console.print(f"[green]✓ Removed '{name}'[/green]")
                Prompt.ask("Press Enter to continue")

            elif choice == "4":
                break

    def browse_database(self):
        """Browse database interactively"""
        self.clear_screen()
        console.print(Panel("[bold cyan]🔍 DATABASE BROWSER[/bold cyan]", style="cyan"))

        uri = self.select_or_add_host("browse")
        client = MongoClient(uri)

        db_name = self.select_database(client, "browse")
        db = client[db_name]

        while True:
            self.clear_screen()
            console.print(f"[bold cyan]📚 Browsing: {db_name}[/bold cyan]\n")

            coll_name = self.select_collection(client, db_name, "browse", allow_all=False)
            if not coll_name:
                break

            collection = db[coll_name]

            # Show collection info
            console.print(f"\n[bold]📁 Collection: {coll_name}[/bold]")
            console.print(f"Documents: {format_number(collection.estimated_document_count())}")

            # Show sample documents
            if Confirm.ask("\nShow sample documents?"):
                samples = list(collection.find().limit(3))
                for i, doc in enumerate(samples, 1):
                    console.print(f"\n[yellow]Document {i}:[/yellow]")
                    # Pretty print JSON
                    console.print(json.dumps(doc, indent=2, default=str))

            # Show indexes
            if Confirm.ask("\nShow indexes?"):
                indexes = list(collection.list_indexes())
                console.print("\n[cyan]Indexes:[/cyan]")
                for idx in indexes:
                    console.print(f"  - {idx['name']}: {idx['key']}")

            if not Confirm.ask("\n[yellow]Browse another collection?[/yellow]"):
                break

        client.close()

    def run_saved_task(self, task_name: str = None):
        """Run a saved task"""
        if not task_name:
            # Show task selector
            self.clear_screen()
            console.print(Panel("[bold cyan]⚙️  RUN SAVED TASK[/bold cyan]", style="cyan"))

            saved_tasks_dict = self.settings_manager.list_tasks()  # Returns Dict[str, Dict]
            saved_tasks_list = list(saved_tasks_dict.items())  # Convert to list of (name, config) tuples

            if not saved_tasks_list:
                console.print("[yellow]No saved tasks yet![/yellow]")
                Prompt.ask("Press Enter to continue")
                return

            # Display tasks
            table = Table(title="Saved Tasks", box=box.ROUNDED)
            table.add_column("#", style="cyan", width=4)
            table.add_column("Name", style="green")
            table.add_column("Source → Target", style="yellow")
            table.add_column("Collection", style="magenta")

            # Import needed for formatting
            from .utils import format_task_table_row

            for i, (task_name, task_config) in enumerate(saved_tasks_list, 1):
                _, source_target, coll_display = format_task_table_row(task_name, task_config)
                table.add_row(
                    str(i),
                    task_name,
                    source_target,
                    coll_display
                )

            console.print(table)

            while True:
                choice_str = Prompt.ask("\nSelect task to run (or 'q' to quit)")
                if choice_str.lower() == 'q':
                    return
                try:
                    choice = int(choice_str)
                    if 1 <= choice <= len(saved_tasks_list):
                        task_name, task_config = saved_tasks_list[choice - 1]
                        task = task_config
                        task['name'] = task_name  # Add name for compatibility
                        break
                    console.print(f"[red]Please enter a number between 1 and {len(saved_tasks_list)}[/red]")
                except ValueError:
                    console.print("[red]Please enter a valid number[/red]")
        else:
            # Get task by name
            task = self.settings_manager.get_task(task_name)
            if not task:
                console.print(f"[red]❌ Task '{task_name}' not found![/red]")
                return

        # Execute the task based on type
        task_type = task.get('type', 'copy')  # Default to copy for backward compatibility
        console.print(f"\n[cyan]🚀 Running {task_type} task: {task.get('name', 'unnamed')}[/cyan]")

        if task_type == 'backup':
            # Handle backup task
            summary = Table(box=box.SIMPLE)
            summary.add_column("Property", style="cyan")
            summary.add_column("Value", style="yellow")

            summary.add_row("Type", "BACKUP")
            summary.add_row("Source", task['mongo_uri'])
            summary.add_row("Database", task['database'])
            summary.add_row("Collections", str(task.get('collections', 'ALL')))
            summary.add_row("Destination", task['storage_url'])

            console.print(summary)

            if not Confirm.ask("\n[yellow]Execute this backup?[/yellow]"):
                return

            # Execute backup
            backup_mgr = BackupManager(task['mongo_uri'], task['storage_url'])
            result = backup_mgr.backup_database(
                task['database'],
                task.get('collections')
            )

            if result['success']:
                console.print(f"[green]✅ Backup completed![/green]")
                console.print(f"  Size: {result['size_human']}")
                console.print(f"  Documents: {format_number(result['documents'])}")
            else:
                console.print(f"[red]❌ Backup failed: {result.get('error')}[/red]")

            backup_mgr.close()

        elif task_type == 'restore':
            # Handle restore task
            summary = Table(box=box.SIMPLE)
            summary.add_column("Property", style="cyan")
            summary.add_column("Value", style="yellow")

            summary.add_row("Type", "RESTORE")
            summary.add_row("Backup File", task['backup_file'])
            summary.add_row("Target", task['mongo_uri'])
            summary.add_row("Database", task.get('target_database', 'from backup'))
            summary.add_row("Drop Target", "YES" if task.get('drop_target') else "NO")
            summary.add_row("Storage", task['storage_url'])

            console.print(summary)

            if not Confirm.ask("\n[yellow]Execute this restore?[/yellow]"):
                return

            # Execute restore
            backup_mgr = BackupManager(task['mongo_uri'], task['storage_url'])
            result = backup_mgr.restore_database(
                task['backup_file'],
                task.get('target_database'),
                task.get('drop_target', False)
            )

            if result['success']:
                console.print(f"[green]✅ Restore completed![/green]")
                console.print(f"  Database: {result['database']}")
                console.print(f"  Documents: {format_number(result['documents'])}")
            else:
                console.print(f"[red]❌ Restore failed: {result.get('error')}[/red]")

            backup_mgr.close()

        else:
            # Handle copy task (default/legacy)
            summary = Table(box=box.SIMPLE)
            summary.add_column("Property", style="cyan")
            summary.add_column("Value", style="yellow")

            summary.add_row("Type", "COPY")
            summary.add_row("Source", task['source_uri'])
            summary.add_row("Target", task['target_uri'])
            summary.add_row("Database", f"{task['source_db']} → {task['target_db']}")
            if task.get('source_collection'):
                summary.add_row("Collection",
                                f"{task['source_collection']} → {task.get('target_collection', task['source_collection'])}")
            summary.add_row("Drop Target", "YES" if task.get('drop_target') else "NO")
            summary.add_row("Copy Indexes", "YES" if task.get('copy_indexes', True) else "NO")

            console.print(summary)

            if not Confirm.ask("\n[yellow]Execute this task?[/yellow]"):
                return

            # Import and execute
            from .core import MongoAdvancedCopier

            copier = MongoAdvancedCopier(task['source_uri'], task['target_uri'])
            copier.connect()

            try:
                if task.get('source_collection'):
                    result = copier.copy_collection_with_indexes(
                        task['source_db'], task['source_collection'],
                        task['target_db'], task.get('target_collection', task['source_collection']),
                        drop_target=task.get('drop_target', False)
                    )
                    console.print(f"[green]✅ Copied {format_number(result['documents_copied'])} documents[/green]")
                else:
                    results = copier.copy_entire_database(
                        task['source_db'], task['target_db'],
                        drop_target=task.get('drop_target', False)
                    )
                    total_docs = sum(r['documents_copied'] for r in results.values())
                    console.print(f"[green]✅ Copied {len(results)} collections, {format_number(total_docs)} documents[/green]")

            except Exception as e:
                console.print(f"[red]❌ Error: {e}[/red]")
            finally:
                copier.close()

        console.print("[green]✅ Task completed![/green]")
        Prompt.ask("Press Enter to continue")

    def manage_tasks(self):
        """Manage saved tasks"""
        while True:
            self.clear_screen()
            console.print(Panel("[bold cyan]⚙ MANAGE SAVED TASKS[/bold cyan]", style="cyan", expand=False))

            saved_tasks_dict = self.settings_manager.list_tasks()  # Returns Dict[str, Dict]
            saved_tasks_list = list(saved_tasks_dict.items())  # Convert to list of (name, config) tuples

            if not saved_tasks_list:
                console.print("[yellow]No saved tasks yet![/yellow]")
            else:
                # Import needed for formatting
                from .utils import format_task_table_row

                table = Table(title="Saved Tasks", box=box.ROUNDED)
                table.add_column("#", style="cyan", width=4)
                table.add_column("Name", style="green")
                table.add_column("Source → Target", style="yellow")
                table.add_column("Collection", style="magenta")

                for i, (task_name, task_config) in enumerate(saved_tasks_list, 1):
                    _, source_target, coll_display = format_task_table_row(task_name, task_config)
                    table.add_row(
                        str(i),
                        task_name,
                        source_target,
                        coll_display
                    )

                console.print(table)

            console.print("\n[bold]Options:[/bold]")
            console.print("  [cyan]1.[/cyan] Run task")
            console.print("  [cyan]2.[/cyan] Delete task")
            console.print("  [cyan]3.[/cyan] Export tasks to file")
            console.print("  [cyan]4.[/cyan] Back to main menu")

            choice = Prompt.ask("Choose option", choices=["1", "2", "3", "4"])

            if choice == "1" and saved_tasks_list:
                self.run_saved_task()

            elif choice == "2" and saved_tasks_list:
                while True:
                    try:
                        idx = int(Prompt.ask("Select task to delete"))
                        if 1 <= idx <= len(saved_tasks_list):
                            break
                        console.print(f"[red]Please enter a number between 1 and {len(saved_tasks_list)}[/red]")
                    except ValueError:
                        console.print("[red]Please enter a valid number[/red]")

                task_name, _ = saved_tasks_list[idx - 1]
                if Confirm.ask(f"Delete task '{task_name}'?"):
                    if self.settings_manager.delete_task(task_name):
                        console.print(f"[green]✓ Deleted '{task_name}'[/green]")
                Prompt.ask("Press Enter to continue")

            elif choice == "3" and saved_tasks_list:
                filename = Prompt.ask("Export filename", default="mongo_tasks.json")
                with open(filename, 'w') as f:
                    json.dump(saved_tasks_dict, f, indent=2)
                console.print(f"[green]✓ Exported {len(saved_tasks_list)} tasks to {filename}[/green]")
                Prompt.ask("Press Enter to continue")

            elif choice == "4":
                break

    def backup_wizard(self):
        """Wizard for database backup"""
        self.clear_screen()
        console.print("\n[bold cyan]🗄️  DATABASE BACKUP WIZARD[/bold cyan]\n")

        # 1. Select MongoDB source
        source_uri = self.select_or_add_host("backup source")
        if not source_uri:
            return

        # Test connection
        console.print("\n[dim]Testing connection...[/dim]")
        is_connected, msg = test_connection(source_uri)
        if not is_connected:
            console.print(f"[red]❌ Connection failed: {msg}[/red]")
            Prompt.ask("Press Enter to continue")
            return

        # Connect to get databases
        try:
            client = MongoClient(source_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
        except Exception as e:
            console.print(f"[red]Connection error: {e}[/red]")
            Prompt.ask("Press Enter to continue")
            return

        # 2. Select database
        database = self.select_database(client, "backup")
        if not database:
            client.close()
            return

        # 3. Select collections or all
        console.print(f"\n[bold]Backup scope for {database}:[/bold]")
        console.print("  [cyan]1.[/cyan] 📁 Entire database")
        console.print("  [cyan]2.[/cyan] 📄 Specific collections")

        scope_choice = Prompt.ask("Choose", choices=["1", "2"])

        collections = None
        if scope_choice == "2":
            selected = self.select_collection(client, database, "backup", allow_multiple=True)
            if selected and selected != "ALL":
                collections = selected if isinstance(selected, list) else [selected]

        client.close()

        # 4. Select storage destination
        self.clear_screen()
        console.print("\n[bold cyan]📍 STORAGE DESTINATION[/bold cyan]\n")

        # Show saved storage configs first
        saved_storages = self.settings_manager.list_storages()
        storage_list = list(saved_storages.items())

        if storage_list:
            console.print("[bold]Saved storage configs:[/bold]")
            for i, (name, config) in enumerate(storage_list, 1):
                storage_type = config.get('type', 'unknown')
                if storage_type == 'ssh':
                    details = f"{config.get('user')}@{config.get('host')}:{config.get('path', '/')}"
                elif storage_type == 'ftp':
                    details = f"{config.get('user')}@{config.get('host')}:{config.get('path', '/')}"
                else:
                    details = config.get('path', '/')
                console.print(f"  [cyan]{i}.[/cyan] 📁 {name} ({storage_type.upper()}: {details})")

            console.print(f"\n  [cyan]{len(storage_list) + 1}.[/cyan] ➕ Add new storage")

            max_choice = str(len(storage_list) + 1)
            choices = [str(i) for i in range(1, len(storage_list) + 2)]
            choice = Prompt.ask("\nChoose storage", choices=choices)

            if choice == max_choice:
                # Add new storage
                storage_config = self._prompt_new_storage()
                storage_url = storage_config
            else:
                # Use saved storage
                idx = int(choice) - 1
                _, storage_config = storage_list[idx]
                storage_url = storage_config
        else:
            # No saved configs, prompt for new one
            console.print("[yellow]No saved storage configs. Let's create one:[/yellow]\n")
            storage_config = self._prompt_new_storage()
            storage_url = storage_config

        # 5. Ask for backup name (optional - for overwriting same file)
        console.print(f"\n[cyan]📝 BACKUP FILENAME[/cyan]\n")
        console.print("[dim]By default, backups include a timestamp (e.g., 2025_09_30_16_45-mydb.tar.gz)[/dim]")
        console.print("[dim]You can specify a custom name to always overwrite the same file[/dim]\n")

        use_custom_name = Confirm.ask("Use custom filename (no timestamp)?", default=False)
        custom_name = None
        if use_custom_name:
            custom_name = Prompt.ask("Enter filename", default=f"{database}.tar.gz")
            if not custom_name.endswith('.tar.gz'):
                custom_name += '.tar.gz'

        # 6. Show configuration and confirm
        console.print(f"\n[bold]Backup Configuration:[/bold]")
        console.print(f"  Source: {source_uri}")
        console.print(f"  Database: {database}")
        console.print(f"  Collections: {collections if collections else 'ALL'}")
        if isinstance(storage_url, dict):
            storage_desc = f"{storage_url['name']} ({storage_url['type']})"
        else:
            storage_desc = storage_url
        console.print(f"  Destination: {storage_desc}")
        if custom_name:
            console.print(f"  Filename: {custom_name} [yellow](will overwrite existing)[/yellow]")
        else:
            console.print(f"  Filename: <timestamp>-{database}.tar.gz")

        if not Prompt.ask("\n[bold yellow]Start backup?[/bold yellow]", choices=["y", "n"], default="y") == "y":
            return

        # Test storage connection first if remote
        from .storage import StorageFactory
        from urllib.parse import urlparse

        # Determine if storage_url is a config dict or URL string
        if isinstance(storage_url, dict):
            # Using saved config
            storage = StorageFactory.create(storage_url)
            storage_type = storage_url.get('type', 'local')

            if storage_type != 'local':
                console.print("\n[dim]Testing storage connection and write permissions...[/dim]")
                if hasattr(storage, 'test_connection'):
                    test_path = storage_url.get('path', '/')
                    success, error_msg = storage.test_connection(test_path)
                    if not success:
                        console.print(f"[red]❌ Storage connection failed: {error_msg}[/red]")
                        Prompt.ask("Press Enter to continue")
                        return
                    console.print("[green]✓ Storage connection and permissions OK[/green]")
        elif '://' in storage_url:
            # Using URL string
            storage = StorageFactory.create(storage_url)

            console.print("\n[dim]Testing storage connection and write permissions...[/dim]")
            if hasattr(storage, 'test_connection'):
                parsed = urlparse(storage_url)
                test_path = parsed.path or '/'
                success, error_msg = storage.test_connection(test_path)
                if not success:
                    console.print(f"[red]❌ Storage connection failed: {error_msg}[/red]")
                    Prompt.ask("Press Enter to continue")
                    return
                console.print("[green]✓ Storage connection OK[/green]")

        # Pass storage config directly to BackupManager (it handles both dict and URL)
        backup_mgr = BackupManager(source_uri, storage_url)

        # Perform backup
        result = backup_mgr.backup_database(database, collections, custom_name=custom_name)

        if result['success']:
            console.print(f"\n[green]✅ Backup completed successfully![/green]")
            console.print(f"[bold]File:[/bold] {result['filename']}")
            console.print(f"[bold]Size:[/bold] {result['size_human']}")
            console.print(f"[bold]Documents:[/bold] {format_number(result['documents'])}")
            console.print(f"[bold]Collections:[/bold] {result['collections']}")

            # Ask to save as task
            if Prompt.ask("\n[bold]Save as task?[/bold]", choices=["y", "n"], default="y") == "y":
                task_name = Prompt.ask("Task name", default=f"backup_{database}")

                task = BackupTask.create_backup_task(
                    name=task_name,
                    mongo_uri=source_uri,
                    database=database,
                    collections=collections,
                    storage_url=storage_url,
                    custom_name=custom_name
                )

                self.settings_manager.add_task(task_name, task)
                console.print(f"[green]✓ Task '{task_name}' saved![/green]")
                console.print(f"[dim]Run with: mw --task {task_name}[/dim]")
        else:
            console.print(f"[red]❌ Backup failed: {result.get('error')}[/red]")

        backup_mgr.close()
        Prompt.ask("\nPress Enter to continue")

    def restore_wizard(self):
        """Wizard for database restore"""
        self.clear_screen()
        console.print("\n[bold cyan]📥 DATABASE RESTORE WIZARD[/bold cyan]\n")

        # 1. Select storage source
        console.print("[bold cyan]📍 STORAGE SOURCE[/bold cyan]\n")

        # Check for saved storage configs
        saved_storages = self.settings_manager.list_storages()
        storage_list = list(saved_storages.items())
        storage_url = None

        if storage_list:
            console.print("[bold]Available storage locations:[/bold]\n")

            for i, (name, config) in enumerate(storage_list, 1):
                storage_type = config.get('type', 'unknown')
                storage_name = config.get('name', name)
                console.print(f"  [cyan]{i}.[/cyan] {storage_name} ({storage_type})")

            console.print(f"  [cyan]{len(storage_list) + 1}.[/cyan] ➕ Add new storage location")

            choices = [str(i) for i in range(1, len(storage_list) + 2)]
            choice = Prompt.ask("\nSelect storage", choices=choices, default="1")

            if choice == str(len(storage_list) + 1):
                # Add new storage
                storage_config = self._prompt_new_storage()
                storage_url = storage_config
            else:
                # Use saved storage
                idx = int(choice) - 1
                _, storage_config = storage_list[idx]
                storage_url = storage_config
        else:
            # No saved configs, prompt for new one
            console.print("[yellow]No saved storage configs. Let's create one:[/yellow]\n")
            storage_config = self._prompt_new_storage()
            storage_url = storage_config

        # 2. List and select backup file
        dummy_uri = "mongodb://localhost:27017"  # Need a URI for BackupManager init
        backup_mgr = BackupManager(dummy_uri, storage_url)

        console.print("\n[dim]Loading backups...[/dim]")
        backups = backup_mgr.list_backups()

        if not backups:
            if isinstance(storage_url, dict):
                location = f"{storage_url.get('name', 'storage')} ({storage_url.get('path', 'N/A')})"
            else:
                location = storage_url
            console.print(f"[yellow]No backups found at: {location}[/yellow]")
            Prompt.ask("Press Enter to continue")
            return

        # Display and select backup
        backup_file = backup_mgr.display_backups(backups)
        if not backup_file:
            return

        # 3. Select target MongoDB
        self.clear_screen()
        console.print("\n[bold cyan]SELECT RESTORE TARGET[/bold cyan]\n")
        target_uri = self.select_or_add_host("restore target")
        if not target_uri:
            return

        # Test connection
        console.print("\n[dim]Testing connection...[/dim]")
        is_connected, msg = test_connection(target_uri)
        if not is_connected:
            console.print(f"[red]❌ Connection failed: {msg}[/red]")
            Prompt.ask("Press Enter to continue")
            return

        # 4. Target database name
        backup_filename = os.path.basename(backup_file)
        # Extract database name from filename (format: YYYY_MM_DD_HH_MM-database.tar.gz)
        if '-' in backup_filename and backup_filename.endswith('.tar.gz'):
            default_db = backup_filename.split('-')[1].replace('.tar.gz', '')
        else:
            default_db = "restored_db"

        target_database = Prompt.ask("Target database name", default=default_db)

        # 5. Drop target option
        drop_target = Prompt.ask(
            "[bold yellow]⚠ Drop target database before restore?[/bold yellow]",
            choices=["y", "n"],
            default="n"
        ) == "y"

        # 6. Confirm and restore
        console.print(f"\n[bold]Restore Configuration:[/bold]")
        console.print(f"  Backup: {backup_filename}")
        console.print(f"  Target: {target_uri}")
        console.print(f"  Database: {target_database}")
        console.print(f"  Drop target: {'Yes' if drop_target else 'No'}")

        if not Prompt.ask("\n[bold yellow]Start restore?[/bold yellow]", choices=["y", "n"], default="y") == "y":
            return

        # Update backup manager with correct target URI
        backup_mgr = BackupManager(target_uri, storage_url)

        # Perform restore
        result = backup_mgr.restore_database(backup_file, target_database, drop_target)

        if result['success']:
            console.print(f"\n[green]✅ Restore completed successfully![/green]")
            console.print(f"[bold]Database:[/bold] {result['database']}")
            console.print(f"[bold]Documents:[/bold] {format_number(result['documents'])}")
            console.print(f"[bold]Collections:[/bold] {result['collections']}")

            # Ask to save as task
            if Prompt.ask("\n[bold]Save as task?[/bold]", choices=["y", "n"], default="n") == "y":
                task_name = Prompt.ask("Task name", default=f"restore_{target_database}")

                task = BackupTask.create_restore_task(
                    name=task_name,
                    mongo_uri=target_uri,
                    backup_file=backup_file,
                    target_database=target_database,
                    storage_url=storage_url,
                    drop_target=drop_target
                )

                self.settings_manager.add_task(task_name, task)
                console.print(f"[green]✓ Task '{task_name}' saved![/green]")
                console.print(f"[dim]Run with: mw --task {task_name}[/dim]")
        else:
            console.print(f"[red]❌ Restore failed: {result.get('error')}[/red]")

        backup_mgr.close()
        Prompt.ask("\nPress Enter to continue")

    def _prompt_new_storage(self):
        """Prompt for new storage configuration"""
        console.print("  [cyan]1.[/cyan] 💾 Local directory")
        console.print("  [cyan]2.[/cyan] 🌐 SSH/SCP remote server")
        console.print("  [cyan]3.[/cyan] 📡 FTP server")

        storage_choice = Prompt.ask("\nChoose storage type", choices=["1", "2", "3"])

        if storage_choice == "1":
            # Local directory
            default_dir = os.path.join(os.path.expanduser("~"), "mongo_backups")
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
            path = Prompt.ask("Remote path", default="/backups/mongodb")
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
        """Main wizard loop"""
        while True:
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


@click.command()
@click.option('--interactive', '-i', is_flag=True, help='Launch interactive wizard')
@click.option('--task', '-t', help='Run a saved task by name')
@click.option('--list-tasks', is_flag=True, help='List all saved tasks')
@click.option('--force', '-f', is_flag=True, help='Skip confirmation when running task (deprecated, use -y)')
@click.option('-y', '--yes', 'assume_yes', is_flag=True, help='Assume yes to all prompts (fully automated)')
def main(interactive, task, list_tasks, force, assume_yes):
    """
    MongoDB Copy Wizard - Interactive tool with saved hosts and tasks

    Examples:

    Interactive mode (default):
        mongo_wizard.py
        mongo_wizard.py -i

    Run saved task:
        mongo_wizard.py --task daily_backup
        mongo_wizard.py -t daily_backup -y  # Fully automated, no prompts

    List saved tasks:
        mongo_wizard.py --list-tasks
    """

    wizard = MongoWizard()

    # List tasks mode
    if list_tasks:
        settings_manager = SettingsManager()
        saved_tasks_dict = settings_manager.list_tasks()  # Returns Dict[str, Dict]

        if not saved_tasks_dict:
            console.print("[yellow]No saved tasks found[/yellow]")
        else:
            table = Table(title="Saved Tasks", box=box.ROUNDED)
            table.add_column("Name", style="green")
            table.add_column("Source DB", style="cyan")
            table.add_column("Target DB", style="yellow")
            table.add_column("Collection", style="magenta")

            for task_name, task_config in saved_tasks_dict.items():
                # Handle collection display (can be string, list, or None)
                coll = task_config.get('source_collection', 'ALL')
                if isinstance(coll, list):
                    coll_display = f"{len(coll)} collections"
                elif coll:
                    coll_display = coll
                else:
                    coll_display = 'ALL'

                table.add_row(
                    task_name,
                    task_config['source_db'],
                    task_config['target_db'],
                    coll_display
                )

            console.print(table)
            console.print(f"\n[dim]Run a task with: python mongo_wizard.py --task <name>[/dim]")
        return

    # Run task mode
    if task:
        settings_manager = SettingsManager()
        task_config = settings_manager.get_task(task)

        if not task_config:
            console.print(f"[red]❌ Task '{task}' not found![/red]")
            console.print("[dim]Use --list-tasks to see available tasks[/dim]")
            sys.exit(1)

        console.print(f"[cyan]🚀 Running task: {task}[/cyan]\n")

        # Show task summary
        console.print(f"[bold]Source:[/bold] {task_config['source_uri']}")
        console.print(f"[bold]Target:[/bold] {task_config['target_uri']}")
        console.print(f"[bold]Database:[/bold] {task_config['source_db']} → {task_config['target_db']}")
        if task_config.get('source_collection'):
            console.print(
                f"[bold]Collection:[/bold] {task_config['source_collection']} → {task_config.get('target_collection', task_config['source_collection'])}")

        if not force and not assume_yes:
            if not Confirm.ask("\n[yellow]Execute this task?[/yellow]"):
                console.print("[red]Cancelled[/red]")
                sys.exit(0)

        # Execute task
        from .core import MongoAdvancedCopier

        copier = MongoAdvancedCopier(task_config['source_uri'], task_config['target_uri'])

        try:
            copier.connect()

            if task_config.get('source_collection'):
                result = copier.copy_collection_with_indexes(
                    task_config['source_db'], task_config['source_collection'],
                    task_config['target_db'], task_config.get('target_collection', task_config['source_collection']),
                    drop_target=task_config.get('drop_target', False),
                    force=assume_yes
                )
                console.print(f"[green]✅ Copied {format_number(result['documents_copied'])} documents[/green]")
            else:
                results = copier.copy_entire_database(
                    task_config['source_db'], task_config['target_db'],
                    drop_target=task_config.get('drop_target', False),
                    force=assume_yes
                )
                total_docs = sum(r['documents_copied'] for r in results.values())
                console.print(f"[green]✅ Copied {len(results)} collections, {format_number(total_docs)} documents[/green]")

            console.print("[green]✅ Task completed successfully![/green]")

        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/red]")
            sys.exit(1)
        finally:
            copier.close()

        return

    # Default: interactive mode
    wizard.run()


if __name__ == '__main__':
    # If no arguments, launch interactive mode
    if len(sys.argv) == 1:
        wizard = MongoWizard()
        wizard.run()
    else:
        main()
