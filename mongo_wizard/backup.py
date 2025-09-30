"""
MongoDB backup and restore manager
Handles backup creation, storage, and restoration with progress tracking
"""

import os
import subprocess
import tempfile
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Any
from pymongo import MongoClient
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich import box

from .storage import StorageFactory, StorageBackend
from .utils import check_mongodb_tools, connect_mongo
from .formatting import format_docs, format_number, format_size
from .constants import DEFAULT_MONGO_TIMEOUT

console = Console()


class BackupManager:
    """Manages MongoDB backups and restores"""

    def __init__(self, mongo_uri: str, storage_url: str | None = None):
        """
        Initialize backup manager

        Args:
            mongo_uri: MongoDB connection URI
            storage_url: Storage destination (local path or remote URL)
        """
        self.mongo_uri = mongo_uri
        self.storage_url = storage_url or os.getcwd()
        self.storage = StorageFactory.create(storage_url)
        self.client = None

    def connect(self) -> bool:
        """Connect to MongoDB"""
        try:
            self.client = connect_mongo(self.mongo_uri, timeout=DEFAULT_MONGO_TIMEOUT)
            return True
        except Exception as e:
            console.print(f"[red]MongoDB connection failed: {e}[/red]")
            return False

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()

    def backup_database(
        self,
        database: str,
        collections: list[str] | None = None,
        custom_name: str | None = None
    ) -> dict[str, Any]:
        """
        Backup database or specific collections

        Args:
            database: Database name
            collections: List of collections to backup (None for all)
            custom_name: Custom backup name (default: auto-generated)

        Returns:
            Dictionary with backup details
        """
        if not self.connect():
            return {'success': False, 'error': 'Connection failed'}

        # Generate backup filename
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M")
        backup_name = custom_name or f"{timestamp}-{database}.tar.gz"

        # Create temporary directory for dump
        with tempfile.TemporaryDirectory() as temp_dir:
            dump_path = os.path.join(temp_dir, 'dump')

            # Count documents and calculate size
            db = self.client[database]
            total_docs = 0
            total_collections = 0

            if collections:
                target_collections = collections
            else:
                target_collections = db.list_collection_names()

            console.print(f"\n[cyan]📦 Preparing backup of {database}...[/cyan]")

            # Count documents
            with console.status("[dim]Analyzing collections...[/dim]"):
                for coll_name in target_collections:
                    if coll_name in db.list_collection_names():
                        count = db[coll_name].estimated_document_count()
                        total_docs += count
                        total_collections += 1
                        console.print(f"  • {coll_name}: {format_docs(count)} documents")

            console.print(f"\n[bold]Total:[/bold] {total_collections} collections, {format_docs(total_docs)} documents\n")

            # Check for mongodump
            tools = check_mongodb_tools()
            if not tools['mongodump']:
                console.print("[red]mongodump not found! Please install MongoDB tools.[/red]")
                return {'success': False, 'error': 'mongodump not found'}

            # Build mongodump command
            cmd = ['mongodump', '--uri', self.mongo_uri, '--db', database, '--out', dump_path]

            if collections:
                # Backup specific collections
                for coll in collections:
                    coll_cmd = cmd + ['--collection', coll]
                    self._run_mongodump(coll_cmd, f"Backing up {coll}")
            else:
                # Backup entire database
                self._run_mongodump(cmd, f"Backing up {database}")

            # Create tar.gz archive
            archive_path = os.path.join(temp_dir, backup_name)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("[cyan]Creating archive...", total=100)

                with tarfile.open(archive_path, 'w:gz') as tar:
                    tar.add(dump_path, arcname='dump')
                    progress.update(task, completed=100)

            # Get archive size
            archive_size = os.path.getsize(archive_path)
            size_human = format_size(archive_size)

            # Upload to storage
            if isinstance(self.storage, type(StorageFactory.create('/'))):  # LocalStorage
                # For local, just move the file
                final_path = os.path.join(self.storage_url, backup_name)
                success = self.storage.upload(archive_path, final_path)
            else:
                # For remote, upload - extract path from URL
                from urllib.parse import urlparse
                parsed = urlparse(self.storage_url)
                base_path = parsed.path or '/backups'
                if not base_path.startswith('/'):
                    base_path = '/' + base_path
                remote_path = os.path.join(base_path, backup_name)
                success = self.storage.upload(archive_path, remote_path)
                final_path = f"{self.storage_url}/{backup_name}"

            if success:
                console.print(f"\n[green]✅ Backup completed successfully![/green]")
                console.print(f"[bold]Location:[/bold] {final_path}")
                console.print(f"[bold]Size:[/bold] {size_human}")
                console.print(f"[bold]Documents:[/bold] {format_docs(total_docs)}")
                console.print(f"[bold]Collections:[/bold] {total_collections}")

                self.close()
                return {
                    'success': True,
                    'path': final_path,
                    'size': archive_size,
                    'size_human': size_human,
                    'documents': total_docs,
                    'collections': total_collections,
                    'database': database,
                    'timestamp': timestamp,
                    'filename': backup_name
                }
            else:
                console.print(f"[red]❌ Failed to store backup[/red]")
                console.print(f"[red]The backup file was created but could not be uploaded to the storage location[/red]")
                console.print(f"[dim]Check your storage credentials and network connection[/dim]")
                self.close()
                return {'success': False, 'error': 'Storage upload failed'}

    def restore_database(
        self,
        backup_file: str,
        target_database: str | None = None,
        drop_target: bool = False
    ) -> dict[str, Any]:
        """
        Restore database from backup

        Args:
            backup_file: Path or name of backup file
            target_database: Target database name (default: from backup)
            drop_target: Drop target database before restore

        Returns:
            Dictionary with restore details
        """
        if not self.connect():
            return {'success': False, 'error': 'Connection failed'}

        # Check for mongorestore
        tools = check_mongodb_tools()
        if not tools['mongorestore']:
            console.print("[red]mongorestore not found! Please install MongoDB tools.[/red]")
            return {'success': False, 'error': 'mongorestore not found'}

        with tempfile.TemporaryDirectory() as temp_dir:
            # Download backup if remote
            local_backup = os.path.join(temp_dir, os.path.basename(backup_file))

            console.print(f"\n[cyan]📥 Retrieving backup...[/cyan]")
            if not self.storage.download(backup_file, local_backup):
                return {'success': False, 'error': 'Failed to retrieve backup'}

            # Get file info
            file_size = os.path.getsize(local_backup)
            console.print(f"[dim]Backup size: {format_size(file_size)}[/dim]")

            # Extract archive
            extract_path = os.path.join(temp_dir, 'restore')
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("[cyan]Extracting archive...", total=100)

                with tarfile.open(local_backup, 'r:gz') as tar:
                    tar.extractall(extract_path)
                    progress.update(task, completed=100)

            # Find database in dump
            dump_path = os.path.join(extract_path, 'dump')
            db_dirs = [d for d in os.listdir(dump_path) if os.path.isdir(os.path.join(dump_path, d))]

            if not db_dirs:
                console.print("[red]No database found in backup![/red]")
                return {'success': False, 'error': 'Invalid backup file'}

            source_db = db_dirs[0]
            target_db = target_database or source_db

            # Count collections and documents in backup
            backup_path = os.path.join(dump_path, source_db)
            collection_files = [f for f in os.listdir(backup_path) if f.endswith('.bson')]
            total_collections = len(collection_files)

            console.print(f"\n[bold]Source database:[/bold] {source_db}")
            console.print(f"[bold]Target database:[/bold] {target_db}")
            console.print(f"[bold]Collections:[/bold] {total_collections}")

            if drop_target:
                console.print(f"\n[yellow]⚠ Dropping target database {target_db}...[/yellow]")
                self.client.drop_database(target_db)

            # Build mongorestore command
            cmd = [
                'mongorestore',
                '--uri', self.mongo_uri,
                '--nsFrom', f'{source_db}.*',
                '--nsTo', f'{target_db}.*',
                dump_path
            ]

            if drop_target:
                cmd.append('--drop')

            # Run mongorestore
            success = self._run_mongorestore(cmd, f"Restoring to {target_db}")

            if success:
                # Count restored documents
                db = self.client[target_db]
                total_docs = 0
                for coll_name in db.list_collection_names():
                    total_docs += db[coll_name].estimated_document_count()

                console.print(f"\n[green]✅ Restore completed successfully![/green]")
                console.print(f"[bold]Database:[/bold] {target_db}")
                console.print(f"[bold]Documents:[/bold] {format_docs(total_docs)}")
                console.print(f"[bold]Collections:[/bold] {total_collections}")

                self.close()
                return {
                    'success': True,
                    'database': target_db,
                    'documents': total_docs,
                    'collections': total_collections,
                    'source_backup': backup_file
                }
            else:
                console.print(f"[red]Restore failed![/red]")
                self.close()
                return {'success': False, 'error': 'mongorestore failed'}

    def list_backups(self, database_filter: str | None = None) -> list[dict[str, Any]]:
        """
        List available backups

        Args:
            database_filter: Filter by database name

        Returns:
            List of backup information dictionaries
        """
        # Parse storage path to get directory
        if '://' in self.storage_url:
            from urllib.parse import urlparse
            parsed = urlparse(self.storage_url)
            path = parsed.path or '/'
        else:
            path = self.storage_url

        backups = self.storage.list_files(path, pattern='*.tar.gz')

        if database_filter:
            # Filter by database name in filename
            backups = [b for b in backups if f"-{database_filter}.tar.gz" in b['name']]

        return backups

    def display_backups(self, backups: list[dict[str, Any]]) -> str | None:
        """
        Display backups in a table and let user select

        Args:
            backups: List of backup dictionaries

        Returns:
            Selected backup path or None
        """
        if not backups:
            console.print("[yellow]No backups found[/yellow]")
            return None

        table = Table(title="🗄️ Available Backups", box=box.ROUNDED)
        table.add_column("#", style="dim", width=4)
        table.add_column("Filename", style="cyan")
        table.add_column("Size", style="green", justify="right")
        table.add_column("Modified", style="yellow")

        for idx, backup in enumerate(backups, 1):
            table.add_row(
                str(idx),
                backup['name'],
                backup['size_human'],
                backup['modified'].strftime("%Y-%m-%d %H:%M")
            )

        console.print(table)

        # Let user select
        try:
            console.print("\n[cyan]Select backup number (or 'q' to cancel):[/cyan]", end=" ")
            choice = input()
            if choice.lower() == 'q':
                return None

            idx = int(choice) - 1
            if 0 <= idx < len(backups):
                return backups[idx]['path']
            else:
                console.print("[red]Invalid selection[/red]")
                return None
        except (ValueError, KeyboardInterrupt):
            return None

    def _run_mongodump(self, cmd: list[str], description: str) -> bool:
        """Run mongodump command with progress"""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"[cyan]{description}...", total=None)

            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    progress.update(task, completed=100)
                    return True
                else:
                    console.print(f"[red]mongodump error: {result.stderr}[/red]")
                    return False
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                return False

    def _run_mongorestore(self, cmd: list[str], description: str) -> bool:
        """Run mongorestore command with progress"""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"[cyan]{description}...", total=None)

            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    progress.update(task, completed=100)
                    return True
                else:
                    console.print(f"[red]mongorestore error: {result.stderr}[/red]")
                    return False
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                return False



class BackupTask:
    """Represents a backup/restore task for settings"""

    @staticmethod
    def create_backup_task(
        name: str,
        mongo_uri: str,
        database: str,
        collections: list[str] | None,
        storage_url: str
    ) -> dict[str, Any]:
        """Create backup task configuration"""
        return {
            'type': 'backup',
            'name': name,
            'mongo_uri': mongo_uri,
            'database': database,
            'collections': collections,
            'storage_url': storage_url,
            'created': datetime.now().isoformat()
        }

    @staticmethod
    def create_restore_task(
        name: str,
        mongo_uri: str,
        backup_file: str,
        target_database: str | None,
        storage_url: str,
        drop_target: bool = False
    ) -> dict[str, Any]:
        """Create restore task configuration"""
        return {
            'type': 'restore',
            'name': name,
            'mongo_uri': mongo_uri,
            'backup_file': backup_file,
            'target_database': target_database,
            'storage_url': storage_url,
            'drop_target': drop_target,
            'created': datetime.now().isoformat()
        }