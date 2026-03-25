"""
Database backup and restore manager
Handles backup creation, storage, and restoration with progress tracking.
Currently uses engine.dump()/engine.restore() for the actual DB operations.
"""

import os
import tempfile
import tarfile
from datetime import datetime
from typing import Any
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich import box

from .storage import StorageFactory, LocalStorage
from .engine import EngineFactory
from .formatting import format_docs, format_size
from .constants import DEFAULT_CONNECTION_TIMEOUT

console = Console()


class BackupManager:
    """Manages database backups and restores"""

    def __init__(self, source_uri: str, storage_url: str | dict | None = None):
        """
        Initialize backup manager

        Args:
            source_uri: Database connection URI
            storage_url: Storage destination (local path, remote URL, or config dict)
        """
        self.source_uri = source_uri
        self.storage_config = storage_url or os.getcwd()
        self.storage = StorageFactory.create(storage_url)
        self.client = None

    def connect(self) -> bool:
        """Connect to database via engine"""
        try:
            self.engine = EngineFactory.create(self.source_uri)
            self.engine.connect(timeout=DEFAULT_CONNECTION_TIMEOUT)
            self.client = self.engine.client if hasattr(self.engine, 'client') else None
            return True
        except Exception as e:
            console.print(f"[red]Database connection failed: {e}[/red]")
            return False

    def close(self):
        """Close database connection"""
        if hasattr(self, 'engine') and self.engine:
            self.engine.close()

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

            # Count rows and list tables via engine interface
            total_docs = 0
            total_collections = 0

            if collections:
                target_collections = collections
            else:
                target_collections = [t['name'] for t in self.engine.list_tables(database)]

            console.print(f"\n[cyan]📦 Preparing backup of {database}...[/cyan]")

            # Count rows
            all_table_names = [t['name'] for t in self.engine.list_tables(database)]
            with console.status("[dim]Analyzing tables...[/dim]"):
                for coll_name in target_collections:
                    if coll_name in all_table_names:
                        count = self.engine.count_rows(database, coll_name)
                        total_docs += count
                        total_collections += 1
                        console.print(f"  * {coll_name}: {format_docs(count)} rows")

            console.print(f"\n[bold]Total:[/bold] {total_collections} collections, {format_docs(total_docs)} documents\n")

            # Check for dump tool
            tools = self.engine.check_tools()
            if not tools.get('mongodump', False) and not tools.get('mysqldump', False):
                console.print("[red]Dump tool not found! Install database tools (mongodump or mysqldump).[/red]")
                return {'success': False, 'error': 'dump tool not found'}

            # Run dump via engine
            with console.status(f"[cyan]Dumping {database}...[/cyan]"):
                success = self.engine.dump(database, collections, dump_path)
                if not success:
                    return {'success': False, 'error': 'Dump failed'}

            # Create tar.gz archive
            archive_path = os.path.join(temp_dir, backup_name)

            with console.status("[cyan]Creating archive... In progress...[/cyan]"):
                with tarfile.open(archive_path, 'w:gz') as tar:
                    tar.add(dump_path, arcname='dump')

            # Get archive size
            archive_size = os.path.getsize(archive_path)
            size_human = format_size(archive_size)

            # Upload to storage
            if isinstance(self.storage, LocalStorage):
                # For local, just move the file
                if isinstance(self.storage_config, dict):
                    final_path = os.path.join(self.storage_config.get('path', '.'), backup_name)
                else:
                    final_path = os.path.join(self.storage_config, backup_name)
                success = self.storage.upload(archive_path, final_path)
            else:
                # For remote, upload - extract path from config or URL
                if isinstance(self.storage_config, dict):
                    base_path = self.storage_config.get('path', '/backups')
                else:
                    from urllib.parse import urlparse
                    parsed = urlparse(self.storage_config)
                    base_path = parsed.path or '/backups'
                    if not base_path.startswith('/'):
                        base_path = '/' + base_path
                remote_path = os.path.join(base_path, backup_name)
                success = self.storage.upload(archive_path, remote_path)
                final_path = remote_path

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
        tools = self.engine.check_tools()
        if not tools.get('mongorestore', False) and not tools.get('mysql', False):
            console.print("[red]Restore tool not found! Install database tools (mongorestore or mysql).[/red]")
            return {'success': False, 'error': 'restore tool not found'}

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
                    # filter='data' prevents path traversal attacks (e.g. ../../etc/cron.d/malware)
                    tar.extractall(extract_path, filter='data')
                    progress.update(task, completed=100)

            # Find database in dump
            dump_path = os.path.join(extract_path, 'dump')
            db_dirs = [d for d in os.listdir(dump_path) if os.path.isdir(os.path.join(dump_path, d))]

            if not db_dirs:
                console.print("[red]No database found in backup![/red]")
                return {'success': False, 'error': 'Invalid backup file'}

            source_db = db_dirs[0]
            target_db = target_database or source_db

            # Count files in backup to estimate collections
            backup_path = os.path.join(dump_path, source_db)
            # Count .bson (MongoDB) or .sql (MySQL) files
            backup_files = [f for f in os.listdir(backup_path)
                           if f.endswith('.bson') or f.endswith('.sql')]
            total_collections = len(backup_files)

            console.print(f"\n[bold]Source database:[/bold] {source_db}")
            console.print(f"[bold]Target database:[/bold] {target_db}")
            console.print(f"[bold]Tables:[/bold] {total_collections}")

            # Restore via engine
            with console.status(f"[cyan]Restoring to {target_db}...[/cyan]"):
                success = self.engine.restore(dump_path, target_db, drop_target=drop_target)

            if success:
                # Count restored rows -- estimated_document_count() is unreliable
                # right after mongorestore, so use count_documents({}) via client
                total_docs = 0
                try:
                    db = self.engine.client[target_db]
                    for coll_name in db.list_collection_names():
                        total_docs += db[coll_name].count_documents({})
                except Exception:
                    pass

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
                return {'success': False, 'error': 'Restore failed'}

    def list_backups(self, database_filter: str | None = None) -> list[dict[str, Any]]:
        """
        List available backups

        Args:
            database_filter: Filter by database name

        Returns:
            List of backup information dictionaries
        """
        # Parse storage path to get directory
        if isinstance(self.storage_config, dict):
            path = self.storage_config.get('path', '/')
        elif '://' in self.storage_config:
            from urllib.parse import urlparse
            parsed = urlparse(self.storage_config)
            path = parsed.path or '/'
        else:
            path = self.storage_config

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



class BackupTask:
    """Represents a backup/restore task for settings"""

    @staticmethod
    def create_backup_task(
        name: str,
        db_uri: str,
        database: str,
        collections: list[str] | None,
        storage_url: str,
        custom_name: str | None = None
    ) -> dict[str, Any]:
        """Create backup task configuration"""
        task = {
            'type': 'backup',
            'name': name,
            'db_uri': db_uri,
            'database': database,
            'collections': collections,
            'storage_url': storage_url,
            'created': datetime.now().isoformat()
        }
        if custom_name:
            task['custom_name'] = custom_name
        return task

    @staticmethod
    def create_restore_task(
        name: str,
        db_uri: str,
        backup_file: str,
        target_database: str | None,
        storage_url: str,
        drop_target: bool = False
    ) -> dict[str, Any]:
        """Create restore task configuration"""
        return {
            'type': 'restore',
            'name': name,
            'db_uri': db_uri,
            'backup_file': backup_file,
            'target_database': target_database,
            'storage_url': storage_url,
            'drop_target': drop_target,
            'created': datetime.now().isoformat()
        }