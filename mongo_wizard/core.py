#!/usr/bin/env python
"""
MongoDB Advanced Copy Tool
Copy with indexes, verification, and backup support
"""

import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime
from typing import Any

from pymongo import MongoClient, ReplaceOne
from pymongo.errors import ConnectionFailure
from rich.console import Console
from .formatting import format_number
from .utils import connect_mongo
from .constants import (
    DEFAULT_MONGO_TIMEOUT,
    DEFAULT_BATCH_SIZE,
    DEFAULT_VERIFICATION_SAMPLE_SIZE,
    CHECKSUM_THRESHOLD
)
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.prompt import Confirm
from rich.table import Table

console = Console()


class MongoAdvancedCopier:
    """Advanced MongoDB copier with index support and verification"""

    def __init__(self, source_uri: str, target_uri: str):
        self.source_uri = source_uri
        self.target_uri = target_uri
        self.source_client = None
        self.target_client = None

    def connect(self):
        """Establish connections"""
        try:
            self.source_client = connect_mongo(self.source_uri, timeout=DEFAULT_MONGO_TIMEOUT)
            self.target_client = connect_mongo(self.target_uri, timeout=DEFAULT_MONGO_TIMEOUT)
            return self

        except ConnectionFailure as e:
            console.print(f"[red]❌ Connection failed: {e}[/red]")
            sys.exit(1)

    def close(self):
        """Close connections"""
        if self.source_client:
            self.source_client.close()
        if self.target_client:
            self.target_client.close()

    def copy_indexes(self, source_db: str, source_coll: str, target_db: str, target_coll: str) -> int:
        """Copy indexes from source to target collection"""
        source_collection = self.source_client[source_db][source_coll]
        target_collection = self.target_client[target_db][target_coll]

        indexes = list(source_collection.list_indexes())
        created = 0

        for index in indexes:
            # Skip the default _id index
            if index['name'] == '_id_':
                continue

            try:
                # Extract index keys and options
                keys = index['key']
                options = {k: v for k, v in index.items() if k not in ['key', 'v', 'ns']}

                target_collection.create_index(list(keys.items()), **options)
                created += 1
                console.print(f"  [green]✓[/green] Created index: {index['name']}")
            except Exception as e:
                console.print(f"  [yellow]⚠[/yellow] Failed to create index {index['name']}: {e}")

        return created

    def copy_collection_with_indexes(
        self,
        source_db: str,
        source_coll: str,
        target_db: str,
        target_coll: str,
        drop_target: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
        force: bool = False,
        force_python: bool = False
    ) -> dict[str, Any]:
        """Copy collection with all indexes

        Args:
            force_python: Force Python copy instead of mongodump (slower but more control)
        """

        source_collection = self.source_client[source_db][source_coll]
        target_collection = self.target_client[target_db][target_coll]

        # Get document count
        total_docs = source_collection.estimated_document_count()
        console.print(f"[green]📊 Collection has ~{format_number(total_docs)} documents[/green]")

        # PREFER MONGODUMP/MONGORESTORE unless force_python is set
        if not force_python and shutil.which('mongodump') and shutil.which('mongorestore'):
            console.print("[cyan]🚀 Using mongodump/mongorestore (fast native mode)...[/cyan]")

            # Handle drop target with confirmation
            if drop_target and target_collection.estimated_document_count() > 0:
                if force or Confirm.ask(f"[yellow]Drop target collection {target_db}.{target_coll}?[/yellow]"):
                    target_collection.drop()
                    console.print("[yellow]🗑️  Dropped target collection[/yellow]")

            # Use native tools
            success = self.copy_with_mongodump(
                source_db, source_coll,
                target_db, target_coll,
                drop_target=False  # Already handled above
            )

            if success:
                # Get actual count for reporting
                copied_count = self.target_client[target_db][target_coll].estimated_document_count()
                console.print(f"[green]✅ Copied {format_number(copied_count)} documents using mongodump[/green]")

                # Count indexes
                target_indexes = list(self.target_client[target_db][target_coll].list_indexes())
                indexes_count = len([idx for idx in target_indexes if idx['name'] != '_id_'])

                return {
                    'documents_copied': copied_count,
                    'indexes_created': indexes_count,
                    'source_count': total_docs,
                    'method': 'mongodump'
                }
            else:
                console.print("[yellow]⚠ mongodump failed, falling back to Python copy...[/yellow]")

        # FALLBACK TO PYTHON COPY (or if force_python=True)
        if force_python:
            console.print("[cyan]📝 Using Python copy mode (--force-python flag set)...[/cyan]")
        else:
            console.print("[yellow]⚠ MongoDB tools not available, using Python copy (slower)...[/yellow]")
            console.print("[dim]Install with: brew install mongodb-database-tools[/dim]")

        # Drop target if requested
        if drop_target and target_collection.estimated_document_count() > 0:
            if force or Confirm.ask(f"[yellow]Drop target collection {target_db}.{target_coll}?[/yellow]"):
                target_collection.drop()
                console.print("[yellow]🗑️  Dropped target collection[/yellow]")

        # Copy indexes first
        console.print("[cyan]📐 Copying indexes...[/cyan]")
        indexes_created = self.copy_indexes(source_db, source_coll, target_db, target_coll)

        # Copy documents
        console.print("[cyan]📄 Copying documents...[/cyan]")
        copied = 0
        batch = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Copying...", total=total_docs)

            for doc in source_collection.find():
                batch.append(doc)

                if len(batch) >= batch_size:
                    target_collection.insert_many(batch, ordered=False)
                    copied += len(batch)
                    progress.update(task, advance=len(batch))
                    batch = []

            # Insert remaining documents
            if batch:
                target_collection.insert_many(batch, ordered=False)
                copied += len(batch)
                progress.update(task, advance=len(batch))

        return {
            'documents_copied': copied,
            'indexes_created': indexes_created,
            'source_count': total_docs,
            'method': 'python'
        }

    def copy_with_mongodump(
        self,
        source_db: str,
        source_coll: str,
        target_db: str,
        target_coll: str,
        drop_target: bool = False
    ) -> bool:
        """Use mongodump/mongorestore for copying (faster for large collections)"""

        # Check if tools are available
        if not shutil.which('mongodump') or not shutil.which('mongorestore'):
            return False

        try:
            # Build mongodump command
            dump_cmd = [
                'mongodump',
                '--uri', self.source_uri,
                '--db', source_db,
                '--collection', source_coll,
                '--archive'
            ]

            # Build mongorestore command
            restore_cmd = [
                'mongorestore',
                '--uri', self.target_uri,
                '--archive',
                '--nsFrom', f'{source_db}.{source_coll}',
                '--nsTo', f'{target_db}.{target_coll}'
            ]

            if drop_target:
                restore_cmd.append('--drop')

            # Pipe mongodump to mongorestore
            dump_process = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE)
            restore_process = subprocess.Popen(restore_cmd, stdin=dump_process.stdout)

            dump_process.stdout.close()
            restore_process.communicate()

            return restore_process.returncode == 0

        except Exception as e:
            console.print(f"[yellow]⚠ mongodump/restore failed: {e}[/yellow]")
            return False

    def backup_before_copy(self, target_db: str, target_collection: str | None = None) -> str:
        """Create backup of target before copying"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if target_collection:
            # Backup single collection
            backup_name = f"{target_collection}_backup_{timestamp}"
            source_coll = self.target_client[target_db][target_collection]
            backup_coll = self.target_client[target_db][backup_name]

            console.print(f"[cyan]💾 Creating backup: {backup_name}[/cyan]")

            # Copy documents
            docs = list(source_coll.find())
            if docs:
                backup_coll.insert_many(docs)

            # Copy indexes
            for index in source_coll.list_indexes():
                if index['name'] != '_id_':
                    keys = index['key']
                    options = {k: v for k, v in index.items() if k not in ['key', 'v', 'ns']}
                    backup_coll.create_index(list(keys.items()), **options)

            console.print(f"[green]✅ Backup created: {backup_name}[/green]")
            return backup_name
        else:
            # Backup entire database using mongodump if available
            if shutil.which('mongodump'):
                backup_name = f"{target_db}_backup_{timestamp}"
                cmd = ['mongodump', '--uri', self.target_uri, '--db', target_db, '--archive', f'{backup_name}.archive']
                subprocess.run(cmd, check=True)
                console.print(f"[green]✅ Database backup created: {backup_name}.archive[/green]")
                return f"{backup_name}.archive"

        return ""

    def copy_multiple_collections(
        self,
        source_db: str,
        target_db: str,
        collections: list[str],
        drop_target: bool = False,
        create_backup: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
        force: bool = False,
        force_python: bool = False
    ) -> dict[str, Any]:
        """Copy multiple collections"""

        results = {}

        # Create backup if requested
        if create_backup and drop_target:
            backed_up = []
            for coll in collections:
                if coll in self.target_client[target_db].list_collection_names():
                    backup_name = self.backup_before_copy(target_db, coll)
                    backed_up.append(backup_name)

            if backed_up:
                console.print(f"[green]✅ Created backups: {', '.join(backed_up)}[/green]")

        # Copy each collection
        for collection_name in collections:
            console.print(f"\n[bold]📁 Copying: {collection_name}[/bold]")
            result = self.copy_collection_with_indexes(
                source_db, collection_name,
                target_db, collection_name,
                drop_target=drop_target,
                batch_size=batch_size,
                force=force,
                force_python=force_python
            )
            results[collection_name] = result

        return results

    def copy_entire_database(
        self,
        source_db: str,
        target_db: str,
        exclude_collections: list[str] | None = None,
        drop_target: bool = False,
        create_backup: bool = False,
        force: bool = False,
        force_python: bool = False
    ) -> dict[str, Any]:
        """Copy entire database with all collections and indexes"""

        exclude = exclude_collections or ['system.']
        source_database = self.source_client[source_db]
        target_database = self.target_client[target_db]

        # Get all collections
        collections = source_database.list_collection_names()
        collections = [c for c in collections if not any(c.startswith(ex) for ex in exclude)]

        console.print(f"[green]📚 Found {len(collections)} collections to copy[/green]")

        # Create backup if requested
        backup_name = None
        if create_backup and target_db in self.target_client.list_database_names():
            if force or Confirm.ask(f"[cyan]💾 Create backup of {target_db} before copying?[/cyan]"):
                backup_name = self.backup_before_copy(target_db)

        # Drop target database if requested
        if drop_target:
            if force or Confirm.ask(f"[red]⚠️  Drop entire target database {target_db}?[/red]"):
                self.target_client.drop_database(target_db)
                console.print("[yellow]🗑️  Dropped target database[/yellow]")

        results = {}

        for collection_name in collections:
            console.print(f"\n[bold]📁 Copying collection: {collection_name}[/bold]")
            result = self.copy_collection_with_indexes(
                source_db, collection_name,
                target_db, collection_name,
                drop_target=False,  # Already handled at DB level
                force=force,
                force_python=force_python
            )
            results[collection_name] = result

        return results

    def verify_copy(
        self,
        source_db: str,
        source_coll: str,
        target_db: str,
        target_coll: str,
        sample_size: int = DEFAULT_VERIFICATION_SAMPLE_SIZE
    ) -> dict[str, Any]:
        """Verify that copy was successful"""

        source_collection = self.source_client[source_db][source_coll]
        target_collection = self.target_client[target_db][target_coll]

        console.print("[cyan]🔍 Verifying copy...[/cyan]")

        # Compare counts
        source_count = source_collection.estimated_document_count()
        target_count = target_collection.estimated_document_count()

        # Compare indexes
        source_indexes = list(source_collection.list_indexes())
        target_indexes = list(target_collection.list_indexes())

        # Sample comparison
        sample_errors = []
        for doc in source_collection.aggregate([{'$sample': {'size': sample_size}}]):
            target_doc = target_collection.find_one({'_id': doc['_id']})
            if not target_doc:
                sample_errors.append(f"Missing document: {doc['_id']}")
            elif doc != target_doc:
                sample_errors.append(f"Document mismatch: {doc['_id']}")

        # Calculate checksums for small collections
        checksum_match = None
        if source_count < CHECKSUM_THRESHOLD:
            source_checksum = self._calculate_collection_checksum(source_collection)
            target_checksum = self._calculate_collection_checksum(target_collection)
            checksum_match = source_checksum == target_checksum

        return {
            'source_count': source_count,
            'target_count': target_count,
            'count_match': source_count == target_count,
            'source_indexes': len(source_indexes),
            'target_indexes': len(target_indexes),
            'index_match': len(source_indexes) == len(target_indexes),
            'sample_errors': sample_errors,
            'sample_match': len(sample_errors) == 0,
            'checksum_match': checksum_match
        }

    def _calculate_collection_checksum(self, collection) -> str:
        """Calculate checksum for collection data"""
        hasher = hashlib.sha256()
        for doc in collection.find().sort('_id', 1):
            # Remove variable fields like timestamps if needed
            doc_str = json.dumps(doc, sort_keys=True, default=str)
            hasher.update(doc_str.encode())
        return hasher.hexdigest()