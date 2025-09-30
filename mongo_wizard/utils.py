"""
Utility functions for MongoDB Wizard
"""

import shutil
from typing import Any

from pymongo import MongoClient
from rich.console import Console
from rich.table import Table
from .constants import (
    DEFAULT_MONGO_TIMEOUT,
    QUICK_CHECK_TIMEOUT,
    LONG_OPERATION_TIMEOUT
)
# Re-export for backward compatibility
from .formatting import format_document_count

console = Console()


# ============================================================================
# Storage Helper Functions
# ============================================================================

def storage_config_to_url(storage_url_or_config: str | dict[str, Any]) -> str:
    """
    Convert storage config dict to URL string.
    If already a string, return as-is.

    Args:
        storage_url_or_config: Storage URL string or config dict

    Returns:
        Storage URL string

    Examples:
        >>> storage_config_to_url("/local/path")
        '/local/path'
        >>> storage_config_to_url({"type": "ssh", "user": "root", "host": "server.com", "path": "/backup"})
        'ssh://root@server.com:22/backup'
    """
    # Already a URL string
    if isinstance(storage_url_or_config, str):
        return storage_url_or_config

    # Convert config dict to URL
    config = storage_url_or_config
    config_type = config.get('type', 'local')

    if config_type == 'ssh':
        user = config['user']
        host = config['host']
        port = config.get('port', 22)
        path = config.get('path', '/')
        # Ensure path starts with / for URL format
        if not path.startswith('/'):
            path = '/' + path
        return f"ssh://{user}@{host}:{port}{path}"

    elif config_type == 'ftp':
        user = config['user']
        password = config['password']
        host = config['host']
        port = config.get('port', 21)
        path = config.get('path', '/')
        if not path.startswith('/'):
            path = '/' + path
        return f"ftp://{user}:{password}@{host}:{port}{path}"

    else:  # local
        return config.get('path', '/')


# ============================================================================
# MongoDB Connection Helpers
# ============================================================================

def connect_mongo(uri: str, timeout: int = DEFAULT_MONGO_TIMEOUT) -> MongoClient:
    """
    Connect to MongoDB and verify connection with ping.
    Raises exception if connection fails.

    Args:
        uri: MongoDB connection URI
        timeout: Connection timeout in milliseconds (default: 5000)

    Returns:
        Connected MongoClient instance

    Raises:
        ConnectionFailure: If connection fails
    """
    client = MongoClient(uri, serverSelectionTimeoutMS=timeout)
    client.admin.command('ping')  # Verify connection
    return client


def check_mongodb_tools() -> dict[str, bool]:
    """Check if MongoDB tools are available"""
    tools = {
        'mongodump': shutil.which('mongodump') is not None,
        'mongorestore': shutil.which('mongorestore') is not None,
        'mongo': shutil.which('mongo') is not None,
        'mongosh': shutil.which('mongosh') is not None
    }
    return tools


def test_connection(uri: str, timeout: int = DEFAULT_MONGO_TIMEOUT) -> tuple[bool, str]:
    """Test MongoDB connection and return status with database count"""
    try:
        client = connect_mongo(uri, timeout)
        db_count = len(client.list_database_names())
        client.close()
        return True, f"OK ({db_count} databases)"
    except Exception as e:
        return False, str(e)


def parse_collection_selection(selection: str, max_value: int) -> list[int]:
    """
    Parse collection selection string like "1,3-5,7" into list of indices
    """
    if selection.upper() == 'ALL':
        return list(range(max_value))

    indices = []
    parts = selection.split(',')

    for part in parts:
        part = part.strip()
        if '-' in part:
            # Range like "3-5"
            start, end = part.split('-')
            try:
                start_idx = int(start) - 1
                end_idx = int(end)
                indices.extend(range(start_idx, end_idx))
            except ValueError:
                console.print(f"[red]Invalid range: {part}[/red]")
        else:
            # Single number
            try:
                idx = int(part) - 1
                if 0 <= idx < max_value:
                    indices.append(idx)
            except ValueError:
                console.print(f"[red]Invalid number: {part}[/red]")

    # Remove duplicates and sort
    return sorted(list(set(indices)))


def build_connection_uri(host: str, port: int = 27017, username: str | None = None, password: str | None = None, auth_db: str | None = None) -> str:
    """Build MongoDB connection URI from components"""
    auth = ""
    if username and password:
        auth = f"{username}:{password}@"

    uri = f"mongodb://{auth}{host}:{port}/"

    if auth_db:
        uri += f"{auth_db}"

    return uri


def display_copy_summary(source_db: str, target_db: str, collections: list[str], doc_counts: dict[str, int], drop_target: bool = False):
    """Display a summary of the copy operation"""
    from .formatting import format_docs

    table = Table(title="📋 Copy Summary", show_header=True)
    table.add_column("Collection", style="cyan")
    table.add_column("Documents", justify="right", style="green")
    table.add_column("Status", style="yellow")

    total_docs = 0
    for coll in collections:
        count = doc_counts.get(coll, 0)
        total_docs += count
        status = "Drop & Replace" if drop_target else "Merge"
        table.add_row(coll, format_docs(count), status)

    console.print(table)
    console.print(f"\n[bold]Total:[/bold] {format_docs(total_docs)} documents")
    console.print(f"[bold]Source:[/bold] {source_db}")


def format_task_table_row(task_name: str, task_config: dict) -> tuple[str, str, str]:
    """
    Format a task for display in a table with colors and statistics.
    Returns: (task_name, source_target_display, collection_display)

    Supports both 'copy' and 'backup'/'restore' task types.
    """
    from urllib.parse import urlparse

    # Check task type - backup/restore tasks have different structure
    task_type = task_config.get('type', 'copy')

    if task_type in ('backup', 'restore'):
        # Backup/restore tasks don't have source_uri/target_uri
        # Return simplified display
        if task_type == 'backup':
            display = f"[cyan]BACKUP:[/cyan] {task_config.get('database', 'N/A')}"
            coll_display = task_config.get('collections', 'ALL')
            if isinstance(coll_display, list):
                coll_display = f"{len(coll_display)} collections"
        else:  # restore
            display = f"[cyan]RESTORE:[/cyan] {task_config.get('backup_file', 'N/A')}"
            coll_display = task_config.get('target_database', 'from backup')

        return task_name, display, str(coll_display)

    # Copy task - original logic
    source_uri = task_config.get('source_uri')
    target_uri = task_config.get('target_uri')
    source_db = task_config.get('source_db')
    target_db = task_config.get('target_db')

    if not all([source_uri, target_uri, source_db, target_db]):
        # Invalid task config, return safe defaults
        return task_name, "[red]Invalid task config[/red]", "N/A"

    # Extract hostnames
    try:
        source_parsed = urlparse(source_uri)
        target_parsed = urlparse(target_uri)
        source_host = source_parsed.hostname or 'localhost'
        target_host = target_parsed.hostname or 'localhost'
    except Exception as e:
        console.print(f"[dim]Could not parse URIs: {e}[/dim]")
        source_host = 'unknown'
        target_host = 'unknown'

    # Handle collection display
    coll = task_config.get('source_collection', 'ALL')
    if isinstance(coll, list):
        coll_display = f"{len(coll)} collections"
        collection_list = coll
    elif coll and coll != 'ALL':
        coll_display = str(coll)
        collection_list = [coll]
    else:
        coll_display = 'ALL'
        collection_list = None

    # Get row counts with status
    source_count_int = 0
    target_count_int = 0
    source_count = ""
    target_count = ""

    try:
        # Show status while counting
        with console.status(f"[dim]Counting docs for {task_name}...[/dim]"):
            # Source count
            client = MongoClient(source_uri, serverSelectionTimeoutMS=3000)
            db = client[source_db]

            if collection_list:
                count = sum(db[c].estimated_document_count() for c in collection_list if c in db.list_collection_names())
            else:
                count = sum(db[c].estimated_document_count() for c in db.list_collection_names())

            source_count_int = count if count > 0 else 0
            source_count = f"~{source_count_int:_} docs".replace('_', '_') if source_count_int > 0 else "0 docs"
            client.close()

            # Check source connection
            try:
                client = MongoClient(source_uri, serverSelectionTimeoutMS=QUICK_CHECK_TIMEOUT)
                client.server_info()
                client.close()
                source_host_color = "green"
            except Exception:
                source_host_color = "red"

            # Target count
            client = MongoClient(target_uri, serverSelectionTimeoutMS=3000)
            try:
                db = client[target_db]
                if collection_list:
                    count = sum(db[c].estimated_document_count() for c in collection_list if c in db.list_collection_names())
                else:
                    count = sum(db[c].estimated_document_count() for c in db.list_collection_names())

                target_count_int = count if count > 0 else 0
                target_count = f"~{target_count_int:_} docs".replace('_', '_') if target_count_int > 0 else "0 docs"
            except Exception:
                target_count = "? docs"
            finally:
                client.close()

            # Check target connection
            try:
                client = MongoClient(target_uri, serverSelectionTimeoutMS=QUICK_CHECK_TIMEOUT)
                client.server_info()
                client.close()
                target_host_color = "green"
            except Exception:
                target_host_color = "red"

            # Color target count based on comparison
            if target_count_int < source_count_int:
                target_count = f"[green]{target_count}[/green]"
            elif target_count_int > source_count_int:
                target_count = f"[red][blink]{target_count}[/blink][/red]"

    except Exception:
        # Silently fall back to simple display
        source_host_color = "white"
        target_host_color = "white"
        source_count = ""
        target_count = ""

    # Build display string with colors
    source_part = f"[{source_host_color}]{source_host}[/{source_host_color}]:[blue]{source_db}[/blue] {source_count}".strip()
    target_part = f"[{target_host_color}]{target_host}[/{target_host_color}]:[blue]{target_db}[/blue] {target_count}".strip()
    source_target = f"{source_part} → {target_part}"

    return task_name, source_target, coll_display