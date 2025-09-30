"""
Centralized task runner to eliminate code duplication
Handles copy, backup, and restore tasks
"""

from typing import Any
from rich.console import Console
from .core import MongoAdvancedCopier
from .backup import BackupManager
from .formatting import format_number
from .utils import storage_config_to_url

console = Console()


def run_task(task_config: dict[str, Any], assume_yes: bool = False, force_python: bool = False) -> bool:
    """
    Execute a task based on its type

    Args:
        task_config: Task configuration dictionary
        assume_yes: Skip confirmations
        force_python: Force Python copy mode

    Returns:
        True if successful, False otherwise
    """
    task_type = task_config.get('type', 'copy')  # Default to copy for backward compatibility

    try:
        if task_type == 'backup':
            return run_backup_task(task_config)
        elif task_type == 'restore':
            return run_restore_task(task_config)
        else:
            return run_copy_task(task_config, assume_yes, force_python)
    except Exception as e:
        console.print(f"[red]❌ Task error: {e}[/red]")
        return False


def run_copy_task(task_config: dict[str, Any], assume_yes: bool = False, force_python: bool = False) -> bool:
    """Execute a copy task"""
    copier = MongoAdvancedCopier(task_config['source_uri'], task_config['target_uri'])

    try:
        copier.connect()

        if task_config.get('source_collection'):
            result = copier.copy_collection_with_indexes(
                task_config['source_db'],
                task_config['source_collection'],
                task_config['target_db'],
                task_config.get('target_collection', task_config['source_collection']),
                drop_target=task_config.get('drop_target', False),
                force=assume_yes,
                force_python=task_config.get('force_python', force_python)
            )
            method = result.get('method', 'unknown')
            console.print(f"[green]✅ Copied {format_number(result['documents_copied'])} documents (method: {method})[/green]")
        else:
            results = copier.copy_entire_database(
                task_config['source_db'],
                task_config['target_db'],
                drop_target=task_config.get('drop_target', False),
                force=assume_yes,
                force_python=task_config.get('force_python', force_python)
            )
            total_docs = sum(r['documents_copied'] for r in results.values())
            console.print(f"[green]✅ Copied {len(results)} collections, {format_number(total_docs)} documents[/green]")

        return True

    except Exception as e:
        console.print(f"[red]❌ Copy error: {e}[/red]")
        return False
    finally:
        copier.close()


def run_backup_task(task_config: dict[str, Any]) -> bool:
    """Execute a backup task"""
    # Pass storage config directly (can be dict or URL string)
    backup_mgr = BackupManager(
        task_config['mongo_uri'],
        task_config['storage_url']
    )

    result = backup_mgr.backup_database(
        task_config['database'],
        task_config.get('collections'),
        custom_name=task_config.get('custom_name')
    )

    if result['success']:
        console.print(f"[green]✅ Backup completed![/green]")
        console.print(f"  File: {result['filename']}")
        console.print(f"  Size: {result['size_human']}")
        console.print(f"  Documents: {format_number(result['documents'])}")
        console.print(f"  Collections: {result['collections']}")
        backup_mgr.close()
        return True
    else:
        console.print(f"[red]❌ Backup failed: {result.get('error')}[/red]")
        backup_mgr.close()
        return False


def run_restore_task(task_config: dict[str, Any]) -> bool:
    """Execute a restore task"""
    # Pass storage config directly (can be dict or URL string)
    backup_mgr = BackupManager(
        task_config['mongo_uri'],
        task_config['storage_url']
    )

    result = backup_mgr.restore_database(
        task_config['backup_file'],
        task_config.get('target_database'),
        task_config.get('drop_target', False)
    )

    if result['success']:
        console.print(f"[green]✅ Restore completed![/green]")
        console.print(f"  Database: {result['database']}")
        console.print(f"  Documents: {format_number(result['documents'])}")
        console.print(f"  Collections: {result['collections']}")
        backup_mgr.close()
        return True
    else:
        console.print(f"[red]❌ Restore failed: {result.get('error')}[/red]")
        backup_mgr.close()
        return False


def display_task_summary(task_config: dict[str, Any]) -> None:
    """Display task configuration summary"""
    task_type = task_config.get('type', 'copy')

    if task_type == 'backup':
        console.print(f"[bold]Type:[/bold] BACKUP")
        console.print(f"[bold]Source:[/bold] {task_config['mongo_uri']}")
        console.print(f"[bold]Database:[/bold] {task_config['database']}")
        console.print(f"[bold]Collections:[/bold] {task_config.get('collections', 'ALL')}")
        console.print(f"[bold]Destination:[/bold] {task_config['storage_url']}")

    elif task_type == 'restore':
        console.print(f"[bold]Type:[/bold] RESTORE")
        console.print(f"[bold]Backup:[/bold] {task_config['backup_file']}")
        console.print(f"[bold]Target:[/bold] {task_config['mongo_uri']}")
        console.print(f"[bold]Database:[/bold] {task_config.get('target_database', 'from backup')}")
        console.print(f"[bold]Drop Target:[/bold] {'Yes' if task_config.get('drop_target') else 'No'}")
        console.print(f"[bold]Storage:[/bold] {task_config['storage_url']}")

    else:
        console.print(f"[bold]Type:[/bold] COPY")
        console.print(f"[bold]Source:[/bold] {task_config['source_uri']}")
        console.print(f"[bold]Target:[/bold] {task_config['target_uri']}")
        console.print(f"[bold]Database:[/bold] {task_config['source_db']} → {task_config['target_db']}")
        if task_config.get('source_collection'):
            console.print(
                f"[bold]Collection:[/bold] {task_config['source_collection']} → "
                f"{task_config.get('target_collection', task_config['source_collection'])}"
            )
        console.print(f"[bold]Drop Target:[/bold] {'Yes' if task_config.get('drop_target') else 'No'}")