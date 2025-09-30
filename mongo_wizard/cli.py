#!/usr/bin/env python
"""
mongo-wizard CLI - Main entry point

Usage:
    # Interactive mode
    python cli.py

    # Task execution
    python cli.py -t daily_backup
    python cli.py -t daily_backup -y  # Automated mode

    # Direct copy
    python cli.py -s mongodb://source/db/coll -t mongodb://target/db/coll

    # As module
    python -m mongo_wizard
"""

import os
import sys
import click
from . import __version__
from .wizard import MongoWizard
from .settings import SettingsManager
from .core import MongoAdvancedCopier
from .utils import check_mongodb_tools, test_connection
from .task_runner import run_task, display_task_summary
from .backup import BackupManager, BackupTask
from .formatting import format_number
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

console = Console()


@click.command()
@click.version_option(version=__version__)
@click.option('-s', '--source', help='Source MongoDB URI')
@click.option('-t', '--target', help='Target MongoDB URI')
@click.option('--source-db', help='Source database name')
@click.option('--target-db', help='Target database name (defaults to source-db)')
@click.option('--source-collection', help='Source collection (omit for all collections)')
@click.option('--drop-target', is_flag=True, help='Drop target before copying')
@click.option('--dry-run', is_flag=True, help='Show what would be done without executing')
@click.option('--verify', is_flag=True, help='Verify copy after completion')
@click.option('--list-tasks', is_flag=True, help='List saved tasks')
@click.option('--list-hosts', is_flag=True, help='List saved hosts')
@click.option('--task', help='Run a saved task')
@click.option('-f', '--force', is_flag=True, help='Skip confirmation when running task (deprecated, use -y)')
@click.option('-y', '--yes', 'assume_yes', is_flag=True, help='Assume yes to all prompts (fully automated)')
@click.option('--force-python', is_flag=True, help='Force Python copy instead of mongodump (slower but more control)')
@click.option('--verify-connection', help='Test connection to MongoDB URI')
@click.option('--backup', help='Backup a database (format: mongodb://uri/database)')
@click.option('--backup-to', help='Backup destination (local path or ssh://user@host/path)')
@click.option('--restore', help='Restore from backup file')
@click.option('--restore-to', help='Restore target MongoDB URI')
def main(source, target, source_db, target_db, source_collection, drop_target,
         dry_run, verify, list_tasks, list_hosts, task, force, assume_yes, force_python, verify_connection,
         backup, backup_to, restore, restore_to):
    """
    mongo-wizard - Advanced MongoDB copy and migration tool

    Examples:

    Interactive mode:
        mw

    Run saved task:
        mw --task daily_backup
        mw --task daily_backup -y  # Automated mode

    Direct copy:
        mw -s mongodb://localhost/db1 -t mongodb://remote/db2 --source-collection users

    List tasks:
        mw --list-tasks

    List hosts:
        mw --list-hosts
    """

    # Check MongoDB tools on first run
    tools = check_mongodb_tools()
    if not any(tools.values()):
        console.print("[yellow]⚠ MongoDB tools not found. Some features may be limited.[/yellow]")
        console.print("[dim]Install with: brew install mongodb-database-tools[/dim]\n")

    # Connection verification mode
    if verify_connection:
        success, message = test_connection(verify_connection)
        if success:
            console.print(f"[green]✅ {message}[/green]")
        else:
            console.print(f"[red]❌ {message}[/red]")
        return

    # List tasks mode
    if list_tasks:
        from .utils import format_task_table_row

        settings_manager = SettingsManager()
        tasks = settings_manager.list_tasks()

        if not tasks:
            console.print("[yellow]No saved tasks found[/yellow]")
            console.print("[dim]Create tasks using interactive mode[/dim]")
            return

        # Title box
        from rich.panel import Panel
        console.print()
        console.print(Panel("[bold blue]⚙️  SAVED TASKS[/bold blue]", expand=False))
        console.print()

        table = Table(title="Saved Tasks", box=box.ROUNDED)
        table.add_column("#", style="dim", width=4)
        table.add_column("Name", style="cyan")
        table.add_column("Source → Target", style="green")
        table.add_column("Collection", style="magenta")

        # Format each task with statistics
        for idx, (name, task) in enumerate(tasks.items(), 1):
            task_name, source_target, coll_display = format_task_table_row(name, task)
            table.add_row(
                str(idx),
                task_name,
                source_target,
                coll_display
            )

        console.print(table)
        console.print()
        console.print(f"[dim]Run a task with: mw --task <name>[/dim]")
        console.print(f"[dim]Automated mode: mw --task <name> -y[/dim]")
        return

    # List hosts mode
    if list_hosts:
        settings_manager = SettingsManager()
        hosts = settings_manager.list_hosts()

        if not hosts:
            console.print("[yellow]No saved hosts found[/yellow]")
            console.print("[dim]Add hosts using interactive mode (mw)[/dim]")
            return

        table = Table(title="💾 Saved Hosts", box=box.ROUNDED)
        table.add_column("Name", style="cyan")
        table.add_column("URI", style="green")
        table.add_column("Status", style="yellow")

        for name, uri in hosts.items():
            # Quick connection test
            is_online, msg = test_connection(uri, timeout=1000)
            status = "🟢 Online" if is_online else "🔴 Offline"

            # Mask password in URI for display
            display_uri = uri
            if '@' in uri and ':' in uri.split('@')[0]:
                parts = uri.split('@')
                user_pass = parts[0].split('://')[-1]
                user = user_pass.split(':')[0]
                display_uri = uri.replace(user_pass, f"{user}:****")

            table.add_row(name, display_uri, status)

        console.print(table)
        console.print(f"\n[dim]Total: {len(hosts)} hosts[/dim]")
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
        display_task_summary(task_config)

        if not force and not assume_yes:
            if not click.confirm("\nExecute this task?"):
                console.print("[red]Cancelled[/red]")
                sys.exit(0)

        # Execute task using centralized runner
        success = run_task(task_config, assume_yes, force_python)

        if success:
            console.print("[green]✅ Task completed successfully![/green]")
        else:
            sys.exit(1)

        return

    # Backup mode
    if backup:
        if not backup_to:
            console.print("[red]❌ --backup-to is required for backup[/red]")
            sys.exit(1)

        # Parse backup URI (format: mongodb://uri/database)
        if '/' not in backup.replace('mongodb://', ''):
            console.print("[red]❌ Backup URI must include database (mongodb://host/database)[/red]")
            sys.exit(1)

        # Extract database from URI
        parts = backup.rsplit('/', 1)
        mongo_uri = parts[0]
        database = parts[1]

        console.print(f"[cyan]📦 Backup mode[/cyan]")
        console.print(f"Source: {mongo_uri}")
        console.print(f"Database: {database}")
        console.print(f"Destination: {backup_to}")

        if not assume_yes:
            if not click.confirm("\nProceed with backup?"):
                console.print("[red]Cancelled[/red]")
                sys.exit(0)

        backup_mgr = BackupManager(mongo_uri, backup_to)
        result = backup_mgr.backup_database(database)

        if result['success']:
            console.print(f"[green]✅ Backup completed![/green]")
            console.print(f"File: {result['filename']}")
            console.print(f"Size: {result['size_human']}")
            console.print(f"Documents: {format_number(result['documents'])}")
        else:
            console.print(f"[red]❌ Backup failed: {result.get('error')}[/red]")
            sys.exit(1)

        backup_mgr.close()
        return

    # Restore mode
    if restore:
        if not restore_to:
            console.print("[red]❌ --restore-to is required for restore[/red]")
            sys.exit(1)

        console.print(f"[cyan]📥 Restore mode[/cyan]")
        console.print(f"Backup: {restore}")
        console.print(f"Target: {restore_to}")

        if not assume_yes:
            if not click.confirm("\nProceed with restore?"):
                console.print("[red]Cancelled[/red]")
                sys.exit(0)

        # Determine storage location from backup path
        storage_url = os.path.dirname(restore)
        backup_mgr = BackupManager(restore_to, storage_url)
        result = backup_mgr.restore_database(restore, drop_target=drop_target)

        if result['success']:
            console.print(f"[green]✅ Restore completed![/green]")
            console.print(f"Database: {result['database']}")
            console.print(f"Documents: {format_number(result['documents'])}")
        else:
            console.print(f"[red]❌ Restore failed: {result.get('error')}[/red]")
            sys.exit(1)

        backup_mgr.close()
        return

    # Direct copy mode
    if source and target:
        if not source_db:
            console.print("[red]❌ --source-db is required for direct copy[/red]")
            sys.exit(1)

        target_db = target_db or source_db

        console.print(f"[cyan]📋 Direct copy mode[/cyan]")
        console.print(f"Source: {source}/{source_db}/{source_collection or 'ALL'}")
        console.print(f"Target: {target}/{target_db}/{source_collection or 'ALL'}")

        if drop_target:
            console.print("[yellow]⚠ Will drop target before copying[/yellow]")

        if not assume_yes:
            if not click.confirm("\nProceed with copy?"):
                console.print("[red]Cancelled[/red]")
                sys.exit(0)

        copier = MongoAdvancedCopier(source, target)

        try:
            copier.connect()

            if source_collection:
                result = copier.copy_collection_with_indexes(
                    source_db, source_collection,
                    target_db, source_collection,
                    drop_target=drop_target,
                    force=assume_yes,
                    force_python=force_python
                )
                method = result.get('method', 'unknown')
                console.print(f"[green]✅ Copied {format_number(result['documents_copied'])} documents (method: {method})[/green]")
            else:
                results = copier.copy_entire_database(
                    source_db, target_db,
                    drop_target=drop_target,
                    force=assume_yes,
                    force_python=force_python
                )
                total_docs = sum(r['documents_copied'] for r in results.values())
                console.print(f"[green]✅ Copied {len(results)} collections, {format_number(total_docs)} documents[/green]")

            if verify:
                # Run verification
                if source_collection:
                    verify_result = copier.verify_copy(
                        source_db, source_collection,
                        target_db, source_collection
                    )
                    if verify_result['count_match'] and verify_result['index_match']:
                        console.print("[green]✅ Verification passed![/green]")
                    else:
                        console.print("[yellow]⚠ Verification issues found[/yellow]")
                        console.print(f"  Count match: {verify_result['count_match']}")
                        console.print(f"  Index match: {verify_result['index_match']}")

        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/red]")
            sys.exit(1)
        finally:
            copier.close()

        return

    # Default: interactive mode
    wizard = MongoWizard()
    wizard.run()


if __name__ == '__main__':
    # If no arguments, launch interactive mode directly
    if len(sys.argv) == 1:
        wizard = MongoWizard()
        wizard.run()
    else:
        main()
