import sys
import subprocess
from rich import box
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm
from .engine import EngineFactory

console = Console()

def _check_tools(uri: str = "mongodb://localhost") -> dict[str, bool]:
    """Check CLI tools for the given engine type."""
    try:
        engine = EngineFactory.create(uri)
        return engine.check_tools()
    except ValueError:
        return {}

def check_system_requirements():
    """Check system requirements at startup"""
    console.print("[cyan]🔍 Checking system requirements...[/cyan]\n")

    # Check tools for both engines
    mongo_tools = _check_tools("mongodb://localhost")
    mysql_tools = _check_tools("mysql://localhost")
    postgres_tools = _check_tools("postgres://localhost")
    redis_tools = _check_tools("redis://localhost")

    all_good = True
    requirements_table = Table(title="System Requirements", box=box.ROUNDED)
    requirements_table.add_column("Component", style="cyan")
    requirements_table.add_column("Status", style="green")
    requirements_table.add_column("Notes", style="yellow")

    # -- Python packages --
    try:
        import pymongo
        requirements_table.add_row("PyMongo", "✅ Installed", f"v{pymongo.version}")
    except ImportError:
        requirements_table.add_row("PyMongo", "❌ Missing", "pip install pymongo")
        all_good = False

    try:
        import importlib.metadata
        rich_version = importlib.metadata.version('rich')
        requirements_table.add_row("Rich", "✅ Installed", f"v{rich_version}")
    except Exception:
        requirements_table.add_row("Rich", "⚠️  Missing", "pip install rich")

    # -- MongoDB tools --
    if mongo_tools.get('mongodump'):
        try:
            result = subprocess.run(['mongodump', '--version'], capture_output=True, text=True)
            version = result.stdout.split('\n')[0] if result.stdout else ""
            requirements_table.add_row("mongodump", "✅ Installed", version[:40])
        except Exception:
            requirements_table.add_row("mongodump", "✅ Installed", "")
    else:
        requirements_table.add_row("mongodump", "⚠️  Missing", "brew install mongodb-database-tools")

    if mongo_tools.get('mongorestore'):
        requirements_table.add_row("mongorestore", "✅ Installed", "")
    else:
        requirements_table.add_row("mongorestore", "⚠️  Missing", "Optional")

    if mongo_tools.get('mongosh'):
        requirements_table.add_row("mongosh", "✅ Installed", "")
    else:
        requirements_table.add_row("mongosh", "⚠️  Missing", "Optional")

    # -- MySQL tools --
    if mysql_tools.get('mysql'):
        try:
            result = subprocess.run(['mysql', '--version'], capture_output=True, text=True)
            version = result.stdout.strip() if result.stdout else ""
            requirements_table.add_row("mysql", "✅ Installed", version[:40])
        except Exception:
            requirements_table.add_row("mysql", "✅ Installed", "")
    else:
        requirements_table.add_row("mysql", "⚠️  Missing", "Required for MySQL operations")

    if mysql_tools.get('mysqldump'):
        try:
            result = subprocess.run(['mysqldump', '--version'], capture_output=True, text=True)
            version = result.stdout.strip() if result.stdout else ""
            requirements_table.add_row("mysqldump", "✅ Installed", version[:40])
        except Exception:
            requirements_table.add_row("mysqldump", "✅ Installed", "")
    else:
        requirements_table.add_row("mysqldump", "⚠️  Missing", "Required for MySQL backup/copy")
        
    # -- Postgres tools --
    if postgres_tools.get('psql'):
        requirements_table.add_row("psql", "✅ Installed", "")
    else:
        requirements_table.add_row("psql", "⚠️  Missing", "Required for Postgres operations")
    if postgres_tools.get('pg_dump'):
        requirements_table.add_row("pg_dump", "✅ Installed", "")
    else:
        requirements_table.add_row("pg_dump", "⚠️  Missing", "Required for Postgres backup/copy")
        
    # -- Redis tools --
    if redis_tools.get('redis-cli'):
        requirements_table.add_row("redis-cli", "✅ Installed", "")
    else:
        requirements_table.add_row("redis-cli", "⚠️  Missing", "Required for Redis operations")

    console.print(requirements_table)

    # Show install hints for missing tools
    missing_mongo = not mongo_tools.get('mongodump') or not mongo_tools.get('mongorestore')
    missing_mysql = not mysql_tools.get('mysql') or not mysql_tools.get('mysqldump')
    missing_postgres = not postgres_tools.get('psql') or not postgres_tools.get('pg_dump')
    missing_redis = not redis_tools.get('redis-cli')

    if missing_mongo or missing_mysql or missing_postgres or missing_redis:
        console.print("\n[yellow]⚠️  Some tools are missing:[/yellow]")
        if missing_mongo:
            if sys.platform == "darwin":
                console.print("  [cyan]MongoDB:[/cyan] brew install mongodb-database-tools")
            else:
                console.print("  [cyan]MongoDB:[/cyan] apt-get install mongodb-database-tools")
        if missing_mysql:
            if sys.platform == "darwin":
                console.print("  [cyan]MySQL:[/cyan]   brew install mysql-client")
            else:
                console.print("  [cyan]MySQL:[/cyan]   apt-get install mysql-client")
        if missing_postgres:
            if sys.platform == "darwin":
                console.print("  [cyan]Postgres:[/cyan] brew install postgresql")
            else:
                console.print("  [cyan]Postgres:[/cyan] apt-get install postgresql-client")
        if missing_redis:
            if sys.platform == "darwin":
                console.print("  [cyan]Redis:[/cyan]    brew install redis")
            else:
                console.print("  [cyan]Redis:[/cyan]    apt-get install redis-tools")

    if not all_good:
        console.print("\n[red]❌ Missing required Python packages![/red]")
        console.print("Run: [cyan]pip install pymongo rich[/cyan]")
        sys.exit(1)

    console.print("\n[green]✅ All required components are installed![/green]")

    if missing_mongo or missing_mysql or missing_postgres or missing_redis:
        if not Confirm.ask("\n[yellow]Continue with missing tools?[/yellow]"):
            sys.exit(0)
    else:
        Prompt.ask("\nPress Enter to continue")
    console.clear()
