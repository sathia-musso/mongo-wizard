import json
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm

from ..engine import EngineFactory
from ..formatting import format_number

console = Console()

class BrowseWizardFlow:
    """Handles the browse database workflow logic decoupled from main wizard"""

    def __init__(self, wizard_context):
        self.wizard = wizard_context

    def run(self):
        """Browse database interactively"""
        self.wizard.clear_screen()
        console.print(Panel("[bold cyan]🔍 DATABASE BROWSER[/bold cyan]", style="cyan"))

        uri = self.wizard.select_or_add_host("browse")
        if not uri:
            return
            
        try:
            engine = EngineFactory.create(uri)
            engine.connect()
        except Exception as e:
            console.print(f"[red]❌ Connection failed: {e}[/red]")
            Prompt.ask("Press Enter to continue")
            return

        db_name = self.wizard.select_database(engine, "browse")

        while True:
            self.wizard.clear_screen()
            console.print(f"[bold cyan]📚 Browsing: {db_name}[/bold cyan]\n")

            coll_name = self.wizard.select_collection(engine, db_name, "browse", allow_all=False)
            if not coll_name:
                break

            # Show table/collection info
            term = engine.table_term
            row_count = engine.count_rows(db_name, coll_name)
            console.print(f"\n[bold]📁 {term.capitalize()}: {coll_name}[/bold]")
            console.print(f"Rows: {format_number(row_count)}")

            # Show sample data
            if Confirm.ask("\nShow sample data?"):
                if engine.scheme == 'mongodb':
                    # MongoDB: pretty JSON
                    collection = engine.client[db_name][coll_name]
                    samples = list(collection.find().limit(3))
                    for i, doc in enumerate(samples, 1):
                        console.print(f"\n[yellow]Document {i}:[/yellow]")
                        console.print(json.dumps(doc, indent=2, default=str))
                else:
                    # MySQL/Postgres/Redis: Rich Table
                    columns, rows = engine.sample_rows(db_name, coll_name, limit=5)
                    if columns:
                        sample_table = Table(title=f"Sample from {coll_name}", box=box.ROUNDED)
                        for col in columns:
                            sample_table.add_column(col, style="cyan", overflow="fold")
                        for row in rows:
                            sample_table.add_row(*[cell[:80] for cell in row])
                        console.print(sample_table)
                    else:
                        console.print("[yellow]No data found[/yellow]")

            # Show indexes (MongoDB-specific for now)
            if engine.scheme == 'mongodb' and Confirm.ask("\nShow indexes?"):
                collection = engine.client[db_name][coll_name]
                indexes = list(collection.list_indexes())
                console.print("\n[cyan]Indexes:[/cyan]")
                for idx in indexes:
                    console.print(f"  - {idx['name']}: {idx['key']}")

            if not Confirm.ask("\n[yellow]Browse another collection?[/yellow]"):
                break

        engine.close()