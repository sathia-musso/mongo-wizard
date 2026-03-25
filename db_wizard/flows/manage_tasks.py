import json
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm, Prompt
from rich import box

console = Console()

class ManageTasksFlow:
    """Flow for managing saved execution tasks"""
    def __init__(self, wizard_context):
        self.wizard = wizard_context

    def run(self):
        while True:
            self.wizard.clear_screen()
            console.print(Panel("[bold cyan]⚙ MANAGE SAVED TASKS[/bold cyan]", style="cyan", expand=False))

            saved_tasks_dict = self.wizard.settings_manager.list_tasks()
            saved_tasks_list = list(saved_tasks_dict.items())

            if not saved_tasks_list:
                console.print("[yellow]No saved tasks yet![/yellow]")
            else:
                from ..utils import format_task_table_row

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
            if saved_tasks_list:
                console.print("  [cyan]1.[/cyan] Run task")
                console.print("  [cyan]2.[/cyan] Delete task")
                console.print("  [cyan]3.[/cyan] Export tasks to file")
            console.print("  [cyan]4.[/cyan] Back to main menu")

            choices = ["1", "2", "3", "4"] if saved_tasks_list else ["4"]
            choice = Prompt.ask("Choose option", choices=choices)

            if choice == "1" and saved_tasks_list:
                self.wizard.run_saved_task()
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
                    if self.wizard.settings_manager.delete_task(task_name):
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
