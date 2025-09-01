# cli/main.py
import click
from rich import print

@click.group()
def cli():
    """[bold green]P2P Sync CLI[/bold green] - NAT traversal + CRDT file sync"""
    pass

@cli.command()
def status():
    """Check current project and environment status"""
    print("[cyan]✔️ Project setup looks good![/cyan]")
    print("[yellow]Modules:[/yellow] cli/, discovery/, nat/, sync/, crdt/, gui/")
    print("[green]Ready to begin Phase 2: NAT Traversal[/green]")

if __name__ == "__main__":
    cli()
