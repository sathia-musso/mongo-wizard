#!/usr/bin/env python
"""
Test performance difference between mongodump and Python copy
"""

import time
from mongo_wizard import MongoAdvancedCopier
from pymongo import MongoClient
from rich.console import Console

console = Console()


def setup_test_data():
    """Create test collection with data"""
    client = MongoClient("mongodb://localhost:27017")

    # Clean up
    client.drop_database("perf_test")

    # Create test data
    db = client["perf_test"]
    docs = [{"_id": i, "data": f"Document {i}" * 10} for i in range(100000)]  # 100k docs for better test
    db.test_collection.insert_many(docs)
    db.test_collection.create_index("data")

    console.print(f"[green]✅ Created test collection with {len(docs)} documents[/green]")
    client.close()


def test_mongodump():
    """Test with mongodump (default)"""
    copier = MongoAdvancedCopier(
        "mongodb://localhost:27017",
        "mongodb://localhost:27017"
    )
    copier.connect()

    start = time.time()

    result = copier.copy_collection_with_indexes(
        "perf_test", "test_collection",
        "perf_test", "copy_mongodump",
        drop_target=True,
        force=True,
        force_python=False  # Use mongodump (default)
    )

    elapsed = time.time() - start

    copier.close()

    return elapsed, result


def test_python():
    """Test with Python copy"""
    copier = MongoAdvancedCopier(
        "mongodb://localhost:27017",
        "mongodb://localhost:27017"
    )
    copier.connect()

    start = time.time()

    result = copier.copy_collection_with_indexes(
        "perf_test", "test_collection",
        "perf_test", "copy_python",
        drop_target=True,
        force=True,
        force_python=True  # Force Python
    )

    elapsed = time.time() - start

    copier.close()

    return elapsed, result


def main():
    console.print("\n[bold]mongo-wizard - Performance Test[/bold]\n")

    # Check MongoDB
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        client.close()
    except Exception:
        console.print("[red]❌ MongoDB not running on localhost:27017[/red]")
        return

    # Setup test data
    console.print("[cyan]Setting up test data...[/cyan]")
    setup_test_data()

    # Test mongodump
    console.print("\n[cyan]Testing mongodump/mongorestore (default)...[/cyan]")
    time1, result1 = test_mongodump()

    # Test Python
    console.print("\n[cyan]Testing Python copy (--force-python)...[/cyan]")
    time2, result2 = test_python()

    # Results
    console.print("\n[bold]📊 Performance Results:[/bold]\n")

    console.print(f"mongodump/mongorestore: [green]{time1:.2f}s[/green] (method: {result1['method']})")
    console.print(f"Python copy: [yellow]{time2:.2f}s[/yellow] (method: {result2['method']})")

    if time1 < time2:
        speedup = time2 / time1
        console.print(f"\n[bold green]✅ mongodump is {speedup:.1f}x faster![/bold green]")
    else:
        console.print("\n[yellow]⚠ Python was faster (mongodump might not be available)[/yellow]")

    # Cleanup
    client = MongoClient("mongodb://localhost:27017")
    client.drop_database("perf_test")
    client.close()


if __name__ == "__main__":
    main()