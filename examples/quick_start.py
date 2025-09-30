#!/usr/bin/env python
"""
mongo-wizard - Quick Start Examples

Simple, runnable examples to get started quickly.
Run with: python examples/quick_start.py
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mongo_wizard import MongoAdvancedCopier
from rich.console import Console

console = Console()


def example_1_simple_copy():
    """Example 1: Simple collection copy on localhost"""
    console.print("\n[bold cyan]Example 1: Simple Collection Copy[/bold cyan]")
    console.print("Copying test_db.users to backup_db.users_backup")

    copier = MongoAdvancedCopier(
        "mongodb://localhost:27017",  # Source
        "mongodb://localhost:27017"   # Target (same server, different DB)
    )

    try:
        copier.connect()
        console.print("[green]✓ Connected to MongoDB[/green]")

        # This will fail gracefully if collections don't exist
        result = copier.copy_collection_with_indexes(
            source_db="test_db",
            source_coll="users",
            target_db="backup_db",
            target_coll="users_backup",
            drop_target=True,
            force=True
        )

        console.print(f"[green]✓ Copied {result['documents_copied']} documents[/green]")
        console.print(f"[green]✓ Created {result['indexes_created']} indexes[/green]")

    except Exception as e:
        console.print(f"[red]✗ Error: {e}[/red]")
        console.print("[yellow]Note: Make sure MongoDB is running on localhost:27017[/yellow]")
        console.print("[yellow]Create test data with: example_create_test_data()[/yellow]")

    finally:
        copier.close()


def example_2_copy_with_verification():
    """Example 2: Copy with verification"""
    console.print("\n[bold cyan]Example 2: Copy with Verification[/bold cyan]")

    copier = MongoAdvancedCopier(
        "mongodb://localhost:27017",
        "mongodb://localhost:27017"
    )

    try:
        copier.connect()

        # Copy
        copier.copy_collection_with_indexes(
            "test_db", "products",
            "backup_db", "products_verified",
            drop_target=True,
            force=True
        )

        # Verify
        result = copier.verify_copy(
            "test_db", "products",
            "backup_db", "products_verified"
        )

        if result['count_match'] and result['index_match']:
            console.print("[green]✓ Verification passed![/green]")
            console.print(f"  Documents: {result['source_count']} = {result['target_count']}")
            console.print(f"  Indexes: {result['source_indexes']} = {result['target_indexes']}")
        else:
            console.print("[red]✗ Verification failed![/red]")

    except Exception as e:
        console.print(f"[red]✗ Error: {e}[/red]")

    finally:
        copier.close()


def example_create_test_data():
    """Create test data for examples"""
    console.print("\n[bold cyan]Creating Test Data[/bold cyan]")

    from pymongo import MongoClient

    try:
        client = MongoClient("mongodb://localhost:27017")

        # Create test database and collections
        db = client["test_db"]

        # Create users collection
        if "users" in db.list_collection_names():
            db.users.drop()

        users = [
            {"_id": i, "username": f"user{i}", "email": f"user{i}@example.com", "age": 20 + i}
            for i in range(100)
        ]
        db.users.insert_many(users)
        db.users.create_index("username")
        db.users.create_index([("age", -1)])
        console.print(f"[green]✓ Created 'users' collection with {len(users)} documents[/green]")

        # Create products collection
        if "products" in db.list_collection_names():
            db.products.drop()

        products = [
            {"_id": i, "name": f"Product {i}", "price": 10.99 * i, "stock": 100 - i}
            for i in range(50)
        ]
        db.products.insert_many(products)
        db.products.create_index("name")
        db.products.create_index("price")
        console.print(f"[green]✓ Created 'products' collection with {len(products)} documents[/green]")

        # Create orders collection
        if "orders" in db.list_collection_names():
            db.orders.drop()

        orders = [
            {"_id": i, "user_id": i % 10, "product_id": i % 5, "quantity": (i % 3) + 1}
            for i in range(200)
        ]
        db.orders.insert_many(orders)
        db.orders.create_index([("user_id", 1), ("product_id", 1)])
        console.print(f"[green]✓ Created 'orders' collection with {len(orders)} documents[/green]")

        client.close()
        console.print("\n[green]✓ Test data created successfully![/green]")
        console.print("[dim]Database: test_db[/dim]")
        console.print("[dim]Collections: users, products, orders[/dim]")

        return True

    except Exception as e:
        console.print(f"[red]✗ Error creating test data: {e}[/red]")
        console.print("[yellow]Make sure MongoDB is running on localhost:27017[/yellow]")
        return False


def main():
    """Run examples"""
    console.print("[bold]mongo-wizard - Quick Start Examples[/bold]\n")

    # Check MongoDB connection
    from pymongo import MongoClient
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        client.close()
        console.print("[green]✓ MongoDB is running on localhost:27017[/green]")
    except Exception:
        console.print("[red]✗ MongoDB is not running on localhost:27017[/red]")
        console.print("[yellow]Please start MongoDB first:[/yellow]")
        console.print("[dim]  brew services start mongodb-community[/dim]")
        console.print("[dim]  or: mongod --dbpath /path/to/data[/dim]")
        return

    # Menu
    console.print("\n[bold]Available Examples:[/bold]")
    console.print("1. Create test data")
    console.print("2. Simple collection copy")
    console.print("3. Copy with verification")
    console.print("4. Run all examples")
    console.print("0. Exit")

    choice = input("\nSelect an example (0-4): ").strip()

    if choice == "1":
        example_create_test_data()
    elif choice == "2":
        example_1_simple_copy()
    elif choice == "3":
        example_2_copy_with_verification()
    elif choice == "4":
        if example_create_test_data():
            example_1_simple_copy()
            example_2_copy_with_verification()
    elif choice == "0":
        console.print("Goodbye!")
    else:
        console.print("[red]Invalid choice[/red]")


if __name__ == "__main__":
    main()