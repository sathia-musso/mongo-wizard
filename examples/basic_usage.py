#!/usr/bin/env python
"""
db-wizard - Usage Examples

This file shows how to use db_wizard as a Python module
for programmatic database operations.
"""

from db_wizard import DatabaseEngine, EngineFactory
from db_wizard.engines.mongo import MongoEngine
from db_wizard.settings import SettingsManager


def example_direct_copy():
    """Example of direct database copy"""

    source_uri = "mongodb://localhost:27017"
    target_uri = "mongodb://backup-server:27017"

    source_engine = MongoEngine(source_uri)
    target_engine = MongoEngine(target_uri)
    source_engine.connect()
    target_engine.connect()

    try:
        result = target_engine.copy(
            source_engine=source_engine,
            source_db="production",
            source_table="users",
            target_db="backup",
            target_table="users_backup",
            drop_target=True,
            force=True
        )

        print(f"Copied {result['documents_copied']:,} documents")
        print(f"Created {result['indexes_created']} indexes")

    finally:
        source_engine.close()
        target_engine.close()


def example_copy_with_verification():
    """Example with verification after copy"""

    uri = "mongodb://localhost:27017"

    source_engine = MongoEngine(uri)
    target_engine = MongoEngine(uri)
    source_engine.connect()
    target_engine.connect()

    try:
        target_engine.copy(
            source_engine=source_engine,
            source_db="myapp", source_table="products",
            target_db="myapp_backup", target_table="products",
            drop_target=True,
            force=True
        )

        verification = source_engine.verify_copy(
            "myapp", "products",
            "myapp_backup", "products",
            target_engine=target_engine
        )

        if verification['count_match'] and verification['index_match']:
            print("Copy verified successfully!")
        else:
            print("Verification failed:")
            print(f"  Document count match: {verification['count_match']}")
            print(f"  Index match: {verification['index_match']}")

    finally:
        source_engine.close()
        target_engine.close()


def example_backup_and_copy():
    """Example with backup before destructive copy"""

    source_engine = MongoEngine("mongodb://prod-server:27017")
    target_engine = MongoEngine("mongodb://staging-server:27017")
    source_engine.connect()
    target_engine.connect()

    try:
        backup_name = target_engine.backup_before_copy("staging_db", "important_collection")
        print(f"Created backup: {backup_name}")

        result = target_engine.copy(
            source_engine=source_engine,
            source_db="production_db", source_table="important_collection",
            target_db="staging_db", target_table="important_collection",
            drop_target=True,
            force=True
        )

        print(f"Copied {result['documents_copied']:,} documents")

    finally:
        source_engine.close()
        target_engine.close()


def example_copy_entire_database():
    """Example copying entire database"""

    source_engine = MongoEngine("mongodb://source:27017")
    target_engine = MongoEngine("mongodb://target:27017")
    source_engine.connect()
    target_engine.connect()

    try:
        result = target_engine.copy(
            source_engine=source_engine,
            source_db="myapp", source_table=None,
            target_db="myapp_copy", target_table=None,
            drop_target=True,
            force=True
        )

        print(f"Copy completed: {result}")

    finally:
        source_engine.close()
        target_engine.close()


def example_multiple_collections():
    """Example copying multiple specific collections"""

    source_engine = MongoEngine("mongodb://localhost:27017")
    target_engine = MongoEngine("mongodb://backup:27017")
    source_engine.connect()
    target_engine.connect()

    try:
        collections_to_copy = ["users", "products", "orders", "reviews"]

        for coll_name in collections_to_copy:
            result = target_engine.copy(
                source_engine=source_engine,
                source_db="ecommerce", source_table=coll_name,
                target_db="ecommerce_backup", target_table=coll_name,
                drop_target=True,
                force=True
            )
            print(f"{coll_name}: {result['documents_copied']:,} documents")

    finally:
        source_engine.close()
        target_engine.close()


def example_using_settings():
    """Example using saved settings"""

    settings = SettingsManager()

    settings.add_host("production", "mongodb://prod.example.com:27017")
    settings.add_host("staging", "mongodb://staging.example.com:27017")

    task_config = {
        "source_uri": settings.get_host("production"),
        "target_uri": settings.get_host("staging"),
        "source_db": "myapp",
        "target_db": "myapp_staging",
        "drop_target": True,
        "source_collection": None
    }

    settings.add_task("sync_to_staging", task_config)

    for task_name, config in settings.list_tasks().items():
        print(f"Task: {task_name}")
        print(f"  Source: {config['source_db']}")
        print(f"  Target: {config['target_db']}")


def example_engine_factory():
    """Example using the EngineFactory for auto-detection"""

    engine = EngineFactory.create("mongodb://localhost:27017")
    print(f"Created {type(engine).__name__} for {engine.scheme}:// URIs")

    success, msg = engine.test_connection()
    print(f"Connection test: {msg}")


if __name__ == "__main__":
    print("db-wizard - Usage Examples\n")
    print("Available examples:")
    print("1. example_direct_copy() - Simple collection copy")
    print("2. example_copy_with_verification() - Copy with integrity check")
    print("3. example_backup_and_copy() - Backup before destructive copy")
    print("4. example_copy_entire_database() - Copy full database")
    print("5. example_multiple_collections() - Copy specific collections")
    print("6. example_using_settings() - Work with saved configurations")
    print("7. example_engine_factory() - Auto-detect database engine")
    print("\nUncomment the example you want to run in the code.")
