#!/usr/bin/env python
"""
mongo-wizard - Usage Examples

This file shows how to use mongo_wizard as a Python module
for programmatic database operations.
"""

from mongo_wizard import MongoAdvancedCopier, SettingsManager

def example_direct_copy():
    """Example of direct database copy"""

    source_uri = "mongodb://localhost:27017"
    target_uri = "mongodb://backup-server:27017"

    copier = MongoAdvancedCopier(source_uri, target_uri)
    copier.connect()

    try:
        # Copy single collection with indexes
        result = copier.copy_collection_with_indexes(
            source_db="production",
            source_coll="users",
            target_db="backup",
            target_coll="users_backup",
            drop_target=True,
            force=True  # Skip confirmations
        )

        print(f"Copied {result['documents_copied']:,} documents")
        print(f"Created {result['indexes_created']} indexes")

    finally:
        copier.close()


def example_copy_with_verification():
    """Example with verification after copy"""

    source_uri = "mongodb://localhost:27017"
    target_uri = "mongodb://localhost:27017"

    copier = MongoAdvancedCopier(source_uri, target_uri)
    copier.connect()

    try:
        # Copy collection
        copier.copy_collection_with_indexes(
            "myapp", "products",
            "myapp_backup", "products",
            drop_target=True,
            force=True
        )

        # Verify the copy
        verification = copier.verify_copy(
            "myapp", "products",
            "myapp_backup", "products"
        )

        if verification['count_match'] and verification['index_match']:
            print("✅ Copy verified successfully!")
        else:
            print("⚠️ Verification failed:")
            print(f"  Document count match: {verification['count_match']}")
            print(f"  Index match: {verification['index_match']}")

    finally:
        copier.close()


def example_backup_and_copy():
    """Example with backup before destructive copy"""

    source_uri = "mongodb://prod-server:27017"
    target_uri = "mongodb://staging-server:27017"

    copier = MongoAdvancedCopier(source_uri, target_uri)
    copier.connect()

    try:
        # Create backup first
        backup_name = copier.backup_before_copy("staging_db", "important_collection")
        print(f"Created backup: {backup_name}")

        # Now do the copy
        result = copier.copy_collection_with_indexes(
            "production_db", "important_collection",
            "staging_db", "important_collection",
            drop_target=True,
            force=True
        )

        print(f"Copied {result['documents_copied']:,} documents")

    finally:
        copier.close()


def example_copy_entire_database():
    """Example copying entire database"""

    copier = MongoAdvancedCopier(
        "mongodb://source:27017",
        "mongodb://target:27017"
    )
    copier.connect()

    try:
        results = copier.copy_entire_database(
            source_db="myapp",
            target_db="myapp_copy",
            exclude_collections=["system.", "temp_"],  # Skip system and temp collections
            drop_target=True,
            create_backup=True,
            force=True
        )

        total_docs = sum(r['documents_copied'] for r in results.values())
        print(f"Copied {len(results)} collections")
        print(f"Total documents: {total_docs:,}")

        for collection, result in results.items():
            print(f"  - {collection}: {result['documents_copied']:,} docs")

    finally:
        copier.close()


def example_multiple_collections():
    """Example copying multiple specific collections"""

    copier = MongoAdvancedCopier(
        "mongodb://localhost:27017",
        "mongodb://backup:27017"
    )
    copier.connect()

    try:
        collections_to_copy = ["users", "products", "orders", "reviews"]

        results = copier.copy_multiple_collections(
            source_db="ecommerce",
            target_db="ecommerce_backup",
            collections=collections_to_copy,
            drop_target=True,
            create_backup=False,
            force=True
        )

        for coll_name, result in results.items():
            print(f"{coll_name}: {result['documents_copied']:,} documents")

    finally:
        copier.close()


def example_using_settings():
    """Example using saved settings"""

    settings = SettingsManager()

    # Add a new host
    settings.add_host("production", "mongodb://prod.example.com:27017")
    settings.add_host("staging", "mongodb://staging.example.com:27017")

    # Create a task
    task_config = {
        "source_uri": settings.get_host("production"),
        "target_uri": settings.get_host("staging"),
        "source_db": "myapp",
        "target_db": "myapp_staging",
        "drop_target": True,
        "source_collection": None  # Copy all collections
    }

    settings.add_task("sync_to_staging", task_config)

    # List all saved tasks
    for task_name, config in settings.list_tasks().items():
        print(f"Task: {task_name}")
        print(f"  Source: {config['source_db']}")
        print(f"  Target: {config['target_db']}")


def example_with_mongodump():
    """Example using mongodump/mongorestore for large collections"""

    copier = MongoAdvancedCopier(
        "mongodb://localhost:27017",
        "mongodb://backup:27017"
    )
    copier.connect()

    try:
        # Try mongodump first (faster for large collections)
        success = copier.copy_with_mongodump(
            source_db="bigdata",
            source_coll="huge_collection",
            target_db="bigdata_backup",
            target_coll="huge_collection",
            drop_target=True
        )

        if success:
            print("✅ Copied using mongodump/mongorestore (fast)")
        else:
            print("⚠️ Falling back to Python copy...")
            result = copier.copy_collection_with_indexes(
                "bigdata", "huge_collection",
                "bigdata_backup", "huge_collection",
                drop_target=True,
                batch_size=5000,  # Larger batch for performance
                force=True
            )
            print(f"Copied {result['documents_copied']:,} documents")

    finally:
        copier.close()


if __name__ == "__main__":
    print("mongo-wizard - Usage Examples\n")
    print("Available examples:")
    print("1. example_direct_copy() - Simple collection copy")
    print("2. example_copy_with_verification() - Copy with integrity check")
    print("3. example_backup_and_copy() - Backup before destructive copy")
    print("4. example_copy_entire_database() - Copy full database")
    print("5. example_multiple_collections() - Copy specific collections")
    print("6. example_using_settings() - Work with saved configurations")
    print("7. example_with_mongodump() - Use native tools for large data")
    print("\nUncomment the example you want to run in the code.")