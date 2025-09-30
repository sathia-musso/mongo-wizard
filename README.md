# mongo-wizard

Advanced MongoDB copy and migration tool with a powerful CLI.

## Why Use mongo-wizard?

### The Problem
- **Manual mongodump/mongorestore** requires remembering complex commands and connection strings
- **No progress tracking** when copying large databases
- **Risk of data loss** without proper backup procedures
- **Repetitive tasks** require typing the same long commands repeatedly

### The Solution - mongo-wizard
- **Smart Tool Selection** - Automatically uses the fastest method (mongodump when available, Python fallback)
- **Interactive Wizard** - No need to remember connection strings or commands
- **Saved Configurations** - Store hosts and tasks for one-command execution
- **Production Ready** - Automated backups, verification, and error handling
- **Full Automation** - Perfect for cron jobs and CI/CD pipelines with `-y` flag
- **Real Progress Tracking** - See exactly what's happening during long operations

### Real-World Use Cases
```bash
# 1. Daily automated backups (cron job)
0 3 * * * mongo-wizard -t daily_backup -y

# 2. Sync staging from production
mongo-wizard -t sync_staging

# 3. Quick collection copy without remembering URIs
mongo-wizard  # Interactive mode guides you

# 4. Migrate between cloud providers
mongo-wizard -s mongodb://aws-server -t mongodb://gcp-server --source-db production
```

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Optional: Install MongoDB Tools
For 10-100x faster operations, install MongoDB database tools:
```bash
# macOS
brew install mongodb-database-tools

# Ubuntu/Debian
apt-get install mongodb-database-tools

# Other systems
# Download from: https://www.mongodb.com/try/download/database-tools
```

## Key Features

- 🚀 **Native MongoDB Tools**: Prefers mongodump/mongorestore for speed (10-100x faster)
- 💾 **Interactive Mode**: Guided menu for all operations
- 💾 **Saved Hosts**: Save and reuse connection configurations
- 📋 **Saved Tasks**: Create and execute repeatable tasks
- 🔄 **Full Automation**: `-y` flag for unattended cron jobs
- 📦 **Backup & Restore**: Complete backup/restore with compression
- 🗄️ **Storage Backends**: Support for Local, SSH/SCP, and FTP storage
- 💼 **Saved Storage Configs**: Reusable storage configurations
- 🛡️ **Automatic Backups**: Create backups before destructive operations
- 📊 **Integrity Verification**: Compare source and target after copy
- 🎯 **Multiple Selection**: Copy multiple collections with range syntax (1,3-5,7)
- 🌍 **Python Fallback**: Falls back to Python if MongoDB tools unavailable
- 🔢 **Smart Formatting**: Numbers with underscore separators (1_234_567)

## Usage Modes

### 1. Interactive Mode (Recommended)

```bash
python mongo_wizard.py
```

Guides you through:
- Select/create saved hosts
- Choose database and collections
- Copy options (drop, backup, verify)
- Save as reusable task

### 2. Saved Tasks (Automation)

List available tasks:
```bash
python mongo_wizard.py --list-tasks
```

Run task interactively:
```bash
python mongo_wizard.py -t daily_backup
```

Run task automated (for cron):
```bash
python mongo_wizard.py -t daily_backup -y
```

### 3. Direct Command Line

Copy single collection:
```bash
python mongo_wizard.py \
  -s mongodb://localhost/myapp/users \
  -t mongodb://remote/backup/users \
  --drop-target
```

Copy entire database:
```bash
python mongo_wizard.py \
  -s mongodb://localhost/ \
  -t mongodb://remote/ \
  --source-db myapp \
  --target-db myapp_backup \
  --drop-target \
  --force
```

## Main Options

| Option | Description |
|--------|-------------|
| `-s, --source` | Source MongoDB URI |
| `-t, --target` | Target MongoDB URI |
| `--source-db` | Source database |
| `--target-db` | Target database |
| `--source-collection` | Specific collection (optional) |
| `--drop-target` | Drop target before copying |
| `--dry-run` | Show operations without executing |
| `-f, --force` | Skip confirmations (deprecated, use -y) |
| `-y, --yes` | Full automation without prompts |
| `--force-python` | Force Python copy instead of mongodump |
| `--verify` | Verify integrity after copy |
| `--list-tasks` | Show saved tasks |
| `--task <name>` | Execute saved task |
| `--backup` | Backup database (mongodb://uri/database) |
| `--backup-to` | Backup destination (path or ssh://user@host/path) |
| `--restore` | Restore from backup file |
| `--restore-to` | Restore target MongoDB URI |

## Practical Examples

### Nightly Backup with Cron

1. Create task interactively:
```bash
python mongo_wizard.py
# Select hosts, database, collections
# Save as "nightly_backup"
```

2. Add to crontab:
```bash
# Backup every night at 3:00 AM
0 3 * * * cd ./scripts && source .venv/bin/activate && python mongo_wizard.py -t nightly_backup -y >> ~/logs/mongo_backup.log 2>&1
```

### Copy with Filter and Verification

```bash
python mongo_wizard.py \
  -s mongodb://user:pass@prod.server.com/AppDB \
  -t mongodb://localhost/AppDB_Local \
  --source-collection users \
  --drop-target \
  --verify \
  -y
```

### Complete Database Migration

```bash
python mongo_wizard.py \
  -s mongodb://old-server/ \
  -t mongodb://new-server/ \
  --source-db Production \
  --target-db Production \
  --drop-target \
  --verify
```

## Configuration File

Settings are saved in `~/.mongo_wizard_settings.json`:

```json
{
  "hosts": {
    "local": "mongodb://localhost:27017",
    "prod": "mongodb://user:pass@prod.server.com:27017"
  },
  "tasks": {
    "daily_backup": {
      "source_uri": "mongodb://localhost:27017",
      "target_uri": "mongodb://backup-server:27017",
      "source_db": "production",
      "target_db": "production_backup",
      "drop_target": true
    }
  },
  "storages": {
    "backup_server": {
      "type": "ssh",
      "host": "backup.server.com",
      "user": "backup",
      "port": 22,
      "path": "/backups/mongodb"
    },
    "local_backup": {
      "type": "local",
      "path": "/var/backups/mongodb"
    }
  }
}
```

## Backup and Restore

### Interactive Backup
```bash
python mongo_wizard.py
# Select option 7 (Backup Database)
# Choose database, collections, and storage destination
```

### Command-Line Backup
```bash
# Local backup
python mongo_wizard.py \
  --backup mongodb://localhost/myapp \
  --backup-to /var/backups/mongodb

# SSH backup
python mongo_wizard.py \
  --backup mongodb://localhost/production \
  --backup-to ssh://backup@server.com:/backups

# FTP backup
python mongo_wizard.py \
  --backup mongodb://localhost/myapp \
  --backup-to ftp://user:pass@ftp.server.com:/backups
```

### Restore from Backup
```bash
# Restore to same database
python mongo_wizard.py \
  --restore /backups/2024_01_15_10_30-production.tar.gz \
  --restore-to mongodb://localhost

# Restore to different database
python mongo_wizard.py \
  --restore ssh://backup@server:/backups/prod.tar.gz \
  --restore-to mongodb://localhost \
  --drop-target
```

### Storage Management
Saved storage configurations can be managed through the interactive menu:
1. Launch `python mongo_wizard.py`
2. Select "Manage Storage Configs"
3. Add/test/remove storage configurations

Supported storage types:
- **Local**: Local filesystem paths
- **SSH/SCP**: Remote servers via SSH (with optional key authentication)
- **FTP**: FTP servers with authentication

## Advanced Features

### Multiple Collection Selection

During interactive mode, you can select:
- Single: `3`
- Multiple: `1,3,5`
- Range: `1-5`
- Combined: `1,3-5,8,10-12`
- All: `ALL`

### Automatic Backups

When using `--drop-target`, the wizard can create automatic backups:
- Format: `collection_backup_YYYYMMDD_HHMMSS`
- Saved in the same target database
- Option available in interactive mode

### Integrity Verification

With `--verify`:
- Compare document counts
- Verify copied indexes
- Random document sampling
- Checksum for small collections (<10k docs)

## System Requirements

- Python 3.10+ (uses modern type hints with | syntax)
- MongoDB 5.0+ recommended (tested with 5.0, 6.0, 7.0)
- MongoDB tools (mongodump, mongorestore) for optimal performance (10-100x faster)
- Connection to source and target MongoDB servers

The wizard automatically checks requirements at startup and warns if optional components are missing.

## Testing

### Comprehensive Test Suite
The package includes a complete test suite with **16 integration tests** that test all features with real MongoDB operations.

```bash
# Install test dependencies
pip install -r requirements-dev.txt

# Run all tests (requires MongoDB on localhost:27017)
pytest tests/ -v

# Run only unit tests (no MongoDB required)
pytest tests/ --ignore=tests/test_integration.py --ignore=tests/test_full_integration.py -v

# Run full integration tests (requires MongoDB)
pytest tests/test_full_integration.py -v

# Run with coverage report
pytest --cov=mongo_wizard --cov-report=html tests/
```

### Test Coverage
- **Unit tests**: Mock-based tests for core logic
- **Integration tests**: 16 comprehensive tests with real MongoDB:
  - Single/multiple collection copy
  - Database copy with all collections
  - Backup creation and restore
  - Integrity verification
  - Edge cases (empty collections, special characters)
  - Performance comparison (mongodump vs Python)
  - Settings management with multiple formats
  - CLI command testing
  - Large document handling

### CI/CD Pipeline
The project uses GitHub Actions for automated testing:
- **Python versions**: 3.9, 3.10, 3.11, 3.12, 3.13
- **MongoDB versions**: 5.0, 6.0, 7.0
- **Security scanning**: Bandit + Safety
- **Package build verification**
- **Automatic on every push/PR**

### Running Tests Locally
```bash
# Quick test of core functionality
pytest tests/test_full_integration.py::TestFullIntegration::test_copy_single_collection -v

# Test with specific MongoDB version using Docker
docker run -d -p 27017:27017 mongo:7.0
pytest tests/test_full_integration.py -v
```

## Performance Notes

### Default Behavior (Recommended)
- **Automatically uses mongodump/mongorestore** when available (10-100x faster)
- Falls back to Python copy only if tools are missing or fail
- Preserves all indexes, options, and metadata perfectly

### When to use `--force-python`
- Need document-by-document control or transformation
- Want detailed progress tracking
- Debugging or special filtering requirements
- MongoDB tools not available and can't be installed

### Speed Comparison
| Scenario | mongodump | Python Copy | Best Choice |
|----------|-----------|-------------|-------------|
| Small collections (<100K) on localhost | ~1-2s | ~0.5-1s | Either |
| Large collections (>1M) on localhost | ~15s | ~5 min | mongodump |
| Network transfers (any size) | Fast | Slower | mongodump |
| Very large docs (>1MB each) | Fast | Very slow | mongodump |
| With transformation needed | N/A | Available | Python |

## Notes

- MongoDB credentials are saved locally in `~/.mongo_wizard_settings.json`
- **mongodump/mongorestore are preferred by default** for performance
- Use `--force-python` only when you need fine-grained control
- The `-y` mode is perfect for CI/CD automation and cron jobs
- Install MongoDB tools with: `brew install mongodb-database-tools`

## Troubleshooting

Connection errors:
1. Verify MongoDB URI with `mongo` CLI
2. Check firewall/network access
3. Use `--verify-connection` for quick test

For large collections:
- **mongodump/mongorestore are used automatically** (10-100x faster)
- Use `--force-python` only if you need document-level control
- For Python mode, increase timeout: `MONGO_TIMEOUT=300`