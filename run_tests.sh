#!/bin/bash

# mongo-wizard - Complete Test Suite
# Run this before releasing to make sure nothing is broken

set -e  # Exit on error

echo "======================================"
echo "mongo-wizard - Complete Test Suite"
echo "======================================"

# Check if venv exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate venv
source .venv/bin/activate

# Install dependencies
echo ""
echo "1. Installing dependencies..."
pip install -q -r requirements.txt
pip install -q pytest pytest-mock

# Run unit tests
echo ""
echo "2. Running unit tests..."
pytest tests/ --ignore=tests/test_integration.py -q

# Check imports
echo ""
echo "3. Testing imports..."
python -c "
from mongo_wizard import MongoAdvancedCopier, SettingsManager, MongoWizard
print('✓ All imports successful')
"

# Test CLI
echo ""
echo "4. Testing CLI..."
python cli.py --version
python cli.py --help > /dev/null && echo "✓ CLI help works"

# Check MongoDB
echo ""
echo "5. Checking MongoDB..."
python -c "
from pymongo import MongoClient
try:
    client = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=2000)
    client.admin.command('ping')
    client.close()
    print('✓ MongoDB is running on localhost:27017')
    mongodb_available = True
except Exception as e:
    print('⚠ MongoDB not available - skipping integration tests')
    mongodb_available = False
"

# Run integration tests if MongoDB is available
if python -c "from pymongo import MongoClient; client = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=1000); client.admin.command('ping')" 2>/dev/null; then
    echo ""
    echo "6. Running integration tests..."
    pytest tests/test_integration.py -q
else
    echo ""
    echo "6. Skipping integration tests (MongoDB not available)"
fi

# Test examples
echo ""
echo "7. Testing examples..."
python -c "
import sys
sys.path.insert(0, '.')
import examples.basic_usage
print('✓ Examples import correctly')
"

echo ""
echo "======================================"
echo "✅ ALL TESTS PASSED!"
echo "======================================"
echo ""
echo "Package is ready for use. You can now:"
echo "  - Run interactively: python cli.py"
echo "  - Install package: pip install -e ."
echo "  - Run examples: python examples/quick_start.py"