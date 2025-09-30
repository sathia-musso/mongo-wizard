#!/usr/bin/env python
"""
mongo-wizard - Standalone script wrapper
This file allows running the wizard without installing the package
"""

import sys
from mongo_wizard.wizard import MongoWizard, check_system_requirements
from mongo_wizard.settings import SettingsManager
from mongo_wizard.core import MongoAdvancedCopier
from mongo_wizard.utils import check_mongodb_tools

# For backward compatibility, import main classes
__all__ = ['MongoWizard', 'SettingsManager', 'MongoAdvancedCopier']

def main():
    """Main entry point for standalone script"""
    # Check system requirements
    check_system_requirements()

    # Run wizard
    wizard = MongoWizard()
    wizard.run()

if __name__ == '__main__':
    main()