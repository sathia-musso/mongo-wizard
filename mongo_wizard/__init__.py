"""
mongo-wizard - Advanced MongoDB copy and migration tool

A comprehensive MongoDB management tool with features for copying,
backing up, and migrating databases and collections.
"""

__version__ = "1.0.0"
__author__ = "Sathia Musso"

from .core import MongoAdvancedCopier
from .wizard import MongoWizard
from .settings import SettingsManager

__all__ = [
    "MongoAdvancedCopier",
    "MongoWizard",
    "SettingsManager",
]