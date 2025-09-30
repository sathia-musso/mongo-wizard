"""
Allow mongo_wizard to be run as a module:
    python -m mongo_wizard
"""

from .cli import main

if __name__ == '__main__':
    main()