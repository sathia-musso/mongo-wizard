"""
Allow db_wizard to be run as a module:
    python -m db_wizard
"""

from .cli import main

if __name__ == '__main__':
    main()
