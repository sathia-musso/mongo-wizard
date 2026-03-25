"""
Shared utilities for wizard flows.
Single source of truth for GoHome, _ask, and _test_connection.
"""

from rich.prompt import Prompt


class GoHome(Exception):
    """Raised when user types 'x' to return to main menu."""
    pass


def _ask(prompt_text: str, **kwargs) -> str:
    """Prompt.ask wrapper that raises GoHome on 'x' input."""
    result = Prompt.ask(prompt_text, **kwargs)
    if result.strip().lower() == 'x':
        raise GoHome()
    return result


def _test_connection(uri: str, timeout: int = 5000) -> tuple[bool, str]:
    """Test connection using the appropriate engine."""
    try:
        from ..engine import EngineFactory
        engine = EngineFactory.create(uri)
        return engine.test_connection(timeout=timeout)
    except ValueError as e:
        return False, str(e)
