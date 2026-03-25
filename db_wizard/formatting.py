"""
Centralized formatting utilities for mongo-wizard
Provides consistent number and size formatting across the application
"""


def format_number(num: int) -> str:
    """
    Format number with underscore as thousand separator

    Args:
        num: Number to format

    Returns:
        Formatted string with underscores

    Examples:
        1234567 -> "1_234_567"
        1000 -> "1_000"
        999 -> "999"
    """
    if num < 1000:
        return str(num)

    # Use Python's built-in formatting then replace
    return f"{num:,}".replace(',', '_')


def format_size(size_bytes: int) -> str:
    """
    Format byte size with appropriate unit and underscore separators

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted size string with unit

    Examples:
        1234567890 -> "1.1 GB"
        14754300000 -> "13_740.5 MB" or "13.7 GB"
        1024 -> "1.0 KB"
        512 -> "512 B"
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            if size_bytes >= 1000:
                # Format with underscore separator for thousands
                formatted = f"{size_bytes:,.1f}".replace(',', '_')
            elif size_bytes >= 100:
                formatted = f"{size_bytes:.0f}"
            else:
                formatted = f"{size_bytes:.1f}"
            return f"{formatted} {unit}"
        size_bytes /= 1024.0

    # Petabytes and beyond
    formatted = f"{size_bytes:,.1f}".replace(',', '_')
    return f"{formatted} PB"


def format_docs(count: int) -> str:
    """
    Format document count with K/M suffix and underscore separators

    Args:
        count: Document count

    Returns:
        Formatted string with appropriate suffix

    Examples:
        1234567 -> "1.2M"
        12345 -> "12.3K"
        999 -> "999"
    """
    from .constants import FORMAT_MILLIONS_THRESHOLD, FORMAT_THOUSANDS_THRESHOLD

    if count >= FORMAT_MILLIONS_THRESHOLD:
        value = count / FORMAT_MILLIONS_THRESHOLD
        if value >= 1000:
            formatted = f"{value:,.1f}".replace(',', '_')
        else:
            formatted = f"{value:.1f}"
        return f"{formatted}M"
    elif count >= FORMAT_THOUSANDS_THRESHOLD:
        value = count / FORMAT_THOUSANDS_THRESHOLD
        if value >= 1000:
            formatted = f"{value:,.1f}".replace(',', '_')
        else:
            formatted = f"{value:.1f}"
        return f"{formatted}K"
    else:
        return str(count)


# Alias for backward compatibility
format_document_count = format_docs