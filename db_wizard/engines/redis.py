from __future__ import annotations
"""
Redis engine implementation.
Uses redis-cli exclusively.
"""

import shutil
import subprocess
from typing import Any, Self
from urllib.parse import urlparse, unquote

from rich.console import Console

from ..engine import DatabaseEngine
from ..constants import DEFAULT_CONNECTION_TIMEOUT

console = Console()

DEFAULT_REDIS_PORT = 6379

def parse_redis_uri(uri: str) -> dict[str, Any]:
    parsed = urlparse(uri)
    return {
        'host': parsed.hostname or 'localhost',
        'port': parsed.port or DEFAULT_REDIS_PORT,
        'password': unquote(parsed.password or ''),
        'database': parsed.path.lstrip('/') if parsed.path else '0',
    }

def _build_redis_args(params: dict[str, Any], include_db: bool = True) -> list[str]:
    args = [
        '-h', params['host'],
        '-p', str(params['port']),
    ]
    if params['password']:
        args.extend(['-a', params['password']])
    if include_db and params['database']:
        args.extend(['-n', params['database']])
    return args

class RedisEngine(DatabaseEngine):
    """
    Redis engine using redis-cli.
    Note: Redis uses logical databases (0-15) and doesn't have tables.
    We treat 'tables' as key patterns (e.g. '*') or omit them.
    """
    def __init__(self, uri: str):
        super().__init__(uri)
        self.params = parse_redis_uri(uri)

    def connect(self, timeout: int = DEFAULT_CONNECTION_TIMEOUT) -> Self:
        cmd = ['redis-cli'] + _build_redis_args(self.params, include_db=False) + ['PING']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout // 1000 + 5)
        if result.returncode != 0 or 'PONG' not in result.stdout:
            raise ConnectionError(f"Redis connection failed: {result.stderr.strip() or result.stdout.strip()}")
        return self

    def close(self) -> None:
        pass

    def test_connection(self, timeout: int = DEFAULT_CONNECTION_TIMEOUT) -> tuple[bool, str]:
        try:
            self.connect(timeout=timeout)
            return True, "OK (Redis)"
        except Exception as e:
            return False, str(e)

    def list_databases(self) -> list[dict[str, Any]]:
        cmd = ['redis-cli'] + _build_redis_args(self.params, include_db=False) + ['INFO', 'keyspace']
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return []

        databases = []
        for line in result.stdout.strip().split('\n'):
            if line.startswith('db'):
                parts = line.split(':')
                db_name = parts[0][2:]  # e.g., db0 -> 0
                keys_info = parts[1].split(',')
                keys = 0
                for info in keys_info:
                    if info.startswith('keys='):
                        keys = int(info.split('=')[1])
                databases.append({
                    'name': db_name,
                    'tables_count': keys, # Using keys as tables_count for display
                    'size_mb': 0.0,
                })
        return databases

    def list_tables(self, database: str) -> list[dict[str, Any]]:
        # Redis doesn't have tables. We can just return a dummy table or scan key types.
        # Let's just return one "table" representing all keys.
        params = self.params.copy()
        params['database'] = database
        cmd = ['redis-cli'] + _build_redis_args(params) + ['DBSIZE']
        result = subprocess.run(cmd, capture_output=True, text=True)
        keys = 0
        if result.returncode == 0:
            try:
                keys = int(result.stdout.strip())
            except ValueError:
                pass

        return [{
            'name': '*',
            'rows': keys,
            'indexes': 0,
        }]

    def count_rows(self, database: str, table: str) -> int:
        params = self.params.copy()
        params['database'] = database
        if table and table != '*':
            # If a specific pattern is passed
            return 0  # not efficient to count exact pattern without SCAN
        cmd = ['redis-cli'] + _build_redis_args(params) + ['DBSIZE']
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            try:
                return int(result.stdout.strip())
            except ValueError:
                return 0
        return 0

    def sample_rows(self, database: str, table: str, limit: int = 5) -> tuple[list[str], list[list[str]]]:
        return ["Key", "Type"], [["(Redis sampling not fully implemented via CLI)", ""]]

    def check_tools(self) -> dict[str, bool]:
        return {
            'redis-cli': shutil.which('redis-cli') is not None,
        }

    def dump(self, database: str, tables: list[str] | None, output_path: str) -> bool:
        console.print("[yellow]Redis dump via wizard currently not supported via CLI[/yellow]")
        return False

    def restore(self, input_path: str, target_database: str, drop_target: bool = False) -> bool:
        console.print("[yellow]Redis restore via wizard currently not supported via CLI[/yellow]")
        return False

    def copy(self, source_engine: 'DatabaseEngine', source_db: str, source_table: str | None, target_db: str, target_table: str | None, drop_target: bool = False, force: bool = False, **kwargs) -> dict[str, Any]:
        console.print("[red]Redis cross-copy currently not implemented fully.[/red]")
        return {'documents_copied': 0, 'method': 'unsupported'}

    @property
    def table_term(self) -> str:
        return "pattern"

    @property
    def table_term_plural(self) -> str:
        return "patterns"

    @property
    def scheme(self) -> str:
        return "redis"
