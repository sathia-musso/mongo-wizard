"""
SSH tunnel manager for db-wizard.
Opens SSH port-forwarding tunnels to reach databases behind SSH.
"""

import subprocess
import socket
import time
import atexit
from typing import Any
from urllib.parse import urlparse, urlunparse
from rich.console import Console

console = Console()

# Track active tunnels for cleanup
_active_tunnels: list[subprocess.Popen] = []


def _find_free_port() -> int:
    """Find a free local port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def _cleanup_tunnels():
    """Kill all active SSH tunnels on exit."""
    for proc in _active_tunnels:
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass
    _active_tunnels.clear()


# Register cleanup on interpreter exit
atexit.register(_cleanup_tunnels)


def open_tunnel(db_uri: str, ssh_config: str | dict[str, Any]) -> str:
    """
    Open an SSH tunnel and return a rewritten URI pointing to localhost.

    Args:
        db_uri: The database URI (e.g. mysql://user:pass@remotehost:3306/db)
        ssh_config: Either:
            - A string hostname (uses your ~/.ssh/config, e.g. "myserver")
            - A dict with {host, user, port?, key_path?}

    Returns:
        Rewritten URI with localhost:<free_port> replacing the original host:port.

    Example:
        uri = open_tunnel(
            "mysql://user:pass@localhost:3306/db",
            "myserver"  # SSH host from ~/.ssh/config
        )
        # Returns: "mysql://user:pass@127.0.0.1:54321/db"
    """
    parsed = urlparse(db_uri)

    # Figure out the remote target (what the DB thinks its host:port is)
    remote_host = parsed.hostname or 'localhost'
    remote_port = parsed.port or _default_port_for_scheme(parsed.scheme)

    # Find a free local port
    local_port = _find_free_port()

    # Build SSH command
    if isinstance(ssh_config, str):
        # Simple hostname - relies on ~/.ssh/config
        ssh_target = ssh_config
        ssh_cmd = [
            'ssh', '-f', '-N',
            '-L', f'{local_port}:{remote_host}:{remote_port}',
            ssh_target
        ]
    else:
        # Dict config with explicit params
        ssh_host = ssh_config['host']
        ssh_user = ssh_config.get('user', 'root')
        ssh_port = ssh_config.get('port', 22)
        ssh_cmd = [
            'ssh', '-f', '-N',
            '-p', str(ssh_port),
            '-L', f'{local_port}:{remote_host}:{remote_port}',
        ]
        if ssh_config.get('key_path'):
            ssh_cmd.extend(['-i', ssh_config['key_path']])
        ssh_cmd.append(f'{ssh_user}@{ssh_host}')

    # Open the tunnel
    console.print(f"[dim]Opening SSH tunnel: localhost:{local_port} -> {remote_host}:{remote_port} via {ssh_config if isinstance(ssh_config, str) else ssh_config['host']}[/dim]")

    try:
        proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # With -f, SSH forks to background after successful auth.
        # The parent process (our Popen) exits with code 0 on success,
        # or non-zero on auth/connection failure.
        # We wait for it to finish forking.
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise ConnectionError("SSH tunnel timed out during setup (15s)")

        if proc.returncode != 0:
            stderr = proc.stderr.read().decode('utf-8', errors='replace').strip()
            raise ConnectionError(f"SSH tunnel failed (exit {proc.returncode}): {stderr}")

        # SSH forked successfully - give the tunnel a moment to be ready
        time.sleep(0.5)

        # Verify the local port is actually listening
        for attempt in range(5):
            try:
                with socket.create_connection(('127.0.0.1', local_port), timeout=2):
                    break
            except (ConnectionRefusedError, OSError):
                if attempt < 4:
                    time.sleep(0.5)
                else:
                    raise ConnectionError(
                        f"SSH tunnel opened but port {local_port} is not responding. "
                        f"Check that the remote database is running on {remote_host}:{remote_port}"
                    )

        console.print(f"[green]✓ SSH tunnel open on port {local_port}[/green]")

    except FileNotFoundError:
        raise ConnectionError("ssh command not found. Install OpenSSH.")
    except Exception as e:
        raise ConnectionError(f"Failed to open SSH tunnel: {e}")

    # Rewrite URI to point to localhost:local_port
    rewritten = parsed._replace(
        netloc=f"{parsed.username or ''}:{parsed.password or ''}@127.0.0.1:{local_port}"
               if parsed.username else f"127.0.0.1:{local_port}"
    )

    return urlunparse(rewritten)


def close_tunnels():
    """Close all active SSH tunnels."""
    _cleanup_tunnels()


def _default_port_for_scheme(scheme: str) -> int:
    """Return default port for a database scheme."""
    if scheme.startswith('mongodb'):
        return 27017
    elif scheme == 'mysql':
        return 3306
    elif scheme in ('postgres', 'postgresql'):
        return 5432
    elif scheme == 'redis':
        return 6379
    return 0
