"""
Storage abstraction layer for backup/restore operations
Supports local filesystem, rsync, SSH, and FTP
"""

import os
import subprocess
import shlex
import ftplib
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from datetime import datetime
from dataclasses import dataclass, asdict
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn
from .formatting import format_size
from .constants import (
    SSH_CONNECT_TIMEOUT,
    SSH_KEEPALIVE_INTERVAL,
    SSH_KEEPALIVE_MAX_COUNT,
    SCP_TRANSFER_TIMEOUT,
    DEFAULT_FTP_PORT
)

console = Console()


@dataclass
class StorageConfig:
    """Base storage configuration"""
    name: str
    type: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        return cls(**data)


@dataclass
class LocalStorageConfig:
    """Local filesystem configuration"""
    name: str
    path: str
    type: str = "local"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SSHStorageConfig:
    """SSH/SCP storage configuration"""
    name: str
    host: str
    user: str
    path: str = "/"
    port: int = 22
    key_path: str | None = None
    type: str = "ssh"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FTPStorageConfig:
    """FTP storage configuration"""
    name: str
    host: str
    user: str
    password: str
    path: str = "/"
    port: int = DEFAULT_FTP_PORT
    type: str = "ftp"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class StorageBackend(ABC):
    """Abstract base class for storage backends"""

    @abstractmethod
    def list_files(self, path: str, pattern: str = "*.tar.gz") -> list[dict[str, Any]]:
        """List files at path matching pattern"""
        pass

    @abstractmethod
    def upload(self, local_path: str, remote_path: str) -> bool:
        """Upload file to storage"""
        pass

    @abstractmethod
    def download(self, remote_path: str, local_path: str) -> bool:
        """Download file from storage"""
        pass

    @abstractmethod
    def get_file_info(self, path: str) -> dict[str, Any]:
        """Get file size and metadata"""
        pass

    @abstractmethod
    def delete(self, path: str) -> bool:
        """Delete file from storage"""
        pass


class LocalStorage(StorageBackend):
    """Local filesystem storage"""

    def list_files(self, path: str, pattern: str = "*.tar.gz") -> list[dict[str, Any]]:
        """List local files"""
        from pathlib import Path
        import glob

        path_obj = Path(path)
        if not path_obj.exists():
            path_obj.mkdir(parents=True, exist_ok=True)
            return []

        files = []
        for file_path in path_obj.glob(pattern):
            if file_path.is_file():
                stat = file_path.stat()
                files.append({
                    'name': file_path.name,
                    'path': str(file_path),
                    'size': stat.st_size,
                    'modified': datetime.fromtimestamp(stat.st_mtime),
                    'size_human': format_size(stat.st_size)
                })

        return sorted(files, key=lambda x: x['modified'], reverse=True)

    def upload(self, local_path: str, remote_path: str) -> bool:
        """Copy file locally"""
        import shutil
        try:
            Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, remote_path)
            return True
        except Exception as e:
            console.print(f"[red]Local copy failed: {e}[/red]")
            return False

    def download(self, remote_path: str, local_path: str) -> bool:
        """Copy file locally"""
        import shutil
        try:
            shutil.copy2(remote_path, local_path)
            return True
        except Exception as e:
            console.print(f"[red]Local copy failed: {e}[/red]")
            return False

    def get_file_info(self, path: str) -> dict[str, Any]:
        """Get local file info"""
        path_obj = Path(path)
        if not path_obj.exists():
            return {}

        stat = path_obj.stat()
        return {
            'size': stat.st_size,
            'size_human': format_size(stat.st_size),
            'modified': datetime.fromtimestamp(stat.st_mtime)
        }

    def delete(self, path: str) -> bool:
        """Delete local file"""
        try:
            Path(path).unlink()
            return True
        except Exception as e:
            console.print(f"[red]Delete failed: {e}[/red]")
            return False


class SSHStorage(StorageBackend):
    """SSH/SCP storage backend"""

    def __init__(self, host: str, user: str, port: int = 22, key_path: str | None = None):
        self.host = host
        self.user = user
        self.port = port
        self.key_path = key_path

    def test_connection(self, test_path: str | None = None) -> tuple[bool, str]:
        """Test SSH connection and write permissions"""
        ssh_cmd = self._build_ssh_command()

        # First test basic connection
        test_cmd = ssh_cmd + ['echo', 'OK']
        try:
            result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0 or 'OK' not in result.stdout:
                error = result.stderr or "Connection failed"
                return False, error
        except subprocess.TimeoutExpired:
            return False, "Connection timeout"
        except Exception as e:
            return False, str(e)

        # Now test write permissions if path provided
        if test_path:
            import time
            test_file = f"{test_path}/.mw_test_{int(time.time())}"

            # Try to create and remove a test file
            touch_cmd = ssh_cmd + [f'touch {shlex.quote(test_file)} && rm {shlex.quote(test_file)} && echo "WRITE_OK"']
            try:
                result = subprocess.run(touch_cmd, capture_output=True, text=True, timeout=10)
                if result.returncode == 0 and 'WRITE_OK' in result.stdout:
                    return True, "SSH connection and write permissions OK"
                else:
                    error = result.stderr or "Write permission denied"
                    if "Permission denied" in error:
                        return False, f"Write permission denied in {test_path}"
                    return False, error
            except Exception as e:
                return False, f"Write test failed: {str(e)}"

        return True, "SSH connection successful"

    def list_files(self, path: str, pattern: str = "*.tar.gz") -> list[dict[str, Any]]:
        """List files via SSH"""
        ssh_cmd = self._build_ssh_command()
        remote_cmd = f'ls -la {shlex.quote(path)}/{pattern} 2>/dev/null | grep -v ^d'
        cmd = ssh_cmd + [remote_cmd]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return []

            files = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                parts = line.split()
                if len(parts) >= 9:
                    size = int(parts[4])
                    name = parts[-1].split('/')[-1]
                    files.append({
                        'name': name,
                        'path': f"{path}/{name}",
                        'size': size,
                        'size_human': format_size(size),
                        'modified': datetime.now()  # Could parse from ls output
                    })

            return sorted(files, key=lambda x: x['name'], reverse=True)
        except Exception as e:
            console.print(f"[red]SSH list failed: {e}[/red]")
            return []

    def upload(self, local_path: str, remote_path: str) -> bool:
        """Upload via SCP"""
        # Ensure remote_path is absolute
        if not remote_path.startswith('/'):
            remote_path = '/' + remote_path

        # First, create remote directory if needed
        remote_dir = os.path.dirname(remote_path)
        if remote_dir and remote_dir != '/':
            ssh_cmd = self._build_ssh_command()
            mkdir_cmd = ssh_cmd + [f'mkdir -p {shlex.quote(remote_dir)}']
            console.print(f"[dim]Creating remote directory: {remote_dir}[/dim]")
            result = subprocess.run(mkdir_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                console.print(f"[red]Failed to create remote directory: {result.stderr}[/red]")
                return False

        # Now upload the file
        scp_cmd = self._build_scp_command()
        scp_target = f"{self.user}@{self.host}:{remote_path}"
        cmd = scp_cmd + [local_path, scp_target]

        # Debug: show the actual command and paths
        console.print(f"[dim]Local file: {local_path}[/dim]")
        console.print(f"[dim]Remote path: {remote_path}[/dim]")
        console.print(f"[dim]SCP command: {' '.join(cmd)}[/dim]")

        with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                console=console
        ) as progress:
            task = progress.add_task(f"[cyan]Uploading to {self.host}...", total=None)

            try:
                # Get file size for progress
                file_size = os.path.getsize(local_path)
                console.print(f"[dim]Uploading {format_size(file_size)}...[/dim]")

                result = subprocess.run(cmd, capture_output=True, text=True, timeout=SCP_TRANSFER_TIMEOUT)
                progress.update(task, completed=100)

                if result.returncode != 0:
                    # Show detailed error
                    console.print(f"[red]SCP upload failed![/red]")
                    if result.stderr:
                        console.print(f"[red]Error: {result.stderr}[/red]")
                    if result.stdout:
                        console.print(f"[yellow]Output: {result.stdout}[/yellow]")
                    return False

                # Verify the file exists on remote
                verify_cmd = self._build_ssh_command() + [f'ls -la {shlex.quote(remote_path)} 2>/dev/null']
                verify_result = subprocess.run(verify_cmd, capture_output=True, text=True)

                if verify_result.returncode != 0 or not verify_result.stdout:
                    console.print(f"[red]❌ Upload verification failed - file not found on remote![/red]")
                    console.print(f"[red]Expected file at: {remote_path}[/red]")
                    return False

                # Parse size from ls output to verify
                remote_size = 0
                try:
                    parts = verify_result.stdout.strip().split()
                    if len(parts) >= 5:
                        remote_size = int(parts[4])
                except (ValueError, IndexError) as e:
                    console.print(f"[yellow]⚠ Could not parse remote file size: {e}[/yellow]")

                if remote_size != file_size:
                    console.print(f"[red]❌ File size mismatch! Local: {file_size}, Remote: {remote_size}[/red]")
                    return False

                console.print(f"[green]✓ Upload verified - {format_size(remote_size)} transferred[/green]")
                return True

            except subprocess.TimeoutExpired:
                console.print(f"[red]❌ Upload timeout - transfer took longer than {SCP_TRANSFER_TIMEOUT}s[/red]")
                console.print(f"[dim]Check your network connection and SSH server[/dim]")
                return False
            except Exception as e:
                console.print(f"[red]SCP upload failed: {e}[/red]")
                return False

    def download(self, remote_path: str, local_path: str) -> bool:
        """Download via SCP"""
        scp_cmd = self._build_scp_command()
        scp_source = f"{self.user}@{self.host}:{remote_path}"
        cmd = scp_cmd + [scp_source, local_path]

        with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                console=console
        ) as progress:
            task = progress.add_task(f"[cyan]Downloading from {self.host}...", total=None)

            try:
                result = subprocess.run(cmd, capture_output=True)
                progress.update(task, completed=100)
                return result.returncode == 0
            except Exception as e:
                console.print(f"[red]SCP download failed: {e}[/red]")
                return False

    def get_file_info(self, path: str) -> dict[str, Any]:
        """Get remote file info via SSH"""
        ssh_cmd = self._build_ssh_command()
        cmd = ssh_cmd + [f'stat -c %s {shlex.quote(path)} 2>/dev/null']

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                size = int(result.stdout.strip())
                return {
                    'size': size,
                    'size_human': format_size(size)
                }
        except (subprocess.SubprocessError, ValueError) as e:
            console.print(f"[dim]Could not get file info: {e}[/dim]")
        return {}

    def delete(self, path: str) -> bool:
        """Delete remote file via SSH"""
        ssh_cmd = self._build_ssh_command()
        cmd = ssh_cmd + [f'rm -f {shlex.quote(path)}']

        try:
            result = subprocess.run(cmd, capture_output=True)
            return result.returncode == 0
        except Exception as e:
            console.print(f"[red]SSH delete failed: {e}[/red]")
            return False

    def _build_ssh_base_command(self, use_scp: bool = False) -> list[str]:
        """
        Build SSH or SCP command with common options.

        Args:
            use_scp: If True, build scp command. Otherwise, build ssh command.

        Returns:
            Command list ready for subprocess execution
        """
        # Port flag differs between ssh and scp
        port_flag = '-P' if use_scp else '-p'

        cmd = [
            'scp' if use_scp else 'ssh',
            port_flag, str(self.port),
            '-o', f'ConnectTimeout={SSH_CONNECT_TIMEOUT}',
            '-o', f'ServerAliveInterval={SSH_KEEPALIVE_INTERVAL}',
            '-o', f'ServerAliveCountMax={SSH_KEEPALIVE_MAX_COUNT}',
            '-o', 'StrictHostKeyChecking=accept-new'
        ]

        if self.key_path:
            cmd.extend(['-i', self.key_path])

        # SSH needs target host, SCP adds it later
        if not use_scp:
            cmd.append(f"{self.user}@{self.host}")

        return cmd

    def _build_ssh_command(self) -> list[str]:
        """Build SSH command with options"""
        return self._build_ssh_base_command(use_scp=False)

    def _build_scp_command(self) -> list[str]:
        """Build SCP command with options"""
        return self._build_ssh_base_command(use_scp=True)


class FTPStorage(StorageBackend):
    """FTP storage backend"""

    def __init__(self, host: str, user: str, password: str, port: int = DEFAULT_FTP_PORT):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def _connect(self):
        """Create FTP connection"""
        ftp = ftplib.FTP()
        ftp.connect(self.host, self.port)
        ftp.login(self.user, self.password)
        return ftp

    def list_files(self, path: str, pattern: str = "*.tar.gz") -> list[dict[str, Any]]:
        """List FTP files"""
        try:
            ftp = self._connect()
            ftp.cwd(path)

            files = []
            file_list = []
            ftp.dir(file_list.append)

            for line in file_list:
                parts = line.split()
                if len(parts) >= 9 and not line.startswith('d'):
                    name = parts[-1]
                    if pattern.replace('*', '') in name:
                        size = int(parts[4])
                        files.append({
                            'name': name,
                            'path': f"{path}/{name}",
                            'size': size,
                            'size_human': format_size(size),
                            'modified': datetime.now()
                        })

            ftp.quit()
            return sorted(files, key=lambda x: x['name'], reverse=True)
        except Exception as e:
            console.print(f"[red]FTP list failed: {e}[/red]")
            return []

    def upload(self, local_path: str, remote_path: str) -> bool:
        """Upload via FTP"""
        try:
            ftp = self._connect()

            # Create directory if needed
            remote_dir = os.path.dirname(remote_path)
            if remote_dir:
                try:
                    ftp.mkd(remote_dir)
                except ftplib.error_perm:
                    # Directory already exists, ignore
                    pass
                ftp.cwd(remote_dir)

            with open(local_path, 'rb') as f:
                file_size = os.path.getsize(local_path)

                with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        DownloadColumn(),
                        console=console
                ) as progress:
                    task = progress.add_task(f"[cyan]Uploading to {self.host}...", total=file_size)

                    def callback(block):
                        progress.advance(task, len(block))

                    ftp.storbinary(f'STOR {os.path.basename(remote_path)}', f, callback=callback)

            ftp.quit()
            return True
        except Exception as e:
            console.print(f"[red]FTP upload failed: {e}[/red]")
            return False

    def download(self, remote_path: str, local_path: str) -> bool:
        """Download via FTP"""
        try:
            ftp = self._connect()

            # Get file size
            ftp.voidcmd('TYPE I')
            size = ftp.size(remote_path)

            with open(local_path, 'wb') as f:
                with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        DownloadColumn(),
                        console=console
                ) as progress:
                    task = progress.add_task(f"[cyan]Downloading from {self.host}...", total=size)

                    def callback(block):
                        f.write(block)
                        progress.advance(task, len(block))

                    ftp.retrbinary(f'RETR {remote_path}', callback)

            ftp.quit()
            return True
        except Exception as e:
            console.print(f"[red]FTP download failed: {e}[/red]")
            return False

    def get_file_info(self, path: str) -> dict[str, Any]:
        """Get FTP file info"""
        try:
            ftp = self._connect()
            ftp.voidcmd('TYPE I')
            size = ftp.size(path)
            ftp.quit()

            return {
                'size': size,
                'size_human': format_size(size)
            }
        except (ftplib.error_perm, Exception) as e:
            console.print(f"[dim]Could not get FTP file info: {e}[/dim]")
            return {}

    def delete(self, path: str) -> bool:
        """Delete FTP file"""
        try:
            ftp = self._connect()
            ftp.delete(path)
            ftp.quit()
            return True
        except Exception as e:
            console.print(f"[red]FTP delete failed: {e}[/red]")
            return False


class StorageFactory:
    """Factory for creating storage backends"""

    @staticmethod
    def create(storage_url_or_config: str | dict[str, Any]) -> StorageBackend:
        """
        Create storage backend from URL or config dict

        Examples:
        - /path/to/local/dir
        - ssh://user@host:port/path
        - ftp://user:pass@host:port/path
        - {"type": "ssh", "name": "mybackup", "host": "server.com", ...}
        """
        # Handle config dict
        if isinstance(storage_url_or_config, dict):
            config_type = storage_url_or_config.get('type', 'local')

            if config_type == 'ssh':
                return SSHStorage(
                    host=storage_url_or_config['host'],
                    user=storage_url_or_config['user'],
                    port=storage_url_or_config.get('port', 22),
                    key_path=storage_url_or_config.get('key_path')
                )
            elif config_type == 'ftp':
                return FTPStorage(
                    host=storage_url_or_config['host'],
                    user=storage_url_or_config['user'],
                    password=storage_url_or_config['password'],
                    port=storage_url_or_config.get('port', 21)
                )
            elif config_type == 'local':
                return LocalStorage()
            else:
                raise ValueError(f"Unknown storage type: {config_type}")

        # Handle URL string
        storage_url = storage_url_or_config
        if not storage_url:
            return LocalStorage()

        # Parse URL
        if '://' in storage_url:
            parsed = urlparse(storage_url)
            scheme = parsed.scheme.lower()

            if scheme == 'ssh':
                return SSHStorage(
                    host=parsed.hostname,
                    user=parsed.username or 'root',
                    port=parsed.port or 22
                )
            elif scheme == 'ftp':
                return FTPStorage(
                    host=parsed.hostname,
                    user=parsed.username,
                    password=parsed.password,
                    port=parsed.port or 21
                )
            elif scheme == 'rsync':
                # Rsync uses SSH backend
                return SSHStorage(
                    host=parsed.hostname,
                    user=parsed.username or 'root',
                    port=22
                )

        # Default to local storage
        return LocalStorage()

    @staticmethod
    def create_config(storage_url: str, name: str) -> StorageConfig:
        """Create a storage config from URL"""
        if not storage_url or not '://' in storage_url:
            return LocalStorageConfig(name=name, path=storage_url or os.getcwd())

        parsed = urlparse(storage_url)
        scheme = parsed.scheme.lower()

        if scheme == 'ssh' or scheme == 'rsync':
            return SSHStorageConfig(
                name=name,
                host=parsed.hostname,
                user=parsed.username or 'root',
                port=parsed.port or 22,
                path=parsed.path or '/'
            )
        elif scheme == 'ftp':
            return FTPStorageConfig(
                name=name,
                host=parsed.hostname,
                user=parsed.username,
                password=parsed.password,
                port=parsed.port or 21,
                path=parsed.path or '/'
            )
        else:
            return LocalStorageConfig(name=name, path=storage_url)
