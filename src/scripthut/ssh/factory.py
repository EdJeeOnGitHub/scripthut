"""Transport selection shared by CLI and controller initialization."""

from scripthut.config_schema import SSHConfig
from scripthut.ssh.client import SSHClient
from scripthut.ssh.transport import ExecutionClient


def create_ssh_client(config: SSHConfig) -> ExecutionClient:
    if config.transport == "openssh":
        from scripthut.ssh.openssh import OpenSSHClient

        return OpenSSHClient(config)
    return SSHClient(
        host=config.host, user=config.user, key_path=config.key_path_resolved,
        port=config.port, cert_path=config.cert_path_resolved,
        known_hosts=config.known_hosts_resolved,
    )
