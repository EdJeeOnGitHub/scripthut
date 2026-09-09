"""SSH client integration tests against a disposable loopback server."""

from pathlib import Path

import asyncssh
import pytest

from scripthut.ssh.client import SSHClient


@pytest.mark.parametrize("matching_host_key", [True, False], ids=["trusted", "untrusted"])
async def test_known_hosts_path(tmp_path: Path, matching_host_key: bool) -> None:
    """A configured Path must verify the server key, including rejecting a mismatch."""
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    key_path = tmp_path / "client_key"
    client_key.write_private_key(str(key_path))
    authorized_keys = tmp_path / "authorized_keys"
    authorized_keys.write_bytes(client_key.export_public_key())

    async with asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[host_key],
        authorized_client_keys=str(authorized_keys),
    ) as server:
        port = server.get_port()
        trusted_key = (
            host_key if matching_host_key else asyncssh.generate_private_key("ssh-ed25519")
        )
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(
            f"[127.0.0.1]:{port} {trusted_key.export_public_key().decode('ascii')}",
            encoding="ascii",
        )
        client = SSHClient(
            host="127.0.0.1", user="test", key_path=key_path,
            port=port, known_hosts=known_hosts,
        )
        try:
            if matching_host_key:
                await client.connect()
                assert client.is_connected
            else:
                with pytest.raises(asyncssh.HostKeyNotVerifiable):
                    await client.connect()
                assert not client.is_connected
        finally:
            await client.disconnect()
        assert not client.is_connected
