from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from scripthut.config_schema import SlurmBackendConfig, SSHConfig
from scripthut.runs.models import TaskDefinition
from scripthut.runtime import init_backend


@pytest.mark.parametrize("qos", [
    "", "two values", "buyin\n#SBATCH --nodes=99", "buyin\x00", "x\x7f",
])
def test_invalid_qos(qos):
    with pytest.raises(ValidationError, match="qos"):
        SlurmBackendConfig(name="q", ssh=SSHConfig(host="q", user="u"), qos=qos)


@pytest.mark.asyncio
@pytest.mark.parametrize("qos", [None, "buyin"])
async def test_runtime_emits_qos(monkeypatch, qos):
    monkeypatch.setattr("scripthut.runtime.SSHClient", lambda **kwargs: AsyncMock())
    cfg = SlurmBackendConfig(name="q", ssh=SSHConfig(host="q", user="u"),
                             account="kellogg", default_partition="kellogg", qos=qos)
    bs = await init_backend(cfg)
    script = bs.backend.generate_script(
        TaskDefinition(id="t", name="t", command="hostname"), "run", "/logs",
        account=cfg.account,
    )
    assert "#SBATCH --account=kellogg\n" in script
    assert "#SBATCH --partition=kellogg\n" in script
    assert ("#SBATCH --qos=buyin\n" in script) == (qos == "buyin")
    if qos is None:
        assert "--qos" not in script
