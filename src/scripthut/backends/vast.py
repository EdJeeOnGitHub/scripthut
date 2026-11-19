"""Run each task in a dedicated Vast marketplace rental."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shlex
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncssh
import httpx

from scripthut.backends.base import JobBackend, JobStats, SubmitResult
from scripthut.backends.batch import gpu_count_from_gres, parse_memory_mib
from scripthut.backends.utils import generate_script_body, parse_duration_hms
from scripthut.backends.vast_api import VastAPI, VastAPIError
from scripthut.config_schema import VastBackendConfig
from scripthut.models import HPCJob, JobState
from scripthut.runs.models import TaskDefinition

logger = logging.getLogger(__name__)
REMOTE = "/tmp/scripthut-job"
FAILURES = {
    "FAILED": "Vast task or output collection failed",
    "TIMEOUT": "Vast task exceeded its deadline",
    "CANCELLED": "Vast task was cancelled",
    "NODE_FAIL": "Vast rental disappeared",
}


class VastBackend(JobBackend):
    def __init__(self, config: VastBackendConfig, state_root: Path) -> None:
        self.config = config
        self.root = state_root / config.rental_namespace
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def api(self) -> VastAPI:
        key = os.environ.get(self.config.api_key_env)
        if self.config.api_key_file is not None:
            key = self.config.api_key_file.expanduser().read_text().strip()
        if not key:
            raise RuntimeError(f"Missing Vast credential {self.config.api_key_env}")
        return VastAPI(key)

    async def is_available(self) -> bool:
        await self.api.list_instances()
        return True

    @property
    def failure_states(self) -> dict[str, str]:
        return FAILURES

    @property
    def terminal_states(self) -> frozenset[str]:
        return frozenset({"COMPLETED", *FAILURES})

    def _directory(self, job_id: str) -> Path:
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            raise ValueError("Invalid Vast job ID")
        return self.root / job_id

    def _save(self, job_id: str, record: dict[str, Any]) -> None:
        directory = self._directory(job_id)
        directory.mkdir(mode=0o700, exist_ok=True)
        temporary = directory / "state.tmp"
        with temporary.open("w") as stream:
            json.dump(record, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(directory / "state.json")

    def _records(self) -> dict[str, dict[str, Any]]:
        return {path.parent.name: json.loads(path.read_text())
                for path in self.root.glob("*/state.json")}

    async def submit_job(self, script: str) -> str:
        raise RuntimeError("Vast requires a task definition")

    async def submit_task(
        self, task: TaskDefinition, script: str, env_vars: dict[str, str] | None = None,
    ) -> SubmitResult:
        staged_inputs = self._resolve_inputs(task.inputs)
        run_id = (env_vars or {}).get("SCRIPTHUT_RUN_ID")
        if not run_id:
            raise RuntimeError("Vast requires a ScriptHut run ID")
        job_id = hashlib.sha256(f"{run_id}\0{task.id}".encode()).hexdigest()[:32]
        profile_name = task.partition or self.config.default_profile
        if profile_name == "normal" and profile_name not in self.config.profiles:
            profile_name = self.config.default_profile
        profile = self.config.profiles.get(profile_name)
        image = task.image or self.config.default_image
        try:
            seconds = int(parse_duration_hms(task.time_limit))
        except (ValueError, OverflowError) as exc:
            raise RuntimeError("Invalid Vast task time limit") from exc
        if profile is None or not image or seconds <= 0:
            raise RuntimeError(
                "Vast requires a configured partition, image, and positive time limit"
            )
        if task.gres and not re.fullmatch(r"gpu(?::(?:[^:,]+:)?[1-9][0-9]*)?", task.gres):
            raise RuntimeError("Unsupported Vast GPU resource specification")
        gpu_names = profile.gpu_names
        if task.gres and task.gres.count(":") == 2:
            model = task.gres.split(":")[1].casefold()
            gpu_names = [name for name in gpu_names if name.casefold() == model]
            if not gpu_names:
                raise RuntimeError("Requested GPU model is outside the Vast offer profile")
        async with self.lock:
            records = self._records()
            if job_id in records:
                return SubmitResult(job_id, "Vast rental recorded")
            if sum(not r.get("state") for r in records.values()) >= self.config.max_instances:
                raise OSError("Vast rental concurrency limit reached")
            label = f"sh-{self.config.rental_namespace}-{job_id}"
            duration = (seconds + self.config.startup_timeout_seconds
                        + self.config.collection_timeout_seconds)
            filters = {
                "type": "on-demand", "verified": {"eq": True}, "rentable": {"eq": True},
                "rented": {"eq": False}, "limit": 20, "order": [["dph_total", "asc"]],
                "gpu_name": {"in": gpu_names},
                "num_gpus": {"eq": max(1, gpu_count_from_gres(task.gres))},
                "cpu_cores_effective": {"gte": task.cpus},
                "cpu_ram": {"gte": parse_memory_mib(task.memory)},
                "disk_space": {"gte": profile.disk_gb}, "allocated_storage": profile.disk_gb,
                "direct_port_count": {"gte": 1},
                "reliability": {"gte": profile.min_reliability},
                "duration": {"gte": duration}, "dph_total": {"lte": profile.max_hourly_usd},
            }
            record = {
                "label": label, "task": task.id, "partition": profile_name,
                "submitted": time.time(), "seconds": seconds, "outputs": task.outputs,
                "inputs": staged_inputs,
                "instance": None,
            }
            directory = self._directory(job_id)
            directory.mkdir(mode=0o700, exist_ok=True)
            (directory / "task.sh").write_text(script)
            for _ in range(2):
                try:
                    offers = await self.api.search_offers(filters)
                except httpx.HTTPError as exc:
                    raise OSError("Vast offer search temporarily unavailable") from exc
                if not offers:
                    break
                for offer in offers:
                    if not 0 < float(offer["dph_total"]) <= profile.max_hourly_usd:
                        continue
                    self._save(job_id, record)
                    try:
                        record["instance"] = await self.api.create_instance(
                            int(offer["id"]), {"image": image, "label": label,
                                               "disk": profile.disk_gb, "runtype": "ssh_direct"},
                        )
                    except VastAPIError as exc:
                        if exc.status_code in (404, 410):
                            (directory / "state.json").unlink()
                            continue
                        if 400 <= exc.status_code < 500:
                            (directory / "state.json").unlink()
                            raise
                        break
                    except (httpx.HTTPError, RuntimeError, ValueError, KeyError):
                        break
                    self._save(job_id, record)
                    return SubmitResult(job_id, f"Vast instance {record['instance']}")
                if (directory / "state.json").exists():
                    return SubmitResult(job_id, "Awaiting Vast rental confirmation")
            raise OSError("No Vast offer satisfies the task requirements")

    def generate_script(
        self, task: TaskDefinition, run_id: str, log_dir: str,
        account: str | None = None, login_shell: bool = False,
        env_vars: dict[str, str] | None = None, extra_init: str = "",
        interactive_wait: bool = False,
    ) -> str:
        if interactive_wait:
            raise ValueError("Interactive wait is unavailable on Vast")
        env = env_vars or {}
        work = task.working_dir
        setup = [extra_init]
        for key, value in env.items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                raise ValueError(f"Invalid task environment name {key!r}")
            setup.append(f"export {key}={shlex.quote(value)}")
        if repo := env.get("SCRIPTHUT_GIT_REPO"):
            sha = env.get("SCRIPTHUT_GIT_SHA", "")
            if not re.fullmatch(r"[0-9a-fA-F]{40,64}", sha):
                raise ValueError("Vast Git tasks require an exact Git revision")
            checkout = f"{REMOTE}/source"
            setup.extend([f"git clone -- {shlex.quote(repo)} {checkout}",
                          f"git -C {checkout} checkout --detach {sha}"])
            if work in ("", "~"):
                work = checkout
            elif not work.startswith("/"):
                work = f"{checkout}/{work}"
        for path in task.outputs:
            if Path(path).is_absolute() or ".." in Path(path).parts or not path.strip():
                raise ValueError("Vast output paths must be relative to the task working directory")
        setup.append(f"export SCRIPTHUT_INPUT_DIR={REMOTE}/inputs")
        working_dir = '"$HOME"' if work in ("", "~") else shlex.quote(work)
        command = f"pwd -P > {REMOTE}/workdir\nset +e\n{task.command}"
        body = generate_script_body(
            task.name, task.id, command, working_dir, extra_init="\n".join(setup),
            output_dir=f"{REMOTE}/outputs", run_summary_path=f"{REMOTE}/outputs/run-summary.md",
        )
        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"
        return f"{shebang}\nset -eo pipefail\n{body}"

    def _resolve_inputs(self, patterns: list[str]) -> list[str]:
        if not patterns or self.config.input_root is None:
            return []
        root = self.config.input_root.resolve()
        paths: set[str] = set()
        for pattern in patterns:
            relative = Path(pattern)
            if relative.is_absolute() or ".." in relative.parts or not relative.parts:
                raise RuntimeError("Vast input patterns must be relative to input_root")
            matched = False
            for path in root.glob(pattern):
                if path.is_dir() and not path.is_symlink():
                    continue
                if path.resolve() != path or not path.is_file():
                    raise RuntimeError("Vast staged inputs must be regular files without symlinks")
                paths.add(path.relative_to(root).as_posix())
                matched = True
            if not matched:
                raise RuntimeError(f"No files matched Vast input {pattern!r}")
        return sorted(paths)

    async def _stage_inputs(self, sftp: Any, record: dict[str, Any]) -> None:
        paths = record.get("inputs", [])
        root = self.config.input_root
        if paths and root is None:
            raise RuntimeError("Vast input_root changed after submission")
        await sftp.makedirs(f"{REMOTE}/inputs", exist_ok=True)
        if root is None:
            return
        for relative in paths:
            source = root.resolve() / relative
            if source.resolve() != source or not source.is_file():
                raise RuntimeError("A staged Vast input is missing or changed type")
            target = f"{REMOTE}/inputs/{relative}"
            await sftp.makedirs(str(Path(target).parent), exist_ok=True)
            await sftp.put(source, target)

    async def _known_hosts(
        self, job_id: str, record: dict[str, Any], host: str, port: int,
    ) -> Any:
        if not self.config.ssh_trust_on_first_use:
            return self.config.ssh_known_hosts_file
        pinned = record.get("ssh_host_key")
        if not pinned:
            async with asyncio.timeout(10):
                key = await asyncssh.get_server_host_key(host, port=port, config=None)
            if key is None:
                raise RuntimeError("Vast SSH endpoint did not provide a host key")
            pinned = key.export_public_key().decode().strip()
            record["ssh_host_key"] = pinned
            self._save(job_id, record)
        return ([asyncssh.import_public_key(pinned)], [], [])

    async def _observe(
        self, job_id: str, record: dict[str, Any], instance: dict[str, Any],
    ) -> None:
        directory = self._directory(job_id)
        ports = (instance.get("ports") or {}).get("22/tcp")
        if ports:
            host = instance.get("public_ipaddr")
            port = int(ports[0]["HostPort"])
        else:
            host = instance.get("ssh_host")
            port = int(instance.get("ssh_port") or 0)
        if not isinstance(host, str) or not host or not 1 <= port <= 65535:
            return
        known_hosts = await self._known_hosts(job_id, record, host, port)
        connection = asyncssh.connect(
            host, port=port, username="root", config=None,
            client_keys=[self.config.ssh_identity_file], known_hosts=known_hosts,
            agent_path=None, connect_timeout=10,
        )
        async with connection as conn, conn.start_sftp_client() as sftp:
            if not record.get("started"):
                await conn.run(f"mkdir -p {REMOTE}", check=True, timeout=15)
                if not await sftp.exists(f"{REMOTE}/started"):
                    await self._stage_inputs(sftp, record)
                    await sftp.put(directory / "task.sh", f"{REMOTE}/task.sh")
                    seconds = record["seconds"]
                    wrapper = (
                        f"cd {REMOTE}; "
                        "mkdir started || exit 0; "
                        f"timeout --kill-after=15 {seconds} ./task.sh > output.log 2>&1; "
                        "echo $? > code.tmp; mv code.tmp code"
                    )
                    await conn.run(f"chmod 700 {REMOTE}/task.sh; "
                                   f"nohup bash -c {shlex.quote(wrapper)} "
                                   "</dev/null >/dev/null 2>&1 &", check=True, timeout=15)
                record["started"] = time.time()
                self._save(job_id, record)
            if not await sftp.exists(f"{REMOTE}/code"):
                return
            if not record.get("collecting"):
                record["collecting"] = time.time()
                self._save(job_id, record)
            async with sftp.open(f"{REMOTE}/code") as stream:
                code = int(await stream.read(16))
            remaining = self.config.collection_timeout_seconds - (
                time.time() - record["collecting"]
            )
            async with asyncio.timeout(max(0, remaining)):
                await self._collect(job_id, record, sftp, code)
            state = "COMPLETED" if code == 0 else "TIMEOUT" if code == 124 else "FAILED"
            record.update(finishing=state, code=code)
            self._save(job_id, record)

    async def _download(self, sftp: Any, remote: str, local: Path) -> None:
        attrs = await sftp.lstat(remote)
        if attrs.type == asyncssh.FILEXFER_TYPE_REGULAR:
            async with sftp.open(remote, "rb") as source:
                with local.open("xb") as target:
                    while chunk := await source.read(1024 * 1024):
                        target.write(chunk)
                    target.flush()
                    os.fsync(target.fileno())
            return
        if attrs.type != asyncssh.FILEXFER_TYPE_DIRECTORY:
            raise ValueError("Vast output is neither a regular file nor a directory")
        local.mkdir()
        async for child in sftp.scandir(remote):
            name = child.filename
            if name in (".", ".."):
                continue
            if not name or "/" in name or "\0" in name:
                raise ValueError("Invalid filename from Vast output directory")
            await self._download(sftp, f"{remote}/{name}", local / name)

    async def _collect(
        self, job_id: str, record: dict[str, Any], sftp: Any, code: int,
    ) -> None:
        directory = self._directory(job_id)
        with tempfile.TemporaryDirectory(dir=directory) as temporary:
            staging = Path(temporary)
            await self._download(sftp, f"{REMOTE}/output.log", staging / "output.log")
            if code == 0 and not (directory / "outputs").exists():
                outputs = staging / "outputs"
                if await sftp.exists(f"{REMOTE}/outputs"):
                    await self._download(sftp, f"{REMOTE}/outputs", outputs)
                outputs.mkdir(exist_ok=True)
                if record["outputs"]:
                    async with sftp.open(f"{REMOTE}/workdir") as stream:
                        working_dir = (await stream.read(4096)).strip()
                    if not working_dir.startswith("/"):
                        raise ValueError("Missing Vast task working directory")
                    for index, pattern in enumerate(record["outputs"]):
                        target = outputs / f"declared-{index}"
                        target.mkdir()
                        matches = await sftp.glob(f"{working_dir}/{pattern}")
                        if not matches:
                            raise ValueError(f"Vast output pattern did not match: {pattern}")
                        for match in matches:
                            name = Path(match).name
                            if not name or name in (".", "..") or "\0" in name:
                                raise ValueError("Invalid filename from Vast output glob")
                            await self._download(sftp, match, target / name)
                outputs.replace(directory / "outputs")
            log = staging / "output.log"
            if log.is_symlink() or not log.is_file():
                raise ValueError("Vast did not return a regular log file")
            log.replace(directory / "output.log")

    async def _finish(self, job_id: str, record: dict[str, Any]) -> None:
        if instance_id := record.get("instance"):
            await self.api.destroy_instance(instance_id)
        record.update(state=record["finishing"], finished=time.time())
        self._save(job_id, record)
        (self._directory(job_id) / "task.sh").unlink(missing_ok=True)

    async def get_jobs(self, user: str | None = None) -> list[HPCJob]:
        async with self.lock:
            instances = await self.api.list_instances()
            by_label = {instance.get("label"): instance for instance in instances}
            active = []
            for job_id, record in self._records().items():
                if record.get("state"):
                    # A lost create response may surface after the local deadline.
                    if not record.get("instance") and (late := by_label.get(record["label"])):
                        await self.api.destroy_instance(str(late["id"]))
                    continue
                instance = by_label.get(record["label"])
                if instance and not record.get("instance"):
                    record["instance"] = str(instance["id"])
                    self._save(job_id, record)
                try:
                    now = time.time()
                    if not record.get("finishing"):
                        if instance is None and record.get("instance"):
                            record.update(finishing="NODE_FAIL", code=-1)
                        elif record.get("collecting"):
                            if now - record["collecting"] > self.config.collection_timeout_seconds:
                                record.update(finishing="FAILED", code=-1)
                        elif record.get("started"):
                            if now - record["started"] > record["seconds"] + 60:
                                record.update(finishing="TIMEOUT", code=124)
                        elif now - record["submitted"] > self.config.startup_timeout_seconds:
                            record.update(finishing="TIMEOUT", code=124)
                        running = instance and instance.get("actual_status") == "running"
                        if not record.get("finishing") and running:
                            deadline = (
                                record["submitted"] + self.config.startup_timeout_seconds
                                + record["seconds"] + self.config.collection_timeout_seconds
                            )
                            async with asyncio.timeout(max(0, deadline - time.time())):
                                await self._observe(job_id, record, instance)
                    if record.get("finishing"):
                        self._save(job_id, record)
                        await self._finish(job_id, record)
                        continue
                except (OSError, asyncssh.Error, RuntimeError, httpx.HTTPError,
                        ValueError, KeyError, TypeError) as exc:
                    logger.warning("Vast job %s will retry after %s", job_id, type(exc).__name__)
                active.append(HPCJob(
                    job_id=job_id, name=record["task"], user="vast",
                    state=JobState.RUNNING if record.get("started") else JobState.PENDING,
                    partition=record["partition"], time_used="0:00",
                    nodes=record.get("instance") or "-",
                    cpus=0, memory="-", submit_time=self._datetime(record["submitted"]),
                    start_time=self._datetime(record.get("started")),
                ))
            return active

    async def cancel_job(self, job_id: str) -> None:
        async with self.lock:
            record = self._records().get(job_id)
            if not record or record.get("state"):
                return
            record.update(finishing="CANCELLED", code=-1)
            self._save(job_id, record)
            if not record.get("instance"):
                instances = await self.api.list_instances()
                for instance in instances:
                    if instance.get("label") == record["label"]:
                        record["instance"] = str(instance["id"])
                        self._save(job_id, record)
                        break
            await self._finish(job_id, record)

    @staticmethod
    def _datetime(value: float | None) -> datetime | None:
        return datetime.fromtimestamp(value, UTC) if value is not None else None

    async def get_job_stats(
        self, job_ids: list[str], user: str | None = None,
    ) -> dict[str, JobStats]:
        records = self._records()
        return {
            job_id: JobStats(0, "", "", start_time=self._datetime(record.get("started")),
                             end_time=self._datetime(record["finished"]),
                             state=record["state"], exit_code=record["code"])
            for job_id in job_ids if (record := records.get(job_id)) and record.get("state")
        }

    async def get_cluster_info(self, user: str | None = None) -> None:
        return None

    async def fetch_log(
        self, job_id: str, log_path: str, log_type: str = "output", tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        path = self._directory(job_id) / "output.log"
        if log_type != "output" or not path.is_file():
            return None, "Vast combined log is not archived yet"
        content = path.read_text(errors="replace")
        if tail_lines:
            content = "\n".join(content.splitlines()[-tail_lines:])
        return content, None
