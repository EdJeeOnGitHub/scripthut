"""Exact revision transport and real Git checkout regression tests."""
import asyncio
import json
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from scripthut.cli import RemoteClient, build_parser
from scripthut.config_schema import GitSourceConfig, PathSourceConfig
from scripthut.sources.git import SourceWorkflow
from tests.test_branch_override import (
    _branch_source_manager,
    _client,
    _git,
    _make_run,
    _make_state,
    _run_manager,
)
from tests.test_branch_override import (
    manager as manager,
)
from tests.test_branch_override import (
    origin as origin,
)


def sha(repo, ref='HEAD'):
    return subprocess.check_output(['git', 'rev-parse', ref], cwd=repo, text=True).strip()


class LocalExecution:
    async def run_command(self, command, timeout=120):
        process = await asyncio.create_subprocess_exec(
            'bash', '-c', command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await asyncio.wait_for(process.communicate(), timeout)
        return out.decode().strip(), err.decode().strip(), process.returncode


async def test_fetch_exact_commit_and_concurrent_branch_fetches(manager, origin):
    main, feature = sha(origin, 'main'), sha(origin, 'feature')
    results = await asyncio.gather(manager.fetch_branch('src', 'main'),
                                   manager.fetch_branch('src', 'feature'))
    assert results == [main, feature]
    assert await manager.fetch_commit('src', main) == main
    assert sha(manager.get_source_path('src')) == main
    with pytest.raises(ValueError, match='Failed to fetch commit'):
        await manager.fetch_commit('src', '0' * 40)
    blob = subprocess.check_output(['git', 'rev-parse', 'HEAD:.hut/workflows/train.json'], cwd=origin, text=True).strip()
    with pytest.raises(ValueError, match='not the requested commit'):
        await manager.fetch_commit('src', blob)


async def test_branch_moves_but_checkout_and_workflow_keep_captured_commit(manager, origin, tmp_path):
    commit = await manager.fetch_branch('src', 'main')
    workflows = await manager.discover_workflows_at('src', commit)
    _git('reset', '--hard', 'feature', cwd=origin)
    source = GitSourceConfig(name='src', url=str(origin), clone_dir=str(tmp_path / 'backend with spaces'))
    rm = _run_manager(source, tmp_path)
    workspace, actual = await rm._clone_pinned_source(LocalExecution(), source, commit)
    assert actual == commit == sha(workspace)
    assert json.loads((Path(workspace) / '.hut/workflows/train.json').read_text())['title'] == 'main-train'
    assert json.loads(workflows[0].tasks_json)['title'] == 'main-train'
    assert subprocess.run(['git', 'symbolic-ref', '-q', 'HEAD'], cwd=workspace).returncode != 0
    (Path(workspace) / '.hut/workflows/train.json').write_text('dirty prior run')
    other, _ = await rm._clone_pinned_source(LocalExecution(), source, commit)
    assert other != workspace
    assert json.loads((Path(other) / '.hut/workflows/train.json').read_text())['title'] == 'main-train'


async def test_postclone_cannot_change_tracked_code_or_head(origin, tmp_path):
    for hook in ('echo changed > .hut/workflows/train.json', 'git checkout --orphan another',
                 'git fetch origin feature && git checkout --detach FETCH_HEAD'):
        source = GitSourceConfig(name='src', url=str(origin), clone_dir=str(tmp_path / 'backend'), postclone=hook)
        rm = _run_manager(source, tmp_path)
        with pytest.raises(ValueError, match='differs from the requested commit'):
            await rm._clone_pinned_source(LocalExecution(), source, sha(origin))


async def test_manager_records_full_sha_and_never_submits_a_missing_commit(origin, tmp_path):
    source = GitSourceConfig(name='src', url=str(origin), clone_dir=str(tmp_path / 'backend'))
    rm = _run_manager(source, tmp_path)
    built = AsyncMock(return_value=_make_run())
    with patch.object(rm, 'get_ssh_client', return_value=LocalExecution()), \
         patch.object(rm, '_build_run', built), \
         patch.object(rm, '_load_source_project_config', AsyncMock(return_value=None)) as overlay:
        await rm.create_run_from_source('src', 'train.json', '{"tasks": []}', 'cluster', commit_hash=sha(origin))
        assert built.await_args.kwargs['commit_hash'] == sha(origin)
        assert built.await_args.kwargs['git_branch'] is None
        assert overlay.await_args.kwargs['commit_hash'] == sha(origin)
        built.reset_mock()
        with pytest.raises(ValueError, match='Pinned source checkout failed'):
            await rm.create_run_from_source('src', 'train.json', '{"tasks": []}', 'cluster', commit_hash='0' * 40)
        built.assert_not_awaited()


def test_api_exact_commit_selects_workflow_and_passes_sha():
    commit = 'a' * 40
    wf = SourceWorkflow(name='src/train', source_name='src', filename='train.json', tasks_json='{"tasks": []}')
    sm = _branch_source_manager([wf])
    sm.fetch_commit = AsyncMock(return_value=commit)
    state = _make_state([GitSourceConfig(name='src', url='https://example.test/repo')], sm)
    response = _client(state).post('/api/v1/sources/src/run-commit', params={
        'workflow': 'train.json', 'backend': 'cluster', 'commit': commit})
    assert response.status_code == 200
    sm.fetch_commit.assert_awaited_once_with('src', commit)
    sm.fetch_branch.assert_not_awaited()
    sm.discover_workflows_at.assert_awaited_once_with('src', commit)
    assert state.run_manager.create_run_from_source.await_args.kwargs['commit_hash'] == commit
    state.run_manager.create_run_from_source.reset_mock()
    assert _client(state).post('/api/v1/sources/src/run-commit', params={
        'workflow': 'train.json', 'backend': 'cluster', 'commit': commit, 'branch': 'main'}).status_code == 422
    state.run_manager.create_run_from_source.assert_not_awaited()


@pytest.mark.parametrize('query', [
    {'commit': 'abc'}, {'commit': 'main'}, {'commit': 'a' * 39 + ';'},
    {'commit': 'a' * 40, 'branch': 'main'},
])
def test_invalid_revisions_never_create_run(query):
    state = _make_state([GitSourceConfig(name='src', url='https://example.test/repo')])
    response = _client(state).post('/api/v1/sources/src/run', params={
        'workflow': 'train.json', 'backend': 'cluster', **query})
    assert response.status_code == 422
    state.run_manager.create_run_from_source.assert_not_awaited()


def test_path_sources_reject_sha_and_failed_fetch_cannot_use_cached_workflow():
    source = PathSourceConfig(name='src', path='/tmp/source', backend='cluster')
    state = _make_state([source])
    assert _client(state).post('/api/v1/sources/src/run-commit', params={
        'workflow': 'train.json', 'commit': 'a' * 40}).status_code == 422
    state.run_manager.create_run_from_source.assert_not_awaited()
    source = GitSourceConfig(name='src', url='https://example.test/repo')
    sm = _branch_source_manager([])
    sm.fetch_branch.side_effect = ValueError('fetch unavailable')
    state = _make_state([source], sm, {'src': [SourceWorkflow(
        name='src/train', source_name='src', filename='train.json', tasks_json='{"tasks": []}')]})
    assert _client(state).post('/api/v1/sources/src/run', params={
        'workflow': 'train.json', 'backend': 'cluster'}).status_code == 422
    state.run_manager.create_run_from_source.assert_not_awaited()


async def test_cli_uses_distinct_route_so_old_server_cannot_ignore_sha():
    requests = []
    def old_server(request):
        requests.append(request)
        return httpx.Response(404, json={'detail': 'Not Found'})
    async with RemoteClient('https://example.test') as client:
        await client._client.aclose()
        client._client = httpx.AsyncClient(base_url='https://example.test/api/v1',
                                          transport=httpx.MockTransport(old_server))
        with pytest.raises(RuntimeError, match='HTTP 404'):
            await client.run_source_workflow('src', 'train.json', backend='cluster', commit='a' * 40)
    assert len(requests) == 1
    assert requests[0].url.path == '/api/v1/sources/src/run-commit'
    assert requests[0].url.params['commit'] == 'a' * 40
    parser = build_parser()
    args = parser.parse_args(['workflow', 'run', 'train.json', '--source', 'src', '--commit', 'a' * 40])
    assert args.commit == 'a' * 40
    with pytest.raises(SystemExit):
        parser.parse_args(['workflow', 'run', 'train.json', '--source', 'src', '--commit', 'a' * 40, '--branch', 'main'])
