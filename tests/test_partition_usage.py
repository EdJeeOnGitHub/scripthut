"""Allocation cards must not sum overlapping scheduler partitions."""
from unittest.mock import AsyncMock

import pytest

import scripthut.main as main
from scripthut.backends.base import ClusterInfo, PartitionInfo
from scripthut.backends.slurm import SlurmBackend
from scripthut.models import ConnectionStatus
from scripthut.runtime import BackendState


def partition(name, allocated=60, idle=20, other=20):
    return PartitionInfo(name, 'up', allocated, idle, other,
                         allocated + idle + other, 1)


def render(info):
    backend = BackendState(name='example', backend_type='slurm',
        status=ConnectionStatus(connected=True, host='example', cluster_info=info))
    return main.templates.get_template('overview_cards.html').render(
        backends={'example': backend}, backend_usage={}, active_runs=[], recent_runs=[],
        activity=main.build_activity_grid([]), hourly=main.build_hourly_usage([]))


@pytest.mark.asyncio
@pytest.mark.parametrize('config,names', [
    ({'default_partition': 'private-x'}, ['private-x']),
    ({'partition_map': {'standard': 'public-a', 'long': 'public-b'}}, ['public-a', 'public-b']),
    ({'default_partition': 'public-a', 'partition_map': {'standard': 'public-a'}}, ['public-a']),
    ({'partition_map': {'standard': 'public-a,public-b'}}, ['public-a', 'public-b']),
    ({}, ['public-a', 'public-b', 'private-x']),
    ({'default_partition': 'missing'}, []),
])
async def test_configuration_scopes_only_overview(config, names):
    # public-a and public-b can refer to the same physical node. The private
    # partition is visible to sinfo but is not necessarily a submission target.
    rows = ''.join(f'{name}|up|60/20/20/100|1|8192|4:00:00|(null)\n'
                   for name in ['public-a', 'public-b', 'private-x'])
    ssh = AsyncMock()
    ssh.user = None

    async def command(cmd, timeout=None):
        return (rows if '--summarize' in cmd else '', '', 0)

    ssh.run_command.side_effect = command
    backend = SlurmBackend(ssh, **config)
    info = await backend.get_cluster_info()
    assert [p.name for p in info.overview_partitions] == names
    assert len(info.partitions) == 3  # Details still have all visible partitions.
    assert all('--partition' not in call.args[0] for call in ssh.run_command.call_args_list)


def test_overlapping_pools_render_separately_and_down_cpus_are_not_allocated():
    html = render(ClusterInfo([partition('public-a'), partition('public-b')], {}))
    assert html.count('60% allocated') == 2
    assert html.count('20 unavailable') == 2
    assert html.count('>20</span> CPUs idle') == 2
    assert '% busy' not in html
    assert '80% allocated' not in html
    assert '>40</span> CPUs' not in html


def test_other_visible_partitions_do_not_leak_into_configured_card():
    html = render(ClusterInfo([partition('public-a'), partition('private-x')], {},
                             overview_partition_names=['public-a']))
    assert 'public-a' in html
    assert 'private-x' not in html
    assert 'Configured partitions' in html


def test_missing_configured_partition_does_not_fall_back_to_cluster_totals():
    html = render(ClusterInfo([partition('private-x')], {},
                             overview_partition_names=['missing']))
    assert 'Partition availability not reported.' in html
    assert '% allocated' not in html


def test_many_partitions_keep_card_compact():
    html = render(ClusterInfo([partition(f'pool-{n}') for n in range(20)], {}))
    assert html.count('60% allocated') == 3
    assert '17 more partitions' in html
    assert 'pool-3' not in html


def test_zero_capacity_and_missing_cluster_data_render_without_error():
    assert '% allocated' not in render(ClusterInfo([partition('empty', 0, 0, 0)], {}))
    assert '% allocated' not in render(None)
