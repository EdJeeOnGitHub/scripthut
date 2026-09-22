from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from scripthut import artifact_views


@pytest.mark.asyncio
@pytest.mark.parametrize('prefix', ['', '/artifact-candidate'])
async def test_catalogue_file_links_escape_metadata_and_survive_outage(monkeypatch, prefix):
    version = 'sha256:' + 'a' * 64
    entry = {'project': 'pilot', 'name': '<script>unsafe</script>', 'kind': 'report', 'version': version}

    async def metadata(path):
        if path.startswith('artifact-catalog/'):
            raise httpx.HTTPStatusError('old gateway', request=httpx.Request('GET', 'http://test'),
                                        response=httpx.Response(404))
        if path == 'artifacts':
            return {'artifacts': [entry]}
        return {'id': version, 'entries': [entry], 'manifest': {'files': [
            {'path': 'summary é.pdf', 'size': 42}]}, 'runs': []}

    monkeypatch.setattr(artifact_views, 'metadata', metadata)
    app = FastAPI(root_path=prefix)
    templates = Jinja2Templates(directory=str(Path(__file__).parents[1] / 'templates'))
    app.include_router(artifact_views.make_artifact_views(templates))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.get(prefix + '/artifacts')
        assert response.status_code == 200
        assert '&lt;script&gt;unsafe&lt;/script&gt;' in response.text
        assert '<script>unsafe</script>' not in response.text
        response = await client.get(prefix + '/artifacts/' + version)
        assert response.status_code == 200
        assert '/files/summary%20%C3%A9.pdf' in response.text
        assert 'http://test' + prefix + '/api/v1/artifacts/' in response.text

        async def unavailable(path):
            raise ConnectionError('offline')

        monkeypatch.setattr(artifact_views, 'metadata', unavailable)
        response = await client.get(prefix + '/artifacts/' + version)
        assert response.status_code == 503
        assert 'run history is still available' in response.text


@pytest.mark.parametrize('prefix', ['', '/artifact-candidate'])
def test_run_navigation_and_live_updates_stay_on_mounted_service(prefix):
    from datetime import datetime, timezone
    from html.parser import HTMLParser
    from starlette.requests import Request
    from scripthut.runs.models import Run

    class LocalRoutes(HTMLParser):
        def handle_starttag(self, tag, attrs):
            for name, value in attrs:
                if name in {'href', 'hx-post', 'hx-delete', 'sse-connect'} and value and value.startswith('/'):
                    assert not prefix or value.startswith(prefix + '/'), (name, value)

    request = Request({'type': 'http', 'method': 'GET', 'scheme': 'http',
                       'server': ('test', 80), 'path': prefix + '/runs/r1',
                       'root_path': prefix, 'headers': []})
    run = Run(id='r1', workflow_name='pilot', backend_name='midway2',
              created_at=datetime.now(timezone.utc), items=[], max_concurrent=None,
              artifact_refs={'request_id': 'b' * 32, 'inputs': [
                  {'name': 'inputs', 'version': 'sha256:' + 'a' * 64}], 'outputs': []})
    templates = Jinja2Templates(directory=str(Path(__file__).parents[1] / 'templates'))
    context = dict(request=request, run=run, runs=[run], error=None,
                   gantt_items=[], markers=[], cost_summary=None)
    for name in ['run_detail.html', 'run_info.html', 'runs.html', 'runs_list.html']:
        html = templates.env.get_template(name).render(context)
        LocalRoutes().feed(html)
        if name == 'run_detail.html':
            assert 'sse-connect="' + prefix + '/runs/r1/events"' in html
            assert 'const scriptHutRoot = "' + prefix + '";' in html
            assert "fetch('/runs/" not in html
            assert 'fetch(`/api/' not in html
            expected_stream = f"new EventSource(\"{prefix}\" + '/notifications/stream')"
            assert expected_stream in html


@pytest.mark.asyncio
@pytest.mark.parametrize('prefix', ['', '/mounted'])
async def test_project_browser_data_history_and_pdf_selection(monkeypatch, prefix):
    version = 'sha256:' + 'a' * 64
    group = 'b' * 64
    async def metadata(path):
        from urllib.parse import urlsplit, parse_qs
        url = urlsplit(path)
        query = parse_qs(url.query)
        if url.path == 'artifact-catalog/storage':
            raise ConnectionError('storage unavailable')
        if url.path == 'artifact-catalog/projects':
            items = [{'project': 'pilot', 'logical_bytes': 120, 'report_count': 1,
                      'data_count': 1, 'latest_report': 12345}]
        elif url.path in {'artifact-catalog/publications', 'artifact-catalog/history'}:
            items = [{'project': 'pilot', 'kind': 'dataset' if query.get('kind') == ['data'] else 'report',
                      'name': '<Report>', 'created': 12345, 'file_count': 2, 'logical_bytes': 22,
                      'status': 'completed', 'run_id': 'r1', 'version': version,
                      'version_count': 2, 'group': group}]
        else:
            return {'id': version, 'entries': [{'project': 'pilot', 'name': 'Report', 'kind': 'report'}],
                    'runs': [], 'manifest': {'files': [{'path': 'b.PDF', 'size': 12}, {'path': 'a.pdf', 'size': 10}]}}
        return {'schema_version': 1, 'items': items, 'page': 1, 'limit': 50, 'total': len(items)}
    monkeypatch.setattr(artifact_views, 'metadata', metadata)
    app = FastAPI(root_path=prefix)
    app.include_router(artifact_views.make_artifact_views(Jinja2Templates(
        directory=str(Path(__file__).parents[1] / 'templates'))))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.get(prefix + '/artifacts')
        assert response.status_code == 200
        assert 'pilot' in response.text and 'Storage measurement unavailable' in response.text
        assert prefix + '/artifacts/projects/pilot' in response.text
        response = await client.get(prefix + '/artifacts/projects/pilot')
        assert response.status_code == 200
        assert 'Open report' in response.text and '&lt;Report&gt;' in response.text
        response = await client.get(prefix + '/artifacts/projects/pilot?kind=data')
        assert 'Browse files' in response.text and 'dataset' in response.text
        response = await client.get(prefix + '/artifacts/history/' + group)
        assert 'Include partial outputs' in response.text
        response = await client.get(prefix + '/artifacts/' + version)
        assert 'title="PDF preview: a.pdf"' in response.text
        response = await client.get(prefix + '/artifacts/' + version + '?pdf=b.PDF')
        assert 'title="PDF preview: b.PDF"' in response.text
        assert prefix + '/api/v1/artifacts/' in response.text
        assert (await client.get(prefix + '/artifacts?page=0')).status_code == 400
