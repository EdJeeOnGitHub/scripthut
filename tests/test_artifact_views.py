from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from scripthut import artifact_views


@pytest.mark.asyncio
async def test_catalogue_file_links_escape_metadata_and_survive_outage(monkeypatch):
    version = 'sha256:' + 'a' * 64
    entry = {'project': 'pilot', 'name': '<script>unsafe</script>', 'kind': 'report', 'version': version}

    async def metadata(path):
        if path == 'artifacts':
            return {'artifacts': [entry]}
        return {'id': version, 'entries': [entry], 'manifest': {'files': [
            {'path': 'summary é.pdf', 'size': 42}]}, 'runs': []}

    monkeypatch.setattr(artifact_views, 'metadata', metadata)
    app = FastAPI()
    templates = Jinja2Templates(directory=str(Path(__file__).parents[1] / 'templates'))
    app.include_router(artifact_views.make_artifact_views(templates))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.get('/artifacts')
        assert response.status_code == 200
        assert '&lt;script&gt;unsafe&lt;/script&gt;' in response.text
        assert '<script>unsafe</script>' not in response.text
        response = await client.get('/artifacts/' + version)
        assert response.status_code == 200
        assert '/files/summary%20%C3%A9.pdf' in response.text

        async def unavailable(path):
            raise ConnectionError('offline')

        monkeypatch.setattr(artifact_views, 'metadata', unavailable)
        response = await client.get('/artifacts/' + version)
        assert response.status_code == 503
        assert 'run history is still available' in response.text
