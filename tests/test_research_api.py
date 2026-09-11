import httpx
import pytest
from fastapi import FastAPI

from scripthut.research_api import make_research_router


@pytest.mark.asyncio
async def test_unconfigured_and_invalid_operations(monkeypatch):
    monkeypatch.delenv('SCRIPTHUT_RESEARCH_SOCKET', raising=False)
    app = FastAPI()
    app.include_router(make_research_router())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        assert (await client.post('/api/v1/research-runs', json={})).status_code == 503
        assert (await client.get('/api/v1/research-runs/not-a-request')).status_code == 404
        assert (await client.get('/api/v1/research-runs/' + 'a'*32 + '/retry')).status_code == 404


@pytest.mark.asyncio
async def test_unix_proxy_preserves_acceptance_and_conflicts(tmp_path, monkeypatch):
    import asyncio
    socket = str(tmp_path / 'launcher.sock')
    monkeypatch.setenv('SCRIPTHUT_RESEARCH_SOCKET', socket)
    seen = []
    async def serve(reader, writer):
        header = await reader.readuntil(b'\r\n\r\n')
        length = int(next(line.split(b':', 1)[1] for line in header.split(b'\r\n') if line.lower().startswith(b'content-length:')))
        seen.append((header, await reader.readexactly(length)))
        code = b'202 Accepted' if len(seen) == 1 else b'409 Conflict'
        writer.write(b'HTTP/1.1 ' + code + b'\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}')
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    server = await asyncio.start_unix_server(serve, path=socket)
    app = FastAPI()
    app.include_router(make_research_router())
    async with server, httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        assert (await client.post('/api/v1/research-runs', json={'commit': 'a'*40})).status_code == 202
        assert (await client.post('/api/v1/research-runs', json={})).status_code == 409
        assert (await client.post('/api/v1/research-runs', content=b'x'*16385)).status_code == 413
    assert b'POST /research-runs HTTP/1.1' in seen[0][0]
    assert b'commit' in seen[0][1]
