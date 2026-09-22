import asyncio

import httpx
import pytest
from fastapi import FastAPI

from scripthut.artifact_api import MAX_PATCH, allowed, make_artifact_router


def test_registered_import_routes_are_bounded():
    session = 'a' * 32
    assert allowed('POST', 'artifact-imports')
    assert allowed('GET', 'artifact-imports/' + session)
    assert allowed('POST', 'artifact-imports/' + session + '/retry')
    assert not allowed('POST', 'artifact-imports/' + session + '/arbitrary-command')
    assert not allowed('GET', 'artifact-imports/not-an-id')
    base = 'artifacts/sha256:' + 'b' * 64 + '/files/'
    assert allowed('GET', base + 'report é.pdf')
    assert allowed('HEAD', base + 'report.pdf')
    for path in ('../host.json', 'nested/../../host.json', 'a\\b', 'a\nb', '/absolute'):
        assert not allowed('GET', base + path)


@pytest.mark.asyncio
@pytest.mark.parametrize('prefix', ['', '/artifact-candidate'])
async def test_pdf_range_headers_and_encoded_filename(tmp_path, monkeypatch, prefix):
    socket = str(tmp_path / 'download.sock')
    monkeypatch.setenv('SCRIPTHUT_ARTIFACT_SOCKET', socket)
    captured = asyncio.get_running_loop().create_future()

    async def serve(reader, writer):
        try:
            headers = await reader.readuntil(b'\r\n\r\n')
            captured.set_result(headers)
            writer.write(b'HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\n'
                         b'Content-Type: application/pdf\r\nContent-Range: bytes 0-4/100\r\n'
                         b'Accept-Ranges: bytes\r\nETag: "fixed"\r\n'
                         b'Content-Disposition: inline\r\nX-Content-Type-Options: nosniff\r\n'
                         b'Content-Security-Policy: sandbox\r\nConnection: close\r\n\r\n%PDF-')
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(serve, path=socket)
    app = FastAPI(root_path=prefix)
    app.include_router(make_artifact_router())
    path = prefix + '/api/v1/artifacts/sha256:' + 'b' * 64 + '/files/report%20%C3%A9.pdf'
    async with server, httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.get(path, headers={'Range': 'bytes=0-4', 'If-Range': '"fixed"'})
    assert response.status_code == 206
    assert response.content == b'%PDF-'
    assert response.headers['content-range'] == 'bytes 0-4/100'
    assert response.headers['content-disposition'] == 'inline'
    assert response.headers['x-content-type-options'] == 'nosniff'
    assert response.headers['content-security-policy'] == 'sandbox'
    headers = await asyncio.wait_for(captured, 5)
    assert b'/files/report%20%C3%A9.pdf HTTP/1.1' in headers
    assert b'Range: bytes=0-4' in headers
    assert b'If-Range: "fixed"' in headers


@pytest.mark.asyncio
async def test_limits_and_closed_routes(monkeypatch):
    app = FastAPI()
    app.include_router(make_artifact_router())
    monkeypatch.delenv('SCRIPTHUT_ARTIFACT_SOCKET', raising=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        assert (await client.get('/api/v1/artifacts')).status_code == 503
        monkeypatch.setenv('SCRIPTHUT_ARTIFACT_SOCKET', '/not/a/socket')
        assert (await client.get('/api/v1/artifacts/../host.json')).status_code == 404
        assert (await client.get('/api/v1/artifacts?url=http://other')).status_code == 404
        path = '/api/v1/artifact-uploads/' + 'a'*32 + '/blobs/' + 'b'*32
        assert (await client.patch(path, headers={'Content-Length': str(MAX_PATCH+1)})).status_code == 413
        assert (await client.post('/api/v1/artifact-uploads', headers={'Transfer-Encoding':'chunked'})).status_code == 400
        assert (await client.get('/api/v1/artifact-uploads/not-an-id')).status_code == 404
        assert (await client.get('/api/v1/artifacts/sha256:'+'c'*64)).status_code == 503
        assert (await client.post('/api/v1/artifact-imports', headers={'Content-Length': '16385'})).status_code == 413


@pytest.mark.asyncio
async def test_patch_is_forwarded_before_full_body_arrives(tmp_path, monkeypatch):
    socket = str(tmp_path/'artifacts.sock')
    monkeypatch.setenv('SCRIPTHUT_ARTIFACT_SOCKET', socket)
    first_received = asyncio.Event()
    completed = asyncio.get_running_loop().create_future()
    block = b'binary\x00\xff' * 8192
    payload = block * 16
    async def serve(reader, writer):
        try:
            headers = await reader.readuntil(b'\r\n\r\n')
            first = await reader.readexactly(len(block))
            first_received.set()
            rest = await reader.readexactly(len(payload)-len(first))
            completed.set_result((headers,first+rest))
            writer.write(b'HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nUpload-Offset: '+str(len(payload)).encode()+b'\r\nConnection: close\r\n\r\n')
            await writer.drain()
        except BaseException as exc:
            if not completed.done():completed.set_exception(exc)
        finally:
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_unix_server(serve,path=socket)
    async def body():
        yield block
        await asyncio.wait_for(first_received.wait(), 5)
        yield payload[len(block):]
    app = FastAPI()
    app.include_router(make_artifact_router())
    path = '/api/v1/artifact-uploads/'+'a'*32+'/blobs/'+'b'*32
    async with server, httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
        response = await client.patch(path,content=body(),headers={'Content-Length':str(len(payload)),
                                      'X-Artifact-Token':'session-secret','Upload-Offset':'0'})
        assert response.status_code==204
        assert response.headers['Upload-Offset']==str(len(payload))
    headers,received = await asyncio.wait_for(completed,5)
    assert received==payload
    assert b'X-Artifact-Token: session-secret' in headers
    assert b'PATCH /artifact-uploads/' in headers


@pytest.mark.asyncio
async def test_catalog_proxy_query_allowlist(tmp_path, monkeypatch):
    socket = str(tmp_path / 'catalog.sock')
    monkeypatch.setenv('SCRIPTHUT_ARTIFACT_SOCKET', socket)
    captured = []
    async def serve(reader, writer):
        captured.append(await reader.readuntil(b'\r\n\r\n'))
        writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}')
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    server = await asyncio.start_unix_server(serve, path=socket)
    app = FastAPI()
    app.include_router(make_artifact_router())
    async with server, httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.get('/api/v1/artifact-catalog/publications?project=pilot&kind=data&page=2')
        assert response.status_code == 200
        for path in ('projects?url=http://other', 'projects?page=1&page=2', 'storage?path=/etc', '../host.json'):
            assert (await client.get('/api/v1/artifact-catalog/' + path)).status_code == 404
        assert (await client.post('/api/v1/artifact-catalog/projects')).status_code == 405
    assert len(captured) == 1
    assert b'?project=pilot&kind=data&page=2 HTTP/1.1' in captured[0]
