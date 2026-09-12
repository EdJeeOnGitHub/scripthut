import asyncio

import httpx
import pytest
from fastapi import FastAPI

from scripthut.artifact_api import MAX_PATCH, make_artifact_router


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
