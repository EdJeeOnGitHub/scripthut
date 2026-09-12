"""Artifact routes under the existing access boundary, streaming to a private socket."""
from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask


MAX_METADATA = 16 * 1024 * 1024 + 16384
MAX_PATCH = 8 * 1024 * 1024


def allowed(method: str, path: str) -> bool:
    session = r'[a-f0-9]{32}'
    if method == 'GET' and re.fullmatch(r'artifacts(?:/sha256:[a-f0-9]{64})?', path):
        return True
    if method == 'POST' and path == 'artifact-uploads':
        return True
    if method == 'GET' and re.fullmatch(r'(?:artifact-uploads|artifact-transfers)/' + session, path):
        return True
    if method == 'POST' and re.fullmatch(r'artifact-uploads/' + session + r'/(?:complete|retry|cancel)', path):
        return True
    return method in {'HEAD', 'POST', 'PATCH'} and bool(re.fullmatch(
        r'artifact-uploads/' + session + r'/blobs/' + session, path))


def make_artifact_router() -> APIRouter:
    router = APIRouter(prefix='/api/v1')

    @router.api_route('/artifact-uploads', methods=['POST'])
    @router.api_route('/artifact-uploads/{suffix:path}', methods=['GET', 'POST', 'HEAD', 'PATCH'])
    @router.api_route('/artifact-transfers/{suffix:path}', methods=['GET'])
    @router.api_route('/artifacts', methods=['GET'])
    @router.api_route('/artifacts/{suffix:path}', methods=['GET'])
    async def proxy(request: Request, suffix: str = '') -> StreamingResponse:
        path = request.url.path.removeprefix('/api/v1/')
        if request.url.query or not allowed(request.method, path):
            raise HTTPException(404, 'Unknown artifact operation')
        socket = os.environ.get('SCRIPTHUT_ARTIFACT_SOCKET')
        if not socket:
            raise HTTPException(503, 'Artifact service is not configured')
        if request.headers.get('transfer-encoding') or len(request.headers.getlist('content-length')) > 1:
            raise HTTPException(400, 'Use one explicit Content-Length')
        try:
            length = int(request.headers.get('content-length', '0'))
        except ValueError as exc:
            raise HTTPException(400, 'Invalid Content-Length') from exc
        limit = MAX_PATCH if request.method == 'PATCH' else MAX_METADATA if path == 'artifact-uploads' else 0
        if not 0 <= length <= limit:
            raise HTTPException(413, 'Artifact request exceeds size limit')

        async def body() -> AsyncIterator[bytes]:
            received = 0
            async for block in request.stream():
                received += len(block)
                if received > length:
                    raise HTTPException(400, 'Artifact body exceeds declared length')
                # ASGI controls incoming block size; keep forwarding blocks bounded.
                for offset in range(0, len(block), 256 * 1024):
                    yield block[offset:offset + 256 * 1024]
            if received != length:
                raise HTTPException(400, 'Artifact body ended before declared length')

        headers = {'Content-Length': str(length)}
        for name in ('Content-Type', 'X-Artifact-Token', 'Upload-Offset'):
            if value := request.headers.get(name):
                headers[name] = value
        client = httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket),
                                   timeout=httpx.Timeout(130, connect=10), follow_redirects=False)
        try:
            upstream = await client.send(client.build_request(request.method, 'http://artifacts/' + path,
                                                               content=body(), headers=headers), stream=True)
        except BaseException as exc:
            await client.aclose()
            if isinstance(exc, httpx.HTTPError):
                raise HTTPException(503, 'Artifact service unavailable; resume with the same upload ID') from exc
            raise

        async def close() -> None:
            await upstream.aclose()
            await client.aclose()

        async def response_body() -> AsyncIterator[bytes]:
            try:
                async for block in upstream.aiter_raw():
                    yield block
            finally:
                await close()

        response_headers = {name: value for name, value in upstream.headers.items()
                            if name.lower() in {'content-type', 'content-length', 'upload-offset',
                                                'upload-length', 'tus-resumable', 'location'}}
        return StreamingResponse(response_body(), status_code=upstream.status_code,
                                 headers=response_headers, background=BackgroundTask(close))

    return router
