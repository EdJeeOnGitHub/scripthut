"""Artifact routes under the existing access boundary, streaming to a private socket."""
from __future__ import annotations

import os
import re
from urllib.parse import quote
from collections.abc import AsyncIterator

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask


MAX_METADATA = 16 * 1024 * 1024 + 16384
MAX_PATCH = 8 * 1024 * 1024


def allowed(method: str, path: str) -> bool:
    session = r'[a-f0-9]{32}'
    file = re.fullmatch(r'artifacts/sha256:[a-f0-9]{64}/files/(.+)', path)
    if method in {'GET', 'HEAD'} and file:
        name = file.group(1)
        return (all(part not in {'', '.', '..'} for part in name.split('/'))
                and '\\' not in name and not any(ord(c) < 32 or ord(c) == 127 for c in name))
    if method == 'GET' and path in {
        'artifact-catalog/projects', 'artifact-catalog/publications',
        'artifact-catalog/history', 'artifact-catalog/storage',
    }:
        return True
    if method == 'GET' and re.fullmatch(r'artifacts(?:/sha256:[a-f0-9]{64})?', path):
        return True
    if method == 'POST' and path == 'artifact-uploads':
        return True
    if method == 'POST' and path == 'artifact-imports':
        return True
    if method == 'GET' and re.fullmatch(r'artifact-imports/' + session, path):
        return True
    if method == 'POST' and re.fullmatch(r'artifact-imports/' + session + '/retry', path):
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
    @router.api_route('/artifact-imports', methods=['POST'])
    @router.api_route('/artifact-imports/{suffix:path}', methods=['GET', 'POST'])
    @router.api_route('/artifact-catalog/{suffix:path}', methods=['GET'])
    @router.api_route('/artifacts', methods=['GET'])
    @router.api_route('/artifacts/{suffix:path}', methods=['GET', 'HEAD'])
    async def proxy(request: Request, suffix: str = '') -> StreamingResponse:
        path = request.url.path.removeprefix('/api/v1/')
        root = request.scope.get('root_path', '')
        if root:
            path = request.url.path.removeprefix(root).removeprefix('/api/v1/')
        catalog_params = {
            'artifact-catalog/projects': {'q', 'page', 'limit'},
            'artifact-catalog/publications': {'project', 'kind', 'q', 'page', 'limit', 'include_partial'},
            'artifact-catalog/history': {'group', 'page', 'limit', 'include_partial'},
            'artifact-catalog/storage': set(),
        }
        query_keys = [key for key, _ in request.query_params.multi_items()]
        valid_query = (path in catalog_params and set(query_keys) <= catalog_params[path]
                       and len(query_keys) == len(set(query_keys)))
        if (request.url.query and not valid_query) or not allowed(request.method, path):
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
        limit = (MAX_PATCH if request.method == 'PATCH' else MAX_METADATA if path == 'artifact-uploads'
                 else 16384 if path == 'artifact-imports' else 0)
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
        for name in ('Content-Type', 'X-Artifact-Token', 'Upload-Offset', 'Range', 'If-Range'):
            if value := request.headers.get(name):
                headers[name] = value
        client = httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket),
                                   timeout=httpx.Timeout(130, connect=10), follow_redirects=False)
        try:
            upstream = await client.send(client.build_request(request.method, 'http://artifacts/' + quote(path, safe='/:') +
                                                               ('?' + request.url.query if request.url.query else ''),
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
                                                'upload-length', 'tus-resumable', 'location', 'content-range',
                                                'content-disposition', 'accept-ranges', 'etag', 'cache-control',
                                                'x-content-type-options', 'content-security-policy'}}
        return StreamingResponse(response_body(), status_code=upstream.status_code,
                                 headers=response_headers, background=BackgroundTask(close))

    return router
