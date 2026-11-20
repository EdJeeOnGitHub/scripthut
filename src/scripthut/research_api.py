"""Proxy launcher requests to the private host service, under existing API auth."""
from __future__ import annotations

import os

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response


def make_research_router() -> APIRouter:
    router = APIRouter(prefix='/api/v1/research-runs')

    async def proxy(request: Request, suffix: str = '') -> Response:
        if (request.method == 'POST' and (not suffix or suffix.endswith('/retry'))
                and os.environ.get('SCRIPTHUT_DEPLOYMENT_STATUS_DIR')):
            from scripthut.maintenance import read_status, status_root
            gate = read_status(status_root() / 'current.json')
            if gate.get('accepting_submissions') is not True:
                return JSONResponse(status_code=503, content={
                    'code': 'deployment_in_progress',
                    'deployment_id': gate.get('deployment_id'),
                    'retry_status_url': gate.get('retry_status_url'),
                    'detail': ('Deployment in progress; '
                               'wait for verified readiness before retrying.'),
                })
        socket = os.environ.get('SCRIPTHUT_RESEARCH_SOCKET')
        if not socket:
            raise HTTPException(503, 'Research launcher is not configured')
        if suffix.endswith('/source'):
            raw_length = request.headers.get('content-length', '')
            if not raw_length.isascii() or not raw_length.isdecimal() or len(raw_length) > 10:
                raise HTTPException(411, 'Git bundle transfer requires Content-Length')
            length = int(raw_length)
            if not 0 < length <= 1024**3:
                raise HTTPException(413, 'Git bundle transfer exceeds the source capture limit')
            async def stream_source():
                received = 0
                async for block in request.stream():
                    received += len(block)
                    if received > length:
                        raise HTTPException(400, 'Git bundle exceeds declared size')
                    yield block
                if received != length:
                    raise HTTPException(400, 'Incomplete Git bundle transfer')
            content = stream_source()
            headers = {'Content-Type': 'application/octet-stream', 'Content-Length': raw_length}
        else:
            body = bytearray()
            async for block in request.stream():
                if len(body) + len(block) > 16384:
                    raise HTTPException(413, 'Research launch request exceeds 16 KiB')
                body.extend(block)
            content = bytes(body)
            headers = {'Content-Type': 'application/json'}
        try:
            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket), timeout=130) as client:
                result = await client.request(request.method, 'http://launcher/research-runs' + suffix,
                                              content=content, headers=headers)
        except httpx.HTTPError as exc:
            raise HTTPException(503, 'Research launcher unavailable; preserve your idempotency key') from exc
        return Response(result.content, status_code=result.status_code, media_type='application/json')

    @router.post('')
    async def submit(request: Request) -> Response:
        return await proxy(request)

    @router.get('/source-cache/{project}/{commit}')
    async def source_cache(project: str, commit: str, request: Request) -> Response:
        import re
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}', project) or not re.fullmatch('[a-f0-9]{40}', commit):
            raise HTTPException(400, 'Expected project identifier and full Git SHA')
        return await proxy(request, '/source-cache/' + project + '/' + commit)

    @router.get('/{request_id}')
    async def status(request_id: str, request: Request) -> Response:
        return await operation(request_id, 'status', request)

    @router.api_route('/{request_id}/{action}', methods=['GET', 'POST'])
    async def operation(request_id: str, action: str, request: Request) -> Response:
        import re
        if not re.fullmatch('[a-f0-9]{32}', request_id):
            raise HTTPException(404, 'Unknown research request')
        allowed = {('GET', 'status'), ('GET', 'logs'), ('GET', 'archive'),
                   ('POST', 'retry'), ('POST', 'cancel'), ('POST', 'promote'), ('POST', 'source')}
        if (request.method, action) not in allowed:
            raise HTTPException(404, 'Unknown research operation')
        return await proxy(request, '/' + request_id + '/' + action)

    return router
