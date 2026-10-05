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
        body = bytearray()
        async for block in request.stream():
            body.extend(block)
            if len(body) > 16384:
                raise HTTPException(413, 'Research launch request exceeds 16 KiB')
        try:
            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket), timeout=130) as client:
                result = await client.request(request.method, 'http://launcher/research-runs' + suffix,
                                              content=bytes(body), headers={'Content-Type': 'application/json'})
        except httpx.HTTPError as exc:
            raise HTTPException(503, 'Research launcher unavailable; preserve your idempotency key') from exc
        return Response(result.content, status_code=result.status_code, media_type='application/json')

    @router.post('')
    async def submit(request: Request) -> Response:
        return await proxy(request)

    @router.get('/{request_id}')
    async def status(request_id: str, request: Request) -> Response:
        return await operation(request_id, 'status', request)

    @router.api_route('/{request_id}/{action}', methods=['GET', 'POST'])
    async def operation(request_id: str, action: str, request: Request) -> Response:
        import re
        if not re.fullmatch('[a-f0-9]{32}', request_id):
            raise HTTPException(404, 'Unknown research request')
        allowed = {('GET', 'status'), ('GET', 'logs'), ('GET', 'archive'),
                   ('POST', 'retry'), ('POST', 'cancel')}
        if (request.method, action) not in allowed:
            raise HTTPException(404, 'Unknown research operation')
        return await proxy(request, '/' + request_id + '/' + action)

    return router
