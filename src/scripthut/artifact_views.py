"""Artifact catalogue pages backed by the private authoritative registry."""
from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


async def metadata(path: str, socket_env: str = 'SCRIPTHUT_ARTIFACT_SOCKET') -> dict[str, Any]:
    socket = os.environ.get(socket_env)
    if not socket:
        raise ConnectionError('Artifact service is not configured')
    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket), timeout=10) as client:
        response = await client.get('http://artifacts/' + path)
        response.raise_for_status()
        return response.json()


def make_artifact_views(templates: Jinja2Templates) -> APIRouter:
    router = APIRouter()

    async def render(request: Request, version: str | None = None) -> HTMLResponse:
        context: dict[str, Any] = {'request': request, 'artifact': None, 'artifacts': [], 'error': None}
        status = 200
        try:
            if version is None:
                context['artifacts'] = (await metadata('artifacts'))['artifacts']
            else:
                if not re.fullmatch(r'sha256:[a-f0-9]{64}', version):
                    return HTMLResponse('Artifact not found', status_code=404)
                artifact = await metadata('artifacts/' + version)
                context['artifact'] = artifact
                context['files'] = [dict(item, url=str(request.base_url).rstrip('/') +
                    '/api/v1/artifacts/' + version + '/files/' + quote(item['path'], safe='/'))
                    for item in artifact['manifest']['files']]
        except httpx.HTTPStatusError as exc:
            status = 404 if exc.response.status_code == 404 else 503
            context['error'] = 'Artifact not found.' if status == 404 else 'Artifact details are temporarily unavailable.'
        except (httpx.HTTPError, ConnectionError, ValueError, KeyError):
            status = 503
            context['error'] = 'Artifact details are temporarily unavailable. Your run history is still available.'
        return templates.TemplateResponse(request=request, name='artifacts.html', context=context, status_code=status)

    @router.get('/artifacts', response_class=HTMLResponse, name='artifact_list')
    async def listing(request: Request) -> HTMLResponse:
        return await render(request)

    @router.get('/artifacts/{version}', response_class=HTMLResponse, name='artifact_detail')
    async def detail(request: Request, version: str) -> HTMLResponse:
        return await render(request, version)

    @router.get('/research-runs/{request_id}', response_class=HTMLResponse, name='research_preparation')
    async def preparation(request: Request, request_id: str) -> HTMLResponse:
        if not re.fullmatch('[a-f0-9]{32}', request_id):
            return HTMLResponse('Request not found', status_code=404)
        context: dict[str, Any] = {'request': request, 'request_id': request_id, 'record': None, 'error': None}
        status = 200
        try:
            context['record'] = await metadata('research-runs/' + request_id + '/status', 'SCRIPTHUT_RESEARCH_SOCKET')
        except httpx.HTTPStatusError as exc:
            status = 404 if exc.response.status_code == 404 else 503
            context['error'] = 'Request not found.' if status == 404 else 'Preparation details are temporarily unavailable.'
        except (httpx.HTTPError, ConnectionError, ValueError):
            status = 503
            context['error'] = 'Preparation details are temporarily unavailable.'
        return templates.TemplateResponse(request=request, name='research_preparation.html', context=context, status_code=status)

    return router
