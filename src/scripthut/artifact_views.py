"""Artifact catalogue pages backed by the private authoritative registry."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
import re
from typing import Any
from urllib.parse import quote, urlencode

import httpx
from fastapi import APIRouter, Request, HTTPException
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

    def byte_size(value):
        size = float(value or 0)
        for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
            if size < 1024 or unit == 'TiB':
                return f'{size:,.1f} {unit}' if unit != 'B' else f'{size:,.0f} B'
            size /= 1024

    templates.env.filters['artifact_bytes'] = byte_size
    templates.env.filters['artifact_date'] = lambda value: (
        datetime.fromtimestamp(value, timezone.utc).strftime('%d %b %Y · %H:%M UTC') if value else 'No reports yet')

    async def storage():
        try:
            return await metadata('artifact-catalog/storage')
        except (httpx.HTTPError, ConnectionError, ValueError, KeyError):
            return {'available': False}

    async def browse(request, project=None, group=None):
        context = dict(request=request, mode='history' if group else 'project' if project else 'projects',
                       project=project, group=group, error=None, legacy=False, storage={'available': False})
        query = request.query_params
        kind = query.get('kind', 'report')
        if kind not in {'report', 'data'}:
            raise HTTPException(400, 'Unknown artifact category')
        try:
            page = int(query.get('page', '1'))
            if page < 1:
                raise ValueError()
        except ValueError:
            raise HTTPException(400, 'Invalid page') from None
        params = {'page': str(page), 'limit': '50'}
        if group:
            params['group'] = group
            params['include_partial'] = query.get('include_partial', 'false')
        else:
            params['q'] = query.get('q', '')
            if project:
                params.update(project=project, kind=kind, include_partial=query.get('include_partial', 'false'))
        toggle = dict(query)
        toggle.update(include_partial='false' if params.get('include_partial') == 'true' else 'true', page='1')
        context.update(kind=kind, search=query.get('q', ''), include_partial=params.get('include_partial') == 'true',
                       toggle_partial_url=str(request.url.replace(query=urlencode(toggle))))
        operation = 'history' if group else 'publications' if project else 'projects'
        status = 200
        try:
            catalog, context['storage'] = await asyncio.gather(
                metadata('artifact-catalog/' + operation + '?' + urlencode(params)), storage())
            if catalog.get('schema_version') != 1:
                raise ValueError('Unsupported catalog version')
            context['catalog'] = catalog
            if group and catalog['items']:
                context['project'] = catalog['items'][0]['project']
                context['title'] = catalog['items'][0]['name']
            for direction, number in [('previous', page - 1), ('next', page + 1)]:
                updated = dict(query)
                updated['page'] = str(number)
                context[direction + '_url'] = str(request.url.replace(query=urlencode(updated)))
        except httpx.HTTPStatusError as exc:
            if not project and not group and exc.response.status_code in {404, 501}:
                try:
                    context.update(legacy=True, artifacts=(await metadata('artifacts'))['artifacts'])
                except (httpx.HTTPError, ConnectionError, ValueError, KeyError):
                    status, context['error'] = 503, 'Artifacts are temporarily unavailable.'
            else:
                status = 404 if exc.response.status_code == 404 else 503
                context['error'] = 'Project or publication not found.' if status == 404 else 'Artifacts are temporarily unavailable.'
        except (httpx.HTTPError, ConnectionError, ValueError, KeyError):
            status, context['error'] = 503, 'Artifacts are temporarily unavailable. Your run history is still available.'
        return templates.TemplateResponse(request=request, name='artifact_browser.html', context=context, status_code=status)

    @router.get('/artifacts', response_class=HTMLResponse, name='artifact_list')
    async def listing(request: Request):
        return await browse(request)

    @router.get('/artifacts/projects/{project}', response_class=HTMLResponse, name='artifact_project')
    async def project_page(request: Request, project: str):
        return await browse(request, project=project)

    @router.get('/artifacts/history/{group}', response_class=HTMLResponse, name='artifact_history')
    async def history(request: Request, group: str):
        if not re.fullmatch('[a-f0-9]{64}', group):
            raise HTTPException(404, 'Publication not found')
        return await browse(request, group=group)

    @router.get('/artifacts/{version}', response_class=HTMLResponse, name='artifact_detail')
    async def detail(request: Request, version: str):
        if not re.fullmatch(r'sha256:[a-f0-9]{64}', version):
            raise HTTPException(404, 'Artifact not found')
        context = dict(request=request, artifact=None, error=None)
        status = 200
        try:
            artifact = await metadata('artifacts/' + version)
            files = [dict(item, url=str(request.base_url).rstrip('/') + '/api/v1/artifacts/' +
                          version + '/files/' + quote(item['path'], safe='/')) for item in artifact['manifest']['files']]
            pdfs = sorted([f for f in files if f['path'].lower().endswith('.pdf')], key=lambda f: f['path'])
            selected = next((f for f in pdfs if f['path'] == request.query_params.get('pdf')), None)
            selected_entries = [e for e in artifact['entries'] if e['project'] == request.query_params.get('project')]
            context.update(artifact=artifact, entries=selected_entries or artifact['entries'],
                           files=files, pdfs=pdfs, selected_pdf=selected or (pdfs[0] if pdfs else None))
        except httpx.HTTPStatusError as exc:
            status = 404 if exc.response.status_code == 404 else 503
            context['error'] = 'Artifact not found.' if status == 404 else 'Artifact details are temporarily unavailable.'
        except (httpx.HTTPError, ConnectionError, ValueError, KeyError):
            status = 503
            context['error'] = 'Artifact details are temporarily unavailable. Your run history is still available.'
        return templates.TemplateResponse(request=request, name='artifacts.html', context=context, status_code=status)

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
