"""Vast marketplace API operations used by the task backend."""

from __future__ import annotations

from typing import Any

import httpx


class VastAPIError(RuntimeError):
    def __init__(self, operation: str, status_code: int) -> None:
        super().__init__(f"Vast {operation} failed (HTTP {status_code})")
        self.status_code = status_code


class VastAPI:
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    async def _request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(
            base_url="https://console.vast.ai/api/v0/",
            headers={"Authorization": f"Bearer {self._api_key}"},
            timeout=20,
        ) as client:
            response = await client.request(method, path, json=payload)
        if response.is_error:
            raise VastAPIError(f"{method} {path}", response.status_code)
        data = response.json()
        if not isinstance(data, dict) or data.get("success") is False:
            raise VastAPIError(f"{method} {path}", response.status_code)
        return data

    async def search_offers(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
        data = await self._request("POST", "bundles/", filters)
        offers = data["offers"]
        if isinstance(offers, dict):
            return [offers]
        if not isinstance(offers, list):
            raise RuntimeError("Vast returned an invalid offer list")
        return offers

    async def create_instance(self, offer_id: int, options: dict[str, Any]) -> str:
        data = await self._request("PUT", f"asks/{offer_id}/", options)
        instance_id = data["new_contract"]
        if type(instance_id) is not int or instance_id <= 0:
            raise RuntimeError("Vast returned an invalid instance ID")
        return str(instance_id)

    async def list_instances(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "instances/")
        instances = data["instances"]
        if not isinstance(instances, list):
            raise RuntimeError("Vast returned an invalid instance list")
        return instances

    async def destroy_instance(self, instance_id: str) -> None:
        if not instance_id.isascii() or not instance_id.isdecimal():
            raise ValueError("Invalid Vast instance ID")
        try:
            await self._request("DELETE", f"instances/{instance_id}/")
        except VastAPIError as exc:
            if exc.status_code != 404:
                raise
