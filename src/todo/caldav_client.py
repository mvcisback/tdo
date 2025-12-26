from __future__ import annotations

from datetime import datetime
from typing import Dict
from uuid import uuid4

import httpx

from .config import CaldavConfig
from .models import Task, TaskPatch, TaskPayload


class CalDAVClient:
    def __init__(self, config: CaldavConfig):
        self._config = config
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> CalDAVClient:
        headers = {"User-Agent": "todo-cli/0.1.0"}
        if self._config.token:
            headers["Authorization"] = f"Bearer {self._config.token}"
        auth = None
        if self._config.password:
            auth = httpx.BasicAuth(self._config.username, self._config.password)
        self._client = httpx.AsyncClient(auth=auth, headers=headers, timeout=10.0)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        if self._client:
            await self._client.aclose()

    async def list_tasks(self) -> list[Task]:
        assert self._client
        payload = self._trade_report()
        response = await self._client.request(
            "REPORT",
            self._config.calendar_url,
            headers={"Depth": "1", "Content-Type": "application/xml"},
            content=payload,
        )
        response.raise_for_status()
        return []

    async def create_task(self, payload: TaskPayload) -> Task:
        assert self._client
        uid = self._uid_from_summary(payload.summary)
        destination = self._resource_url(uid)
        body = self._build_ics(payload.summary, payload.due, payload.priority, payload.x_properties, uid)
        response = await self._client.put(
            destination,
            headers={"Content-Type": "text/calendar"},
            content=body,
        )
        response.raise_for_status()
        return Task(uid=uid, summary=payload.summary, due=payload.due, priority=payload.priority, x_properties=payload.x_properties)

    async def modify_task(self, uid: str, patch: TaskPatch) -> Task:
        assert self._client
        destination = self._resource_url(uid)
        summary = patch.summary or uid
        body = self._build_ics(summary, patch.due, patch.priority, patch.x_properties, uid)
        response = await self._client.put(
            destination,
            headers={"Content-Type": "text/calendar"},
            content=body,
        )
        response.raise_for_status()
        return Task(uid=uid, summary=summary, due=patch.due, priority=patch.priority, x_properties=patch.x_properties)

    async def delete_task(self, uid: str) -> str:
        assert self._client
        response = await self._client.delete(self._resource_url(uid))
        response.raise_for_status()
        return uid

    def _resource_url(self, uid: str) -> str:
        base = self._config.calendar_url.rstrip("/")
        return f"{base}/{uid}.ics"

    def _uid_from_summary(self, summary: str) -> str:
        return f"{summary.replace(' ', '_')}-{uuid4()}"

    def _build_ics(
        self,
        summary: str,
        due: datetime | None,
        priority: int | None,
        x_properties: Dict[str, str],
        uid: str,
    ) -> str:
        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//todo-cli//EN",
            "BEGIN:VTODO",
            f"UID:{uid}",
            f"SUMMARY:{summary}",
        ]
        if priority is not None:
            lines.append(f"PRIORITY:{priority}")
        if due is not None:
            lines.append(f"DUE:{self._format_due(due)}")
        for name, value in x_properties.items():
            lines.append(f"{name}:{value}")
        lines.extend(["END:VTODO", "END:VCALENDAR"])
        return "\r\n".join(lines) + "\r\n"

    def _format_due(self, value: datetime) -> str:
        return value.strftime("%Y%m%dT%H%M%SZ")

    def _trade_report(self) -> str:
        return (
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
            "<c:calendar-query xmlns:c=\"urn:ietf:params:xml:ns:caldav\">"
            "<c:prop><d:getetag xmlns:d=\"DAV:\"/></c:prop>"
            "</c:calendar-query>"
        )
