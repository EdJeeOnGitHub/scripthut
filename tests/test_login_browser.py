"""Real Chromium checks. Install the pinned browser-tests extra and Chromium."""

import asyncio
import socket

import pytest
import uvicorn

from tests.test_browser_login import application

playwright = pytest.importorskip("playwright.async_api")


@pytest.fixture
async def browser_app(monkeypatch):
    app, manager, processes, _ = application(monkeypatch)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    origin = f"http://127.0.0.1:{listener.getsockname()[1]}"
    manager.config.allowed_origins = [origin]
    server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                await asyncio.sleep(0.01)
        yield origin, manager, processes
    finally:
        await manager.close()
        server.should_exit = True
        await asyncio.wait_for(serving, 5)
        listener.close()


@pytest.mark.asyncio
async def test_browser_success_has_no_echo_or_persistent_transcript(browser_app):
    origin, manager, processes = browser_app
    async with playwright.async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            urls, errors = [], []
            page.on("request", lambda request: urls.append(request.url))
            page.on("pageerror", lambda error: errors.append(str(error)))
            await page.goto(origin + "/login/backend/cluster")
            await page.get_by_role("button", name="Connect", exact=True).click()
            await playwright.expect(page.locator("#output")).to_contain_text("Password:")
            await page.locator("#input").focus()
            await page.keyboard.type("BROWSER_SECRET")
            await page.keyboard.press("Enter")
            async with asyncio.timeout(3):
                while processes[0].input != b"BROWSER_SECRET\n":
                    await asyncio.sleep(0.01)
            assert "BROWSER_SECRET" not in await page.content()
            assert await page.locator("#input").input_value() == ""
            processes[0].finish(0)
            await playwright.expect(page.locator("#status")).to_have_text("Connected")
            assert await page.locator("#output").inner_text() == ""
            assert await page.evaluate("localStorage.length + sessionStorage.length") == 0
            assert all(url.startswith(origin + "/") for url in urls)
            assert all("BROWSER_SECRET" not in url for url in urls)
            assert errors == []
            await page.close()
            assert processes[0].signal_count == 0
            assert manager.attempts["cluster"].state == "connected"
        finally:
            await browser.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["cancel", "close", "failure"])
async def test_browser_unsuccessful_lifecycle(browser_app, action):
    origin, manager, processes = browser_app
    async with playwright.async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            await page.goto(origin + "/login/backend/cluster")
            await page.get_by_role("button", name="Connect", exact=True).click()
            await playwright.expect(page.locator("#output")).to_contain_text("Password:")
            if action == "cancel":
                await page.get_by_role("button", name="Cancel", exact=True).click()
                await playwright.expect(page.locator("#status")).to_have_text("Cancelled")
            elif action == "close":
                await page.close()
            else:
                processes[0].finish(6)
                await playwright.expect(page.locator("#status")).to_contain_text(
                    "Authentication failed"
                )
            async with asyncio.timeout(3):
                while manager.attempts["cluster"].state in {"waiting", "authenticating"}:
                    await asyncio.sleep(0.01)
            assert manager.attempts["cluster"].state == (
                "failed" if action == "failure" else "cancelled"
            )
            if action != "close":
                assert await page.locator("#output").inner_text() == ""
        finally:
            await browser.close()


@pytest.mark.asyncio
async def test_second_browser_window_reports_existing_attempt(browser_app):
    origin, manager, processes = browser_app
    async with playwright.async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            context = await browser.new_context()
            first = await context.new_page()
            await first.goto(origin + "/login/backend/cluster")
            await first.get_by_role("button", name="Connect", exact=True).click()
            await playwright.expect(first.locator("#output")).to_contain_text("Password:")
            second = await context.new_page()
            await second.goto(origin + "/login/backend/cluster")
            await second.get_by_role("button", name="Connect", exact=True).click()
            await playwright.expect(second.locator("#status")).to_contain_text(
                "active in another window"
            )
            assert len(processes) == 1
            await second.close()
            assert manager.attempts["cluster"].state == "authenticating"
            await first.close()
            async with asyncio.timeout(3):
                while manager.attempts["cluster"].state == "authenticating":
                    await asyncio.sleep(0.01)
            assert manager.attempts["cluster"].state == "cancelled"
        finally:
            await browser.close()
