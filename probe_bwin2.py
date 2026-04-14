"""Probe 2 for bwin tennis:
1. Click Live filter (or direct live URL) to isolate live matches.
2. Dump ms-event inner structure to find clean home/away/status selectors.
3. Watch for WebSocket frames and auto-updates over ~30s without reload.
"""
import asyncio
import sys
from playwright.async_api import async_playwright

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE = "https://www.bwin.com/en/sports/tennis-5"
LIVE = "https://www.bwin.com/en/sports/live/tennis-5"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1400, "height": 900},
            locale="en-US",
        )
        page = await ctx.new_page()

        # --- WebSocket frame listener ---
        ws_counter = {"opened": 0, "frames": 0}
        def on_ws(ws):
            ws_counter["opened"] += 1
            print(f"[ws] OPEN {ws.url[:80]}...")
            ws.on("framereceived", lambda payload: ws_counter.__setitem__("frames", ws_counter["frames"] + 1))
        page.on("websocket", on_ws)

        # Try live-only URL first
        print(f"[*] Trying LIVE url: {LIVE}")
        try:
            resp = await page.goto(LIVE, wait_until="domcontentloaded", timeout=45000)
            print(f"    status={resp.status if resp else '?'}  final={page.url}")
        except Exception as e:
            print(f"    failed: {e}")

        await page.wait_for_timeout(5000)

        # Dismiss cookie banner if present
        for txt in ["Allow All", "Accept All", "Accept"]:
            try:
                btn = page.get_by_role("button", name=txt)
                if await btn.count():
                    await btn.first.click(timeout=2000)
                    print(f"[*] Clicked cookie button: {txt}")
                    break
            except Exception:
                pass

        # Count ms-event
        n_events = await page.locator("ms-event").count()
        print(f"[*] ms-event count on live URL: {n_events}")

        # If too few, fall back to base URL and click Live chip
        if n_events < 2:
            print("[*] Falling back to base URL + Live filter")
            await page.goto(BASE, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(4000)
            for sel in ["text=Live", "[class*='live-chip']", "a:has-text('Live')"]:
                try:
                    loc = page.locator(sel).first
                    if await loc.count():
                        await loc.click(timeout=3000)
                        print(f"    clicked {sel}")
                        break
                except Exception:
                    pass
            await page.wait_for_timeout(3000)
            n_events = await page.locator("ms-event").count()
            print(f"[*] ms-event count after Live click: {n_events}")

        # Dump structure of first ms-event
        print("\n[*] First ms-event innerText:")
        if n_events > 0:
            for i in range(min(n_events, 3)):
                t = await page.locator("ms-event").nth(i).inner_text()
                print(f"    --- event[{i}] ---")
                for line in t.splitlines():
                    if line.strip():
                        print(f"      {line}")

            # Get outerHTML of first event for selector inspection (trimmed)
            html = await page.locator("ms-event").nth(0).evaluate("el => el.outerHTML")
            print(f"\n[*] First ms-event outerHTML (first 2000 chars):\n{html[:2000]}\n")

            # Snapshot: all event texts at t=0
            snap0 = await page.eval_on_selector_all(
                "ms-event",
                "els => els.map(e => e.innerText.replace(/\\s+/g, ' ').trim())"
            )

            print(f"\n[*] Waiting 30s to check for auto-updates (no reload)...")
            await page.wait_for_timeout(30000)

            snap1 = await page.eval_on_selector_all(
                "ms-event",
                "els => els.map(e => e.innerText.replace(/\\s+/g, ' ').trim())"
            )

            changed = sum(1 for a, b in zip(snap0, snap1) if a != b)
            print(f"[*] After 30s: {len(snap1)} events, {changed} changed text (no reload)")
            print(f"[*] WebSocket: {ws_counter['opened']} opened, {ws_counter['frames']} frames received")
        else:
            print("[!] No ms-event found; bwin structure may have changed")

        await page.screenshot(path="C:/flashscore/bwin_probe2.png", full_page=False)
        print("\n[*] Screenshot -> bwin_probe2.png")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
