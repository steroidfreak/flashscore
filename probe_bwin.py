"""Quick probe of bwin tennis live page: check anti-bot, DOM structure, selectors."""
import asyncio
import sys
from playwright.async_api import async_playwright

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

URL = "https://www.bwin.com/en/sports/tennis-5"

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

        print(f"[*] Navigating to {URL}")
        try:
            resp = await page.goto(URL, wait_until="domcontentloaded", timeout=45000)
            print(f"[*] Status: {resp.status if resp else 'N/A'}")
            print(f"[*] Final URL: {page.url}")
        except Exception as e:
            print(f"[!] goto failed: {e}")

        await page.wait_for_timeout(6000)

        title = await page.title()
        print(f"[*] Page title: {title}")

        # Look for common live-event containers / anti-bot signals
        body_text = (await page.inner_text("body"))[:500]
        print(f"[*] First 500 chars of body text:\n{body_text}\n")

        # Check for Cloudflare / access-denied pages
        for sig in ["cloudflare", "access denied", "captcha", "verify you are", "bot"]:
            if sig.lower() in body_text.lower():
                print(f"[!] Anti-bot signal detected: '{sig}'")

        # Count candidate match rows using a few likely selectors
        candidates = [
            "ms-event",                        # Entain-family SPA component
            "[class*='event']",
            "a[href*='/sports/tennis']",
            "[data-testid*='event']",
            "ms-live-event",
            "ms-six-pack-event",
            ".grid-event",
            ".grid-scoreboard",
        ]
        for sel in candidates:
            try:
                n = await page.locator(sel).count()
                print(f"    {sel:40s} -> {n}")
            except Exception as e:
                print(f"    {sel:40s} -> err: {e}")

        # Dump first 3 links that look like tennis events
        print("\n[*] Sample tennis-ish links:")
        hrefs = await page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.getAttribute('href')).filter(h => h && h.includes('tennis')).slice(0, 10)"
        )
        for h in hrefs:
            print(f"    {h}")

        await page.screenshot(path="C:/flashscore/bwin_probe.png", full_page=False)
        print("\n[*] Screenshot -> bwin_probe.png")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
