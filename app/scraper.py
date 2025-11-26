from playwright.async_api import async_playwright

async def fetch_quiz_page_html(url: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle", timeout=120000)
        # Some quizzes populate innerHTML via JS; ensure DOM settled
        await page.wait_for_timeout(500)  # tiny settle
        html = await page.content()
        await browser.close()
        return html
