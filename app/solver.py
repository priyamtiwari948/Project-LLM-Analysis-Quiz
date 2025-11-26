import re, time, asyncio, pandas as pd
from urllib.parse import urlparse
from app.scraper import fetch_quiz_page_html
from app.utils import (
    extract_submit_url, extract_download_links, http_get_bytes,
    http_post_json, find_question_text, sum_value_column_in_pdf, decode_atob_blocks
)

# -------------------------- WINDOWS EVENT LOOP FIX --------------------------
import sys
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# ----------------------------------------------------------------------------

async def solve_single(url: str, email: str, secret: str) -> dict:
    html = await fetch_quiz_page_html(url)
    submit_url = extract_submit_url(html, url)
    
    if not submit_url:
        # sometimes in decoded atob text
        for decoded in decode_atob_blocks(html):
            submit_url = extract_submit_url(decoded, url) or submit_url
    if not submit_url:
        raise ValueError("Submit URL not found on quiz page.")

    qtext = find_question_text(html)
    answer = None

    # Pattern 1: Scrape secret code from a data page
    if re.search(r'scrape.*secret.*code', qtext, re.I | re.DOTALL):
        links = extract_download_links(html)
        data_url = None
        relative_match = re.search(r'(/[\w\-]+\?[^\s\)]*)', qtext)
        if relative_match:
            from urllib.parse import urljoin
            data_url = urljoin(url, relative_match.group(1))
        else:
            for link in links:
                if 'scrape-data' in link or 'data' in link:
                    data_url = link
                    break
        
        if data_url:
            data_html = await fetch_quiz_page_html(data_url)
            secret_match = re.search(r'(?:secret\s+code|code)\s+is\s+(?:<[^>]+>)?([A-Za-z0-9\-]+)', data_html, re.I)
            if not secret_match:
                secret_match = re.search(r'(?:secret|code)[\s:]+([A-Za-z0-9\-]+)', data_html, re.I)
            if not secret_match:
                secret_match = re.search(r'<strong>(\d+)</strong>', data_html)
            if not secret_match:
                secret_match = re.search(r'\b([A-Z0-9]{6,})\b', data_html)
            if secret_match:
                answer = secret_match.group(1)

    # Pattern 2: CSV file with cutoff filtering
    if answer is None and re.search(r'csv.*cutoff', qtext, re.I | re.DOTALL):
        cutoff_match = re.search(r'cutoff[:\s]+(\d+)', qtext, re.I)
        cutoff = int(cutoff_match.group(1)) if cutoff_match else None
        
        links = extract_download_links(html)
        csv_link = None
        for link in links:
            if link.lower().endswith('.csv'):
                csv_link = link
                break
        
        if not csv_link:
            csv_href_match = re.search(r'href=["\']([^"\']+\.csv)["\']', html, re.I)
            if csv_href_match:
                from urllib.parse import urljoin
                csv_link = urljoin(url, csv_href_match.group(1))
        
        if csv_link and cutoff is not None:
            csv_bytes = await http_get_bytes(csv_link)
            df = pd.read_csv(pd.io.common.BytesIO(csv_bytes), header=None)
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            if len(numeric_cols) >= 2:
                filtered_df = df[df[numeric_cols[0]] > cutoff]
                answer = float(filtered_df[numeric_cols[1]].sum())
            elif len(numeric_cols) == 1:
                col = numeric_cols[0]
                answer = float(df[df[col] > cutoff][col].sum())

    # Pattern 3: PDF table on page 2
    if answer is None and re.search(r'\btable on page\s*2\b', qtext, re.I) and "value" in qtext.lower():
        links = extract_download_links(html)
        pdf_links = [u for u in links if urlparse(u).path.lower().endswith(".pdf")]
        if not pdf_links:
            for decoded in decode_atob_blocks(html):
                pdf_links += [u for u in re.findall(r'https?://[^\s"<>]+', decoded) if u.lower().endswith(".pdf")]
        if pdf_links:
            pdf_bytes = await http_get_bytes(pdf_links[0])
            answer = sum_value_column_in_pdf(pdf_bytes, page_index=1, column_name="value")

    # Pattern 4: Generic CSV/Excel sum
    if answer is None:
        links = extract_download_links(html)
        data_link = next((u for u in links if urlparse(u).path.lower().endswith((".csv", ".xlsx", ".xls"))), None)
        if data_link:
            b = await http_get_bytes(data_link)
            if data_link.lower().endswith(".csv"):
                df = pd.read_csv(pd.io.common.BytesIO(b))
            else:
                df = pd.read_excel(pd.io.common.BytesIO(b))
            target = next((c for c in df.columns if str(c).strip().lower() == "value"), None)
            if target is None:
                target = next((c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])), None)
            if target:
                answer = float(df[target].fillna(0).sum())

    if answer is None:
        answer = "unhandled_question"

    payload = {"email": email, "secret": secret, "url": url, "answer": answer}
    result = await http_post_json(submit_url, payload)
    return {"question": qtext[:280], "submitted_to": submit_url, "answer": answer, "result": result}


async def solve_quiz_chain(start_url: str, email: str, secret: str) -> list[dict]:
    t0 = time.time()
    url = start_url
    out = []
    while url and (time.time() - t0) < 180:
        res = await solve_single(url, email, secret)
        out.append(res)
        url = res.get("result", {}).get("url")
    return out
