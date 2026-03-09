from __future__ import annotations

"""
High-performance BAM curve fetcher.

Architecture and speed gains:
- Direct CSV endpoint first (usually 1 HTTP call/date instead of 2+).
- Async bulk download with aiohttp + connection pooling + DNS cache.
- Intelligent retries with exponential backoff on transient failures.
- Disk cache first: cached dates are loaded immediately (no network).
- "No data" dates are cached as empty files to avoid repeated retries.

This file is a drop-in replacement that keeps the same public interface:
- BamCurveFetcher.get_curve(date)
- BamCurveFetcher.get_curves_parallel(dates, max_workers=8, progress_callback=None)
- BamCurveFetcher.cache_stats()
"""

import asyncio
import logging
import time
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urljoin

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)

BASE_URL = (
    "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/"
    "Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
)
BASE_URL_FR = (
    "https://www.bkam.ma/fr/Marches/Principaux-indicateurs/Marche-obligataire/"
    "Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
)
BLOCK_ID = "e1d6b9bbf87f86f8ba53e8518e882982"
DIRECT_CSV_URL = f"https://www.bkam.ma/export/blockcsv/{BLOCK_ID}"
DIRECT_CSV_URL_FR = f"https://www.bkam.ma/fr/export/blockcsv/{BLOCK_ID}"

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
RETRY_BACKOFF = (0.5, 1.0, 2.0)

NO_DATA_MARKER = ""

SYNC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BAM-Fetcher/2.0",
    "Accept": "text/csv,application/octet-stream,text/plain,text/html,*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

ASYNC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BAM-Fetcher/2.0",
    "Accept": "text/csv,application/octet-stream,text/plain,text/html,*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


def _to_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def _parse_rate(raw: object) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip().replace(" ", "")
    if not s:
        return None
    has_percent = "%" in s
    s = s.replace("%", "").replace(",", ".")
    try:
        val = float(s)
    except ValueError:
        return None
    if has_percent or abs(val) > 1:
        val = val / 100.0
    return val


def _parse_date(raw: object) -> Optional[date]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    for dayfirst in (True, False):
        try:
            return pd.to_datetime(s, dayfirst=dayfirst, errors="raise").date()
        except Exception:
            continue
    return None


def _normalize_col(col: str) -> str:
    return (
        col.strip()
        .lower()
        .replace("é", "e")
        .replace("è", "e")
        .replace("ê", "e")
        .replace("à", "a")
        .replace("ù", "u")
        .replace("ï", "i")
        .replace(" ", "")
        .replace("_", "")
        .replace("'", "")
        .replace("-", "")
    )


def _pick_column(columns: list[str], keys: tuple[str, ...]) -> Optional[str]:
    for c in columns:
        nc = _normalize_col(c)
        if any(k in nc for k in keys):
            return c
    return None


def _read_csv_text(text: str) -> pd.DataFrame:
    lines = text.splitlines()
    if lines:
        for i, line in enumerate(lines):
            l = line.lower()
            if ";" in line and "date" in l and "taux" in l:
                text = "\n".join(lines[i:])
                break

    for sep in (";", ",", "\t"):
        try:
            df = pd.read_csv(StringIO(text), sep=sep, dtype=str)
            if df.shape[1] >= 3:
                return df
        except Exception:
            continue
    raise ValueError("Unable to parse CSV content")


def _looks_like_html(text: str) -> bool:
    return "<html" in text[:200].lower()


def _looks_like_csv(text: str) -> bool:
    if not text:
        return False
    if _looks_like_html(text):
        return False
    return (";" in text or "," in text) and "\n" in text


def _direct_csv_urls_for_date(d: date) -> list[str]:
    q = quote(d.strftime("%d/%m/%Y"))
    return [f"{DIRECT_CSV_URL}?date={q}", f"{DIRECT_CSV_URL_FR}?date={q}"]


class BamCurveFetcher:
    def __init__(self, cache_dir: str | Path = "cache_bam_curves", timeout: int = 15) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = int(timeout)
        self._sync_session = requests.Session()
        self._sync_session.headers.update(SYNC_HEADERS)

    def _cache_path(self, d: date) -> Path:
        return self.cache_dir / f"{d.isoformat()}.csv"

    def _save_no_data_marker(self, d: date) -> None:
        self._cache_path(d).write_text(NO_DATA_MARKER, encoding="utf-8")

    def _load_from_cache(self, d: date) -> tuple[list[int], list[float]] | None:
        p = self._cache_path(d)
        if not p.exists():
            return None
        txt = p.read_text(encoding="utf-8", errors="ignore")
        if not txt.strip():
            # Empty marker from old runs: remove it so next run retries network.
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
            return None
        return self._parse_curve(txt, d)

    def get_curve(self, curve_date: date | datetime) -> tuple[list[int], list[float]]:
        d = _to_date(curve_date)
        cached = self._load_from_cache(d)
        if cached is not None:
            return cached
        if self._cache_path(d).exists():
            raise FileNotFoundError(f"No BAM curve CSV for date {d.isoformat()} (cached no-data)")

        txt = self._sync_fetch_direct_csv(d)
        if txt is None:
            txt = self._sync_fetch_via_html(d)

        if not txt:
            raise FileNotFoundError(f"No BAM curve CSV for date {d.isoformat()}")

        self._cache_path(d).write_text(txt, encoding="utf-8")
        return self._parse_curve(txt, d)

    def get_curves_parallel(
        self,
        dates,
        max_workers: int = 8,
        progress_callback=None,
    ) -> dict[date, tuple[list[int], list[float]] | None]:
        unique_dates = sorted({_to_date(d) for d in dates if d is not None})
        total = len(unique_dates)
        results: dict[date, tuple[list[int], list[float]] | None] = {}

        cached_dates: list[date] = []
        network_dates: list[date] = []
        for d in unique_dates:
            if self._cache_path(d).exists():
                cached_dates.append(d)
            else:
                network_dates.append(d)

        for d in cached_dates:
            try:
                results[d] = self._load_from_cache(d)
            except Exception:
                results[d] = None

        done = len(cached_dates)
        if progress_callback:
            progress_callback(done, total, len(cached_dates), len(network_dates), 0.0)

        if network_dates:
            t0 = time.perf_counter()
            async_results = self._run_async(self._fetch_all_curves_async(network_dates, max_workers=max_workers, progress_start=done, total=total, n_cache=len(cached_dates), progress_callback=progress_callback))
            for d, v in async_results.items():
                results[d] = v
            _ = time.perf_counter() - t0

        return results

    def cache_stats(self) -> dict:
        files = list(self.cache_dir.glob("*.csv"))
        dates: list[date] = []
        for f in files:
            try:
                dates.append(date.fromisoformat(f.stem))
            except ValueError:
                continue
        return {
            "total": len(files),
            "date_min": min(dates).isoformat() if dates else "N/A",
            "date_max": max(dates).isoformat() if dates else "N/A",
        }

    async def _fetch_all_curves_async(
        self,
        dates: list[date],
        max_workers: int,
        progress_start: int,
        total: int,
        n_cache: int,
        progress_callback=None,
    ) -> dict[date, tuple[list[int], list[float]] | None]:
        if not dates:
            return {}

        done = progress_start
        started = time.perf_counter()
        results: dict[date, tuple[list[int], list[float]] | None] = {}

        connector = aiohttp.TCPConnector(
            limit=max(10, min(int(max_workers) * 6, 50)),
            limit_per_host=20,
            ttl_dns_cache=300,
        )
        timeout = aiohttp.ClientTimeout(total=self.timeout, connect=min(5, self.timeout))

        async with aiohttp.ClientSession(connector=connector, headers=ASYNC_HEADERS, timeout=timeout) as session:
            tasks = [self._fetch_one_date_async(session, d) for d in dates]
            for coro in asyncio.as_completed(tasks):
                d, curve = await coro
                results[d] = curve
                done += 1

                elapsed = max(time.perf_counter() - started, 0.001)
                speed = done / elapsed
                eta = max((total - done) / max(speed, 1e-6), 0.0)
                if progress_callback:
                    progress_callback(done, total, n_cache, len(dates), eta)

        return results

    async def _fetch_one_date_async(self, session: aiohttp.ClientSession, d: date) -> tuple[date, tuple[list[int], list[float]] | None]:
        # 1) Try direct CSV URL first (fast path)
        txt = await self._async_get_direct_csv_with_retry(session, d)
        if txt:
            try:
                curve = self._parse_curve(txt, d)
                self._cache_path(d).write_text(txt, encoding="utf-8")
                return d, curve
            except Exception:
                pass

        # 2) Fallback to HTML page + CSV link discovery
        txt = await self._async_fetch_via_html_with_retry(session, d)
        if txt:
            try:
                curve = self._parse_curve(txt, d)
                self._cache_path(d).write_text(txt, encoding="utf-8")
                return d, curve
            except Exception:
                pass

        # 3) no data (do not write empty marker to avoid poisoning cache on transient failures)
        return d, None

    async def _async_get_direct_csv_with_retry(self, session: aiohttp.ClientSession, d: date) -> Optional[str]:
        for url in _direct_csv_urls_for_date(d):
            txt = await self._async_get_text_with_retry(session, url=url)
            if txt and _looks_like_csv(txt):
                return txt
        return None

    async def _async_fetch_via_html_with_retry(self, session: aiohttp.ClientSession, d: date) -> Optional[str]:
        payloads = [
            {"date": d.strftime("%d/%m/%Y"), "block": BLOCK_ID},
            {"date": d.strftime("%Y-%m-%d"), "block": BLOCK_ID},
            {"Date": d.strftime("%d/%m/%Y"), "block": BLOCK_ID},
            {"date": d.strftime("%d/%m/%Y")},
            {},
        ]

        for params in payloads:
            for page_url in (BASE_URL, BASE_URL_FR):
                html = await self._async_get_text_with_retry(session, url=page_url, params=params)
                if not html:
                    continue
                links = self._extract_csv_links(html)
                for link in links:
                    csv_txt = await self._async_get_text_with_retry(session, url=link)
                    if csv_txt and _looks_like_csv(csv_txt):
                        return csv_txt
        return None

    async def _async_get_text_with_retry(self, session: aiohttp.ClientSession, url: str, params: Optional[dict] = None) -> Optional[str]:
        for attempt, backoff in enumerate((0.0, *RETRY_BACKOFF), start=1):
            if backoff > 0:
                await asyncio.sleep(backoff)
            try:
                async with session.get(url, params=params) as resp:
                    status = resp.status
                    text = await resp.text(errors="ignore")

                    if status == 404:
                        return None
                    if status in RETRYABLE_STATUSES:
                        if attempt < 4:
                            continue
                        return None
                    if status >= 400:
                        return None
                    if _looks_like_html(text) and "/export/blockcsv/" in url:
                        # direct CSV endpoint returned HTML (not a CSV payload)
                        return None
                    return text
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt < 4:
                    continue
                return None
        return None

    def _sync_fetch_direct_csv(self, d: date) -> Optional[str]:
        for url in _direct_csv_urls_for_date(d):
            txt = self._sync_get_text_with_retry(url=url)
            if txt and _looks_like_csv(txt):
                return txt
        return None

    def _sync_fetch_via_html(self, d: date) -> Optional[str]:
        payloads = [
            {"date": d.strftime("%d/%m/%Y"), "block": BLOCK_ID},
            {"date": d.strftime("%Y-%m-%d"), "block": BLOCK_ID},
            {"Date": d.strftime("%d/%m/%Y"), "block": BLOCK_ID},
            {"date": d.strftime("%d/%m/%Y")},
            {},
        ]
        for params in payloads:
            for page_url in (BASE_URL, BASE_URL_FR):
                html = self._sync_get_text_with_retry(url=page_url, params=params)
                if not html:
                    continue
                for link in self._extract_csv_links(html):
                    txt = self._sync_get_text_with_retry(url=link)
                    if txt and _looks_like_csv(txt):
                        return txt
        return None

    def _sync_get_text_with_retry(self, url: str, params: Optional[dict] = None) -> Optional[str]:
        for attempt, backoff in enumerate((0.0, *RETRY_BACKOFF), start=1):
            if backoff > 0:
                time.sleep(backoff)
            try:
                resp = self._sync_session.get(url, params=params, timeout=self.timeout)
                status = resp.status_code
                text = resp.text
                if status == 404:
                    return None
                if status in RETRYABLE_STATUSES:
                    if attempt < 4:
                        continue
                    return None
                if status >= 400:
                    return None
                if _looks_like_html(text) and "/export/blockcsv/" in url:
                    return None
                return text
            except requests.RequestException:
                if attempt < 4:
                    continue
                return None
        return None

    def _extract_csv_links(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        links: list[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            txt = a.get_text(" ", strip=True).lower()
            if "/export/blockcsv/" in href or "csv" in txt:
                links.append(urljoin(BASE_URL, href))
        return list(dict.fromkeys(links))

    def _parse_curve(self, csv_text: str, curve_date: date) -> tuple[list[int], list[float]]:
        df = _read_csv_text(csv_text)
        cols = list(df.columns)

        col_echeance = _pick_column(cols, ("echeance", "datedecheance"))
        col_valeur = _pick_column(cols, ("datevaleur", "datedelavaleur", "valeur"))
        col_taux = _pick_column(cols, ("taux", "tx"))

        if col_echeance is None and cols:
            col_echeance = cols[0]
        if col_taux is None:
            for c in cols:
                if "taux" in c.lower():
                    col_taux = c
                    break

        if col_echeance is None or col_taux is None:
            raise ValueError("CSV BAM: colonnes echeance/taux introuvables")

        rows = []
        for _, row in df.iterrows():
            d_ech = _parse_date(row.get(col_echeance))
            d_val = _parse_date(row.get(col_valeur)) if col_valeur else curve_date
            t = _parse_rate(row.get(col_taux))
            if d_ech is None or d_val is None or t is None:
                continue
            mt = (d_ech - d_val).days
            if mt <= 0:
                continue
            rows.append((mt, t))

        if not rows:
            raise ValueError("CSV BAM: aucune donnee exploitable")

        out = pd.DataFrame(rows, columns=["mt", "tx"]).dropna()
        out = out.groupby("mt", as_index=False)["tx"].mean().sort_values("mt")
        mt = out["mt"].astype(int).tolist()
        tx = out["tx"].astype(float).tolist()
        if len(mt) < 2:
            raise ValueError("CSV BAM: pas assez de points pour interpolation")
        return mt, tx

    def _run_async(self, coro):
        try:
            return asyncio.run(coro)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()
