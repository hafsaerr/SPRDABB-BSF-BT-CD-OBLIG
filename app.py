from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
import asyncio
import base64
import io
import logging
import time
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from statistics import median
from typing import List, Optional, Sequence
from urllib.parse import quote, urljoin

import aiohttp
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

# ═════════════════════════════════════════════════════════════════════════════
# BAM CURVE FETCHER
# ═════════════════════════════════════════════════════════════════════════════

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
        txt = await self._async_get_direct_csv_with_retry(session, d)
        if txt:
            try:
                curve = self._parse_curve(txt, d)
                self._cache_path(d).write_text(txt, encoding="utf-8")
                return d, curve
            except Exception:
                pass

        txt = await self._async_fetch_via_html_with_retry(session, d)
        if txt:
            try:
                curve = self._parse_curve(txt, d)
                self._cache_path(d).write_text(txt, encoding="utf-8")
                return d, curve
            except Exception:
                pass

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


# ═════════════════════════════════════════════════════════════════════════════
# VBA EQUIVALENT RATES (interpolation logique BAM)
# ═════════════════════════════════════════════════════════════════════════════

def _one_year_back_same_day(dt: date) -> date:
    return dt + relativedelta(years=-1)


def _days_in_previous_year_window(dt: date) -> int:
    return (dt - _one_year_back_same_day(dt)).days


def mati(date_debut: date | datetime, i: int) -> int:
    d0 = _to_date(date_debut)
    d1 = d0 + relativedelta(months=12 * i)
    return (d1 - d0).days


def _linear_interp(x: float, x0: float, y0: float, x1: float, y1: float) -> float:
    if x1 == x0:
        return y0
    return ((x - x0) * (y1 - y0) / (x1 - x0)) + y0


def _validate_curve(mt: Sequence[float], tx: Sequence[float]) -> None:
    if len(mt) != len(tx):
        raise ValueError("mt and tx must have same length")
    if len(mt) < 2:
        raise ValueError("curve must contain at least 2 points")


def calcul_taux(
    maturity: float,
    mt: Sequence[float],
    tx: Sequence[float],
    date_debut: date | datetime,
) -> float:
    _validate_curve(mt, tx)
    d0 = _to_date(date_debut)
    seuil_1an = mati(d0, 1)
    last = len(mt) - 1

    if maturity <= mt[0]:
        return tx[0]

    if mt[0] < maturity <= mt[last]:
        for i in range(last):
            b = mt[i]
            a = mt[i + 1]
            if b <= maturity <= a:
                if a <= seuil_1an or b > seuil_1an:
                    return _linear_interp(maturity, b, tx[i], a, tx[i + 1])

                if a > seuil_1an and b <= seuil_1an:
                    if maturity > seuil_1an:
                        date_b = d0 + timedelta(days=int(b))
                        base = _days_in_previous_year_window(date_b)
                        taux = ((1 + tx[i] * b / 360) ** (base / b)) - 1
                        return _linear_interp(maturity, b, taux, a, tx[i + 1])

                    date_a = d0 + timedelta(days=int(a))
                    base = _days_in_previous_year_window(date_a)
                    taux = (360 / a) * (((1 + tx[i + 1]) ** (a / base)) - 1)
                    return _linear_interp(maturity, b, tx[i], a, taux)

        return tx[last]

    return _linear_interp(
        maturity,
        mt[last - 1],
        tx[last - 1],
        mt[last],
        tx[last],
    )


def interpol(
    arg: bool,
    maturity: float,
    mtz: Sequence[float],
    txz: Sequence[float],
    date_debut: date | datetime,
) -> float:
    _validate_curve(mtz, txz)
    d0 = _to_date(date_debut)
    seuil_1an = mati(d0, 1)
    last = len(mtz) - 1

    if arg:
        if maturity <= mtz[0]:
            return txz[0]
        if maturity >= mtz[last]:
            return _linear_interp(
                maturity,
                mtz[last - 1],
                txz[last - 1],
                mtz[last],
                txz[last],
            )
        for i in range(last):
            b = mtz[i]
            a = mtz[i + 1]
            if b <= maturity <= a:
                return _linear_interp(maturity, b, txz[i], a, txz[i + 1])
        return txz[last]

    if maturity <= mtz[0]:
        p = txz[0]
    elif maturity >= mtz[last]:
        p = _linear_interp(
            maturity,
            mtz[last - 1],
            txz[last - 1],
            mtz[last],
            txz[last],
        )
    else:
        p = txz[0]
        for i in range(last):
            b = mtz[i]
            a = mtz[i + 1]
            if not (b <= maturity <= a):
                continue
            if a <= seuil_1an or b > seuil_1an:
                p = _linear_interp(maturity, b, txz[i], a, txz[i + 1])
            else:
                date_b = d0 + timedelta(days=int(b))
                base_b = _days_in_previous_year_window(date_b)
                taux_b_actu = ((1 + txz[i] * b / 360) ** (base_b / b)) - 1
                p_actu = _linear_interp(maturity, b, taux_b_actu, a, txz[i + 1])
                if maturity < seuil_1an:
                    alpha = _days_in_previous_year_window(d0 + timedelta(days=int(maturity)))
                    p = (360 / maturity) * (((1 + p_actu) ** (maturity / alpha)) - 1)
                else:
                    p = p_actu
            break
    return p


def conversion_actu_monnaitaire(
    arg: bool,
    maturity: float,
    date_flux: date | datetime | None,
    mt: Sequence[float],
    tx: Sequence[float],
    date_debut: date | datetime,
) -> float:
    _ = date_flux
    if maturity == 0:
        return 0.0

    d0 = _to_date(date_debut)
    seuil_1an = mati(d0, 1)
    taux = calcul_taux(maturity, mt, tx, d0)

    if arg is False:
        if maturity <= seuil_1an:
            alpha_date = d0 + timedelta(days=int(maturity))
            alpha = _days_in_previous_year_window(alpha_date)
            return ((1 + taux * maturity / 360) ** (alpha / maturity)) - 1
        return taux

    if maturity >= seuil_1an:
        return (360 / maturity) * (((1 + taux) ** (maturity / 365)) - 1)
    return taux


# ═════════════════════════════════════════════════════════════════════════════
# APPLICATION STREAMLIT
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
try:
    import streamlit as _st_tmp
    CREDENTIALS = {_st_tmp.secrets["USERNAME"]: _st_tmp.secrets["PASSWORD"]}
except Exception:
    CREDENTIALS = {"spreadABB": "albarid2026"}
APP_TITLE   = "Spread Manager — BSF & CD | Al Barid Bank"
CACHE_DIR   = Path(__file__).parent / "cache_bam_curves"
ASSETS_DIR  = Path(__file__).parent / "assets"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# BANK ISSUERS TO EXCLUDE (OBLIG_ORDN)
# ─────────────────────────────────────────────────────────────────────────────
BANQUES_EXCLUES = {
    'ATW E', 'BCP E', 'CIH E', 'BOA', 'CAM E', 'CDM', 'CDG E',
    'CFG BANK', 'CFC', 'CFM', 'SAHAM FINANCES', 'BMCI',
    'BMCI LEASI', 'AL BARID BANK E', 'CREDIPER FT',
}
BANK_ISSUERS = BANQUES_EXCLUES

# ─────────────────────────────────────────────────────────────────────────────
# SECTOR MAP (OBLIG_ORDN)
# ─────────────────────────────────────────────────────────────────────────────
SECTEUR_MAP = {
    # ENERGIE
    'ONEE': 'ENERGIE', 'TAQA MOROCCO': 'ENERGIE', 'MASEN': 'ENERGIE',
    'FEC': 'ENERGIE', 'LYDEC': 'ENERGIE', 'SAMIR': 'ENERGIE',
    'SEDM': 'ENERGIE', 'TMPA': 'ENERGIE', 'AFRIQUIA GAZ': 'ENERGIE',
    'AFIQUIA LUB': 'ENERGIE', 'MAGHREB OXYGENE': 'ENERGIE',
    'PHOSPH BOUCRAA': 'ENERGIE',
    # BTP
    'TGCC': 'BTP', 'JET CONTRACTORS': 'BTP', 'ADI': 'BTP',
    'SETTAVEX': 'BTP', 'CMGP GROUP': 'BTP', 'ASSIFILL BUILD': 'BTP',
    'BUILDING LOGIST': 'BTP', 'NADOR WEST MED': 'BTP',
    # CIMENTERIE
    'HOLCIM MAROC': 'CIMENTERIE', 'CIMAT': 'CIMENTERIE',
    # IMMOBILIER
    'CGI': 'IMMOBILIER', 'ALLIANCES DARNA': 'IMMOBILIER',
    'DOUJA PROM ADD': 'IMMOBILIER', 'AL OMRANE': 'IMMOBILIER',
    'IMMOLOG': 'IMMOBILIER', 'ARADEI CAPITAL': 'IMMOBILIER',
    'BEST REAL ESTAT': 'IMMOBILIER', 'HAYAN IMMO SA': 'IMMOBILIER',
    'PALME DEV': 'IMMOBILIER', 'CMR LSTONE': 'IMMOBILIER',
    'Immorent SC': 'IMMOBILIER', 'RDS': 'IMMOBILIER',
    # TRANSPORT
    'ONCF': 'TRANSPORT', 'ONDA': 'TRANSPORT', 'CTM SA': 'TRANSPORT',
    'COMANAV': 'TRANSPORT', 'TANGER MED  SA': 'TRANSPORT',
    'TMSA': 'TRANSPORT', 'ANP': 'TRANSPORT', 'ADM': 'TRANSPORT',
    'MEDIACO MAROC': 'TRANSPORT', 'CMT': 'TRANSPORT',
    # TELECOM
    'MEDI TELCOM SA': 'TELECOM', 'IAM': 'TELECOM',
    # CHIMIE / MINES
    'OCP SA': 'CHIMIE / MINES', 'OCP NUTRICROPS': 'CHIMIE / MINES',
    'MANAGEM': 'CHIMIE / MINES',
    # AGRO-ALIMENTAIRE
    'OULMES': 'AGRO-ALIMENTAIRE', 'UNIMER': 'AGRO-ALIMENTAIRE',
    'AFRICA FEED FOOD': 'AGRO-ALIMENTAIRE',
    'SOMACOVAM': 'AGRO-ALIMENTAIRE', 'MUTANDIS SCA': 'AGRO-ALIMENTAIRE',
    'ZALAGH HOLDING': 'AGRO-ALIMENTAIRE',
    # SIDERURGIE
    'MAGHREB STEEL': 'SIDERURGIE',
    # SANTE
    'AKDITAL': 'SANTE',
    # TOURISME
    'RISMA SA': 'TOURISME', 'MADAEF': 'TOURISME',
    # GRANDE DISTRIBUTION
    'LABEL VIE': 'GRANDE DISTRIBUTION',
    'MARJANE HOLDING': 'GRANDE DISTRIBUTION',
    'RETAIL HOLDING': 'GRANDE DISTRIBUTION',
    # HOLDING
    'AL MADA': 'HOLDING', 'HOLMARCOM FIN CO': 'HOLDING',
    'FINANCECOM': 'HOLDING', 'FINANCIER SEC': 'HOLDING',
    'O CAPITAL GROUP': 'HOLDING', 'GROUP INVEST SA': 'HOLDING',
    'GARAN': 'HOLDING', 'Hold GENE EDU': 'HOLDING',
    # LEASING
    'MAGHREBAIL': 'LEASING', 'MA LEASING': 'LEASING',
    # CREDIT CONSO
    'WAFASALAF': 'CREDIT CONSO', 'TASLIF': 'CREDIT CONSO',
    # ASSURANCE
    'SAHAM': 'ASSURANCE',
    # MICROFINANCE
    'JAIDA': 'MICROFINANCE', 'SFI': 'MICROFINANCE',
    # PARA-PUBLIC
    'SCIF': 'PARA-PUBLIC', 'ORMVAD': 'PARA-PUBLIC',
    'COMMUNE AGADIR': 'PARA-PUBLIC', 'BEST FINANCIERE': 'PARA-PUBLIC',
    'AGRI CAPITAL': 'PARA-PUBLIC',
    # AUTOMOBILE
    'RCI': 'AUTOMOBILE', 'UNIVERS MOTORS': 'AUTOMOBILE',
    'AUTO NEJMA': 'AUTOMOBILE', 'OD MAROC': 'AUTOMOBILE',
    # EDUCATION
    'Univ Mohammed VI': 'EDUCATION',
    # TEXTILE
    'DISTRA-S.A': 'TEXTILE', 'MCM': 'TEXTILE',
    # INDUSTRIE
    'CMB PLA MAROC': 'INDUSTRIE', 'TC3PC': 'INDUSTRIE',
    'FINAN HATT': 'INDUSTRIE',
}

# ─────────────────────────────────────────────────────────────────────────────
# CSS  —  palette Al Barid Bank (thème sombre, orange brûlé)
# ─────────────────────────────────────────────────────────────────────────────
STYLE = """
<style>
[data-testid="stAppViewContainer"] {
    background: #100804 !important;
    color: #EDE0D4 !important;
}
[data-testid="stAppViewContainer"] > .main {
    background: #100804 !important;
    color: #EDE0D4 !important;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1E0A03 0%, #130602 100%) !important;
}
[data-testid="stSidebar"] * { color: #EDE0D4 !important; }

/* inputs */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input {
    background: rgba(255,255,255,0.06) !important;
    color: #EDE0D4 !important;
    border: 1px solid rgba(200,80,30,0.35) !important;
    border-radius: 8px !important;
}
[data-baseweb="select"] > div {
    background: rgba(255,255,255,0.06) !important;
    color: #EDE0D4 !important;
    border: 1px solid rgba(200,80,30,0.35) !important;
}
[data-baseweb="popover"],
[data-baseweb="menu"],
[data-baseweb="option"] { background: #1E0A03 !important; color: #EDE0D4 !important; }
[data-baseweb="option"]:hover { background: rgba(200,80,30,0.25) !important; }
[data-testid="stCheckbox"] label { color: #EDE0D4 !important; }

/* file uploader */
[data-testid="stFileUploaderDropzone"] {
    background: rgba(255,255,255,0.05) !important;
    border: 1px dashed rgba(200,80,30,0.5) !important;
    border-radius: 10px !important;
}
[data-testid="stFileUploaderDropzone"] * { color: #EDE0D4 !important; }

/* dataframe */
[data-testid="stDataFrame"] {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(200,80,30,0.2) !important;
    border-radius: 10px !important;
}

/* progress */
[data-testid="stProgressBar"] > div > div {
    background: linear-gradient(90deg, #C8501E, #FF9060) !important;
}

/* alerts */
.stAlert   { background: rgba(200,80,30,0.10) !important; border: 1px solid rgba(200,80,30,0.35) !important; }
.stInfo    { background: rgba(80,120,200,0.10) !important; border: 1px solid rgba(80,120,200,0.3) !important; }
.stSuccess { background: rgba(60,160,60,0.10)  !important; border: 1px solid rgba(60,160,60,0.3) !important; }
[data-testid="stAlertContainer"] * { color: #EDE0D4 !important; }

/* nav buttons */
.nav-btn button {
    background: linear-gradient(90deg, #C8501E 0%, #7A2E08 100%) !important;
    color: #FFFFFF !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    font-size: 14px !important; padding: 10px !important;
    margin-bottom: 6px !important; width: 100% !important;
}
.nav-btn button:hover { opacity: 0.88 !important; }
.nav-btn-active button {
    background: linear-gradient(90deg, #E07030 0%, #C8501E 100%) !important;
    color: #FFFFFF !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    font-size: 14px !important; padding: 10px !important;
    margin-bottom: 6px !important; width: 100% !important;
    box-shadow: 0 0 0 2px #F5C518 !important;
}

/* generic buttons */
.stButton > button {
    background: linear-gradient(90deg, #C8501E 0%, #7A2E08 100%) !important;
    color: #FFFFFF !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    padding: 10px !important; width: 100% !important;
}
.stButton > button:hover { opacity: 0.88 !important; }
.stDownloadButton > button {
    background: linear-gradient(90deg, #1A6B2E 0%, #0D4019 100%) !important;
    color: white !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    padding: 10px !important; width: 100% !important;
}
.stDownloadButton > button:hover { opacity: 0.88 !important; }

/* KPI cards */
.kpi-card {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(200,80,30,0.3);
    border-top: 3px solid #C8501E;
    border-radius: 10px; padding: 14px 16px;
    text-align: center; margin-bottom: 10px;
}
.kpi-title { font-size: 11px; color: #FFFFFF; font-weight: 600;
             letter-spacing: 0.8px; text-transform: uppercase; }
.kpi-value { font-size: 1.45rem; font-weight: 800; color: #FF9060; }
.kpi-sub   { font-size: 0.75rem; color: #FFFFFF; margin-top: 2px; }

/* section headers */
.section-hdr {
    background: linear-gradient(90deg, #C8501E 0%, #7A2E08 100%);
    color: #FFFFFF !important; padding: 9px 18px;
    border-radius: 8px; font-weight: 700; font-size: 0.95rem;
    margin: 20px 0 12px 0;
}

/* instruction cards */
.instr-card {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(200,80,30,0.25);
    border-left: 4px solid #C8501E;
    border-radius: 10px; padding: 16px 20px; margin-bottom: 14px;
}
.instr-card h4 { color: #FF9060; margin: 0 0 8px; font-size: 0.97rem; }
.instr-card ul { margin: 0; padding-left: 18px; color: #FFFFFF; font-size: 0.88rem; }
.instr-card li { margin-bottom: 4px; }

/* login */
[data-testid="stSidebar"].login-hide { display: none !important; }
[data-testid="stForm"] {
    background: rgba(10,5,2,0.75) !important;
    border: 1px solid rgba(200,80,30,0.25) !important;
    border-radius: 20px !important; padding: 10px 20px 20px !important;
}
[data-testid="stForm"] input {
    background: rgba(255,255,255,0.08) !important; color: #EDE0D4 !important;
    border: 1px solid rgba(200,80,30,0.3) !important; border-radius: 8px !important;
}
[data-testid="stForm"] label { color: #FFFFFF !important; font-weight: 600 !important; }
[data-testid="stFormSubmitButton"] > button {
    background: linear-gradient(90deg, #C8501E, #7A2E08) !important;
    color: white !important; border: none !important; border-radius: 10px !important;
    font-weight: 700 !important; font-size: 15px !important;
    padding: 10px !important; width: 100% !important;
}

#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
[data-testid="stSidebarNav"] { display: none !important; }
</style>
"""

# ─────────────────────────────────────────────────────────────────────────────
# LOGO
# ─────────────────────────────────────────────────────────────────────────────
def _logo_b64() -> str:
    p = ASSETS_DIR / "ALBARID.png"
    if p.exists():
        return base64.b64encode(p.read_bytes()).decode()
    return ""

def _logo_img(size: int = 70) -> str:
    b = _logo_b64()
    if b:
        return (f'<img src="data:image/png;base64,{b}" '
                f'style="width:{size}px;height:{size}px;object-fit:contain;'
                f'border-radius:12px;box-shadow:0 2px 12px rgba(0,0,0,0.5);" />')
    return '<div style="font-size:42px;">🏦</div>'

# ─────────────────────────────────────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────────────────────────────────────
def _show_login() -> None:
    st.markdown("""
    <style>
    [data-testid="stSidebar"] { display: none !important; }
    .login-wrapper { max-width: 420px; margin: 0 auto; padding: 10px; }
    </style>
    <div class="login-wrapper">
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style="text-align:center; padding:34px 0 20px 0;">
        {_logo_img(90)}
        <div style="font-size:26px;font-weight:900;color:#FFFFFF;
                    letter-spacing:0.5px;margin:14px 0 4px 0;">Spread Manager</div>
        <div style="font-size:16px;font-weight:700;color:#F5C518;margin-bottom:4px;">
            Al Barid Bank</div>
        <div style="font-size:12px;color:#FFFFFF;margin-bottom:20px;">
            Calculateur de Spread</div>
    </div>
    """, unsafe_allow_html=True)

    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Username", placeholder="spreadABB",  key="lu")
        password = st.text_input("Code", type="password", placeholder="••••••••", key="lp")
        submit   = st.form_submit_button("Se connecter", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if submit:
        if CREDENTIALS.get(username) == password:
            st.session_state["authenticated"] = True
            st.session_state["username"] = username
            st.rerun()
        else:
            st.error("Identifiants incorrects. Réessayez.")

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
def _render_sidebar() -> str:
    with st.sidebar:
        st.markdown(f"""
        <div style="text-align:center;padding:14px 0 6px 0;">{_logo_img(72)}</div>
        <div style="text-align:center;padding:6px 0 16px 0;">
            <div style="font-size:15px;font-weight:900;color:#F5C518;letter-spacing:2px;">
                Al Barid Bank</div>
            <div style="font-size:11px;color:#FFFFFF;margin-top:2px;">Spread Manager</div>
            <hr style="border-color:rgba(200,80,30,0.3);margin:12px 0 8px 0;">
        </div>
        """, unsafe_allow_html=True)

        current = st.session_state.get("_nav", "Accueil")
        for item in ["Accueil", "Calculateur de Spread"]:
            css = "nav-btn-active" if item == current else "nav-btn"
            st.markdown(f'<div class="{css}">', unsafe_allow_html=True)
            if st.button(item, key=f"nav_{item}", use_container_width=True):
                st.session_state["_nav"] = item
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<hr style='border-color:rgba(200,80,30,0.2);margin:14px 0;'>",
                    unsafe_allow_html=True)
        st.markdown(
            f"<span style='font-size:11px;color:#FFFFFF;'>"
            f"Connecté : {st.session_state.get('username','')}</span>",
            unsafe_allow_html=True)
        if st.button("🔓 Déconnexion", use_container_width=True):
            st.session_state.clear()
            st.rerun()

    return st.session_state.get("_nav", "Accueil")

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _kpi(title: str, value: str, sub: str = "") -> str:
    return (f"<div class='kpi-card'><div class='kpi-title'>{title}</div>"
            f"<div class='kpi-value'>{value}</div>"
            + (f"<div class='kpi-sub'>{sub}</div>" if sub else "")
            + "</div>")

def _sec(title: str) -> None:
    st.markdown(f"<div class='section-hdr'>{title}</div>", unsafe_allow_html=True)

def _to_decimal(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, str):
        val = val.strip().replace("%","").replace(",",".").replace(" ","")
    try:
        v = float(val)
    except (ValueError, TypeError):
        return None
    return v / 100.0 if abs(v) > 1 else v

_RATE_KW = ["COUPONRT","TAUX","RATE","RENDEMENT","INTRATE","REND","EMISSION","INTERESTRATE"]

def _detect_rate_cols(cols: list[str]) -> list[str]:
    return [c for c in cols if any(k in c.upper() for k in _RATE_KW)]

def _detect_type(row: pd.Series) -> str:
    ctgry = str(row.get("INSTRCTGRY", "")).strip().upper()
    if ctgry == "BDT":
        return "BT"
    if ctgry == "OBL_ORDN":
        return "OBLIG_ORDN"
    name = (str(row.get("ENGLONGNAME", "")) + " " + str(row.get("ENGPREFERREDNAME", ""))).upper()
    if "BSF" in name:
        return "BSF"
    if "CD" in name:
        return "CD"
    return "Autre"

def _detect_sector(issuer: str) -> str:
    return SECTEUR_MAP.get(str(issuer).strip(), "AUTRES")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE ACCUEIL
# ─────────────────────────────────────────────────────────────────────────────
def _page_home() -> None:
    st.markdown(f"""
    <div style="background:linear-gradient(90deg,#1E0A03 0%,#130602 100%);
                border:1px solid rgba(200,80,30,0.4);border-radius:14px;
                padding:22px 28px;margin-bottom:22px;
                display:flex;align-items:center;gap:20px;">
        {_logo_img(60)}
        <div>
            <div style="font-size:1.55rem;font-weight:900;color:#FFFFFF;">Spread Manager</div>
            <div style="font-size:1rem;font-weight:700;color:#F5C518;">Al Barid Bank</div>
            <div style="font-size:0.85rem;color:#FFFFFF;margin-top:2px;">
                Calcul du spread BSF, CD, BT &amp; Obligations vs courbe BDT Bank Al-Maghrib</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    _sec("📋 Guide d'utilisation")
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown("""
        <div class="instr-card">
            <h4>📁 Étape 1 — Charger le fichier Maroclear</h4>
            <ul>
                <li>Exportez votre fichier depuis <b>Maroclear</b> en format <b>.xlsx</b></li>
                <li>Glissez-déposez le fichier dans la zone d'upload</li>
                <li>L'application détecte automatiquement toutes les <b>feuilles disponibles</b></li>
                <li>Sélectionnez la feuille contenant les instruments</li>
            </ul>
        </div>
        <div class="instr-card">
            <h4>🔍 Étape 2 — Filtrer les instruments</h4>
            <ul>
                <li>Choisissez parmi <b>CD</b>, <b>BSF</b>, <b>BT</b> et <b>OBLIG_ORDN</b></li>
                <li>Les obligations bancaires sont automatiquement <b>exclues</b> du filtre OBLIG_ORDN</li>
                <li>Le <b>secteur</b> est détecté automatiquement pour les obligations</li>
                <li>Ajustez la <b>maturité résiduelle min/max en années</b></li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="instr-card">
            <h4>📈 Étape 3 — Calculer les spreads</h4>
            <ul>
                <li>Récupération automatique des <b>courbes BDT BAM</b>
                    (bkam.ma) pour chaque date d'émission</li>
                <li>Les courbes téléchargées sont <b>mises en cache</b> localement</li>
                <li>Interpolation du taux BDT à la maturité exacte (logique VBA officielle BAM)</li>
                <li><b>Spread (bps) = Taux instrument − Taux BDT</b></li>
            </ul>
        </div>
        <div class="instr-card">
            <h4>📥 Étape 4 — Exporter les résultats</h4>
            <ul>
                <li>CD / BSF / BT : export groupé par type et émetteur</li>
                <li>OBLIG_ORDN : export avec une feuille globale +
                    <b>une feuille par secteur</b> (trié par spread décroissant)</li>
                <li>KPIs : spread moyen, médian, min, max — global et par type</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    _sec("🏦 Instruments couverts")
    c1, c2, c3, c4 = st.columns(4, gap="large")
    with c1:
        st.markdown("""
        <div class="instr-card">
            <h4>CD</h4>
            <ul>
                <li>Certificats de Dépôt</li>
                <li>INSTRCTGRY = TCN</li>
                <li>Maturité : 10j à 5 ans</li>
            </ul>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown("""
        <div class="instr-card">
            <h4>BSF</h4>
            <ul>
                <li>Bons de Sociétés Financières</li>
                <li>INSTRCTGRY = TCN</li>
                <li>Maturité : 2 à 5 ans</li>
            </ul>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown("""
        <div class="instr-card">
            <h4>BT</h4>
            <ul>
                <li>Billets de Trésorerie</li>
                <li>INSTRCTGRY = BDT</li>
                <li>Toutes maturités</li>
            </ul>
        </div>""", unsafe_allow_html=True)
    with c4:
        st.markdown("""
        <div class="instr-card">
            <h4>OBLIG_ORDN</h4>
            <ul>
                <li>Obligations Ordinaires</li>
                <li>INSTRCTGRY = OBL_ORDN</li>
                <li>Hors secteur bancaire</li>
            </ul>
        </div>""", unsafe_allow_html=True)

    st.info("**Connexion internet requise** pour récupérer les courbes BDT. "
            "Les courbes déjà téléchargées sont réutilisées automatiquement (cache local).")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CALCULATEUR
# ─────────────────────────────────────────────────────────────────────────────
def _page_spread() -> None:
    st.markdown(f"""
    <div style="background:linear-gradient(90deg,#1E0A03 0%,#130602 100%);
                border:1px solid rgba(200,80,30,0.4);border-radius:14px;
                padding:18px 26px;margin-bottom:20px;
                display:flex;align-items:center;gap:18px;">
        {_logo_img(52)}
        <div>
            <div style="font-size:1.35rem;font-weight:900;color:#FFFFFF;">
                Calculateur de Spread</div>
            <div style="font-size:0.85rem;color:#FFFFFF;">
                BSF, CD, BT &amp; OBLIG_ORDN — Courbe BDT Bank Al-Maghrib</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── ÉTAPE 1 : Upload ──────────────────────────────────────────────────────
    _sec("📁 Étape 1 — Charger le fichier")
    uploaded = st.file_uploader(
        "Fichier Maroclear (.xlsx)",
        type=["xlsx"],
        help="Fichier exporté depuis Maroclear contenant les instruments",
    )
    if uploaded is None:
        st.info("Chargez un fichier Excel pour commencer.")
        return

    # ── ÉTAPE 2 : Détection auto des feuilles ─────────────────────────────────
    _sec("📄 Étape 2 — Sélectionner la feuille")
    try:
        xl = pd.ExcelFile(uploaded, engine="openpyxl")
        sheet_names = xl.sheet_names
    except Exception as e:
        st.error(f"Impossible de lire le fichier : {e}")
        return

    sheet = st.selectbox(
        "Feuille Excel",
        options=sheet_names,
        help="Toutes les feuilles disponibles dans votre fichier sont listées ici.",
    )

    try:
        uploaded.seek(0)
        df = pd.read_excel(uploaded, sheet_name=sheet, engine="openpyxl")
    except Exception as e:
        st.error(f"Erreur de lecture de la feuille '{sheet}' : {e}")
        return

    st.success(f"✅ Feuille **{sheet}** chargée — {len(df)} lignes, {len(df.columns)} colonnes.")

    # ── Vérification colonnes requises ─────────────────────────────────────────
    REQUIRED = ["ISSUEDT", "MATURITYDT_L", "INSTRCTGRY", "ENGLONGNAME", "ENGPREFERREDNAME"]
    missing  = [c for c in REQUIRED if c not in df.columns]
    if missing:
        st.error(f"Colonnes manquantes : **{missing}**\n\n"
                 f"Colonnes disponibles : {list(df.columns)}")
        return

    # ── ÉTAPE 3 : Paramètres de filtrage ──────────────────────────────────────
    _sec("⚙️ Étape 3 — Paramètres de filtrage")

    _issuedt_series = pd.to_datetime(df["ISSUEDT"], errors="coerce").dropna()
    _date_min_file  = _issuedt_series.min().date() if not _issuedt_series.empty else date(2020, 1, 1)
    _date_max_file  = _issuedt_series.max().date() if not _issuedt_series.empty else date.today()

    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        inc_cd    = st.checkbox("CD — Certificats de Dépôt",         value=True)
    with fc2:
        inc_bsf   = st.checkbox("BSF — Bons de Soc. Financières",    value=True)
    with fc3:
        inc_bt    = st.checkbox("BT — Billets de Trésorerie",         value=False)
    with fc4:
        inc_oblig = st.checkbox("OBLIG_ORDN — Obligations (hors banques)", value=False)

    mc1, mc2 = st.columns(2)
    with mc1:
        res_min_y = st.number_input("Maturité min (ans)", value=0.0, min_value=0.0,
                                    max_value=30.0, step=0.5, format="%.1f")
    with mc2:
        res_max_y = st.number_input("Maturité max (ans)", value=5.0, min_value=0.0,
                                    max_value=30.0, step=0.5, format="%.1f")

    st.markdown(
        "<span style='font-size:0.85rem;color:#FFFFFF;font-weight:600;'>"
        "📅 Filtrer par date d'émission (ISSUEDT)</span>",
        unsafe_allow_html=True,
    )
    _bound_min = min(_date_min_file, date(1990, 1, 1))
    _bound_max = max(_date_max_file, date.today())

    dc1, dc2 = st.columns(2)
    with dc1:
        date_iss_min = st.date_input(
            "Date d'émission — Début",
            value=_date_min_file,
            min_value=_bound_min,
            max_value=_bound_max,
            key="iss_min",
        )
    with dc2:
        date_iss_max = st.date_input(
            "Date d'émission — Fin",
            value=_date_max_file,
            min_value=_bound_min,
            max_value=_bound_max,
            key="iss_max",
        )

    res_min = int(res_min_y * 365)
    res_max = int(res_max_y * 365) if res_max_y > 0 else 36500

    if not any([inc_cd, inc_bsf, inc_bt, inc_oblig]):
        st.warning("Sélectionnez au moins un type d'instrument.")
        return

    # ── Filtrage ───────────────────────────────────────────────────────────────
    dff = df.copy()
    dff["_idt"] = pd.to_datetime(dff["ISSUEDT"],      errors="coerce").dt.date
    dff["_mdt"] = pd.to_datetime(dff["MATURITYDT_L"], errors="coerce").dt.date

    instrctgry  = dff["INSTRCTGRY"].fillna("").astype(str).str.strip().str.upper()
    name_mix    = (dff["ENGLONGNAME"].fillna("").astype(str)
                   + " " + dff["ENGPREFERREDNAME"].fillna("").astype(str)).str.upper()

    mask_issuedt = dff["_idt"].between(date_iss_min, date_iss_max)
    residual     = (pd.to_datetime(dff["MATURITYDT_L"], errors="coerce")
                    - pd.to_datetime(dff["ISSUEDT"],    errors="coerce")).dt.days
    mask_resid   = residual.between(res_min, res_max)

    mask_combined = pd.Series(False, index=dff.index)

    if inc_cd or inc_bsf:
        mask_tcn = instrctgry.eq("TCN")
        if inc_cd:
            mask_combined |= mask_tcn & name_mix.str.contains("CD", regex=False)
        if inc_bsf:
            mask_combined |= mask_tcn & name_mix.str.contains("BSF", regex=False)

    if inc_bt:
        mask_combined |= instrctgry.eq("BDT")

    if inc_oblig:
        mask_oblig = instrctgry.eq("OBL_ORDN")
        if "PREFERREDNAMEISSUER" in dff.columns:
            mask_oblig &= ~dff["PREFERREDNAMEISSUER"].fillna("").astype(str).isin(BANK_ISSUERS)
        mask_combined |= mask_oblig

    mask_combined &= mask_issuedt & mask_resid
    selected_idx = df.index[mask_combined].tolist()

    # ── KPIs avant calcul ─────────────────────────────────────────────────────
    type_counts: dict[str, int] = {}
    if inc_cd:
        type_counts["CD"] = int((mask_combined & instrctgry.eq("TCN") & name_mix.str.contains("CD", regex=False)).sum())
    if inc_bsf:
        type_counts["BSF"] = int((mask_combined & instrctgry.eq("TCN") & name_mix.str.contains("BSF", regex=False)).sum())
    if inc_bt:
        type_counts["BT"] = int((mask_combined & instrctgry.eq("BDT")).sum())
    if inc_oblig:
        _m_oblig = mask_combined & instrctgry.eq("OBL_ORDN")
        type_counts["OBLIG"] = int(_m_oblig.sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(_kpi("Lignes totales",      str(len(df))),            unsafe_allow_html=True)
    k2.markdown(_kpi("Instruments retenus", str(len(selected_idx))),  unsafe_allow_html=True)
    tstr = " / ".join(f"{k}:{v}" for k, v in type_counts.items())
    k3.markdown(_kpi("Répartition", tstr),                            unsafe_allow_html=True)
    k4.markdown(_kpi("Période émission",
                     f"{date_iss_min.strftime('%d/%m/%Y')}",
                     f"→ {date_iss_max.strftime('%d/%m/%Y')}"),       unsafe_allow_html=True)

    if not selected_idx:
        st.warning("Aucun instrument trouvé avec ces filtres.")
        return

    # ── Colonne taux instrument ────────────────────────────────────────────────
    rate_candidates = _detect_rate_cols(list(df.columns))
    rate_col: Optional[str] = None
    if rate_candidates:
        choice   = st.selectbox(
            "Colonne taux instrument (pour calcul spread)",
            ["— aucune —"] + rate_candidates,
            index=1,
        )
        rate_col = None if choice == "— aucune —" else choice
    else:
        st.info("Aucune colonne taux détectée — seul le Taux BDT sera calculé.")

    # ── Bouton calcul ──────────────────────────────────────────────────────────
    if not st.button("⚡ Calculer les spreads", use_container_width=True):
        return

    # ── Préparer le df filtré ──────────────────────────────────────────────────
    df_work = df.loc[selected_idx].copy()

    _dup_col = next(
        (c for c in df_work.columns
         if c.upper() in {"INSTRID", "ISIN", "ISINCODE", "CODEISIN", "INSTRISINOCODE"}),
        None,
    )
    if _dup_col:
        _n_before = len(df_work)
        df_work = df_work.drop_duplicates(subset=[_dup_col], keep="first")
        _n_after = len(df_work)
        if _n_before > _n_after:
            st.caption(f"ℹ️ {_n_before - _n_after} doublon(s) supprimé(s) sur la colonne **{_dup_col}**.")

    df_work["ISSUEDT"]      = pd.to_datetime(df_work["ISSUEDT"],      errors="coerce")
    df_work["MATURITYDT_L"] = pd.to_datetime(df_work["MATURITYDT_L"], errors="coerce")

    df_work["Maturité (ans)"] = (
        (df_work["MATURITYDT_L"] - df_work["ISSUEDT"]).dt.days / 365.25
    ).round(2)

    unique_dates = sorted({d.date() for d in df_work["ISSUEDT"].dropna()})

    # ── Récupération courbes BAM ───────────────────────────────────────────────
    _sec("🌐 Récupération des courbes BDT BAM")
    pbar    = st.progress(0.0)
    stxt    = st.empty()
    fetcher = BamCurveFetcher(cache_dir=str(CACHE_DIR))

    total_dates = len(unique_dates)

    def _progress(done: int, total: int, n_cache: int, n_net: int, eta: float) -> None:
        pbar.progress(done / max(total, 1))
        eta_str = f" — ETA : {eta:.0f}s" if eta > 1 else ""
        stxt.markdown(
            f"<span style='color:#FFFFFF;font-size:0.85rem;'>"
            f"Courbes BAM : <b>{done}/{total}</b>"
            f" &nbsp;(cache : {n_cache} | réseau : {n_net}){eta_str}</span>",
            unsafe_allow_html=True,
        )

    curves = fetcher.get_curves_parallel(
        unique_dates,
        max_workers=10,
        progress_callback=_progress,
    )

    pbar.empty(); stxt.empty()
    ok = sum(1 for v in curves.values() if v is not None)
    st.success(f"✅ {ok}/{total_dates} courbes BDT récupérées.")

    # ── Calcul taux BDT + spread ───────────────────────────────────────────────
    bdt_rates:   list[Optional[float]] = []
    instr_rates: list[Optional[float]] = []
    spreads_bps: list[Optional[float]] = []

    for _, row in df_work.iterrows():
        idt = row["ISSUEDT"]
        mdt = row["MATURITYDT_L"]

        if pd.isna(idt) or pd.isna(mdt):
            bdt_rates.append(None); instr_rates.append(None); spreads_bps.append(None)
            continue

        mat_days = int((mdt - idt).days)
        if mat_days <= 0:
            bdt_rates.append(None); instr_rates.append(None); spreads_bps.append(None)
            continue

        curve = curves.get(idt.date())
        if not curve:
            bdt_rates.append(None); instr_rates.append(None); spreads_bps.append(None)
            continue

        mt, tx = curve
        try:
            bdt = calcul_taux(mat_days, mt, tx, idt.date())
            bdt_rates.append(bdt)
        except Exception:
            bdt_rates.append(None); instr_rates.append(None); spreads_bps.append(None)
            continue

        ir     = _to_decimal(row.get(rate_col)) if rate_col else None
        spread = (ir - bdt) * 10_000 if (ir is not None and bdt is not None) else None
        instr_rates.append(ir)
        spreads_bps.append(spread)

    # ── Attacher les colonnes calculées ───────────────────────────────────────
    df_work["Taux BDT"] = bdt_rates
    if rate_col:
        df_work["Taux instrument"] = instr_rates
        df_work["Spread (bps)"]   = spreads_bps

    df_work["Type"] = df_work.apply(_detect_type, axis=1)

    if inc_oblig and "PREFERREDNAMEISSUER" in df_work.columns:
        df_work["SECTEUR"] = df_work.apply(
            lambda r: _detect_sector(r.get("PREFERREDNAMEISSUER", ""))
            if r["Type"] == "OBLIG_ORDN" else "",
            axis=1,
        )
    elif inc_oblig:
        df_work["SECTEUR"] = df_work["Type"].apply(
            lambda t: "AUTRES" if t == "OBLIG_ORDN" else ""
        )

    # ── KPIs spread ───────────────────────────────────────────────────────────
    if rate_col:
        valid = [s for s in spreads_bps if s is not None]
        if valid:
            _sec("📈 KPIs — Spread")
            s1, s2, s3, s4 = st.columns(4)
            s1.markdown(_kpi("Spread moyen",  f"{sum(valid)/len(valid):.1f}", "bps"), unsafe_allow_html=True)
            s2.markdown(_kpi("Spread médian", f"{median(valid):.1f}",         "bps"), unsafe_allow_html=True)
            s3.markdown(_kpi("Spread max",    f"{max(valid):.1f}",            "bps"), unsafe_allow_html=True)
            s4.markdown(_kpi("Spread min",    f"{min(valid):.1f}",            "bps"), unsafe_allow_html=True)

            for t in df_work["Type"].unique():
                sub = df_work[df_work["Type"] == t]["Spread (bps)"].dropna().tolist()
                if sub:
                    st.markdown(
                        f"<span style='color:#FFFFFF;font-size:0.85rem;'>"
                        f"<b style='color:#FF9060;'>{t}</b> — "
                        f"Spread moyen : <b>{sum(sub)/len(sub):.1f} bps</b> | "
                        f"Médian : <b>{median(sub):.1f} bps</b> | "
                        f"n = {len(sub)}</span>",
                        unsafe_allow_html=True,
                    )

    # ── Tableau ───────────────────────────────────────────────────────────────
    _sec("📋 Tableau des résultats")
    base_cols  = ["ISSUEDT", "MATURITYDT_L", "Maturité (ans)", "ENGLONGNAME", "Type", "INSTRCTGRY"]
    if inc_oblig and "SECTEUR" in df_work.columns:
        base_cols.append("SECTEUR")
    if inc_oblig and "PREFERREDNAMEISSUER" in df_work.columns:
        base_cols.insert(4, "PREFERREDNAMEISSUER")
    extra_cols = (["Taux instrument", "Taux BDT", "Spread (bps)"] if rate_col else ["Taux BDT"])
    disp = [c for c in base_cols + extra_cols if c in df_work.columns]
    seen: set[str] = set()
    disp = [c for c in disp if not (c in seen or seen.add(c))]  # type: ignore[func-returns-value]

    fmt: dict = {}
    if "Maturité (ans)"   in disp: fmt["Maturité (ans)"]   = "{:.2f}"
    if "Taux BDT"         in disp: fmt["Taux BDT"]         = "{:.4%}"
    if "Taux instrument"  in disp: fmt["Taux instrument"]  = "{:.4%}"
    if "Spread (bps)"     in disp: fmt["Spread (bps)"]     = "{:.1f}"

    st.dataframe(
        df_work[disp].reset_index(drop=True).style.format(fmt, na_rep="-"),
        use_container_width=True,
        height=420,
    )

    # ═════════════════════════════════════════════════════════════════════════
    # EXPORT EXCEL
    # ═════════════════════════════════════════════════════════════════════════
    import re as _re
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

    _YELLOW = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    _GRAY   = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
    _BLUE   = PatternFill(start_color="DCE6F1", end_color="DCE6F1", fill_type="solid")
    _NAVY   = PatternFill(start_color="1F3864", end_color="1F3864", fill_type="solid")
    _BOLD   = Font(bold=True)
    _WHITE_BOLD = Font(bold=True, color="FFFFFF")
    _THIN   = Side(border_style="thin", color="000000")
    _BORDER = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)
    _CENTER = Alignment(horizontal="center", vertical="center")
    _HIGHLIGHT_HDRS = {"TAUX BDT", "Spread", "TAUX D'INTERET"}

    # ── Export A : CD / BSF / BT ──────────────────────────────────────────────
    df_tcn_bt = df_work[df_work["Type"].isin(["CD", "BSF", "BT", "Autre"])].copy()

    if not df_tcn_bt.empty:
        _sec("📥 Export Excel — CD / BSF / BT")

        _BANK_ALIASES = {"SGMB": "SAHAM"}

        def _bank_tag(name: str) -> str:
            parts = str(name).strip().split()
            raw = parts[1].upper() if len(parts) >= 2 else "AUTRE"
            return _BANK_ALIASES.get(raw, raw)

        def _mat_label_from_name(details) -> str:
            s = "" if details is None else str(details).strip().lower()
            if not s:
                return "inconnue"
            s = _re.sub(r"\bsem\.\b", "sem", s)
            matches = _re.findall(
                r"(\d+)\s*(semaines?|semaine|sem|s|mois|ans?|an|jours?|jrs?|jr|j)\b", s
            )
            if not matches:
                return "inconnue"
            num, unit = matches[-1]
            unit = unit.lower()
            if unit in {"s", "sem"} or unit.startswith("semaine"):
                unit_norm = "semaine" if num == "1" else "semaines"
            elif unit in {"j", "jr", "jrs"} or unit.startswith("jour"):
                unit_norm = "jour" if num == "1" else "jours"
            elif unit.startswith("mois"):
                unit_norm = "mois"
            else:
                unit_norm = "an" if num == "1" else "ans"
            return f"{num} {unit_norm}"

        def _mat_sort_key(label: str):
            m = _re.match(r"^\s*(\d+)\s*(jour|jours|semaine|semaines|mois|an|ans)\s*$",
                          str(label).lower())
            if not m:
                return (99, 10**9)
            n = int(m.group(1)); u = m.group(2)
            if u.startswith("jour"):     return (0, n)
            if u.startswith("semaine"):  return (1, n)
            if u.startswith("mois"):     return (2, n)
            return (3, n)

        df_xls = df_tcn_bt.copy()
        df_xls["_bank"] = df_xls["ENGLONGNAME"].fillna("").apply(_bank_tag)

        has_spread = "Spread (bps)" in df_xls.columns
        if has_spread:
            df_xls["Spread"] = df_xls["Spread (bps)"]
            df_xls_filt = df_xls[df_xls["Spread (bps)"].between(10, 70)].copy()
        else:
            df_xls_filt = df_xls.copy()

        if "Taux instrument" in df_xls_filt.columns:
            df_xls_filt["_taux_instr_pct"] = df_xls_filt["Taux instrument"].apply(
                lambda v: round(float(v) * 100, 4) if pd.notna(v) and v is not None else None
            )

        n_export = len(df_xls_filt)
        n_total  = len(df_xls)
        if has_spread:
            st.info(f"Export CD/BSF/BT : **{n_export}** instruments avec spread entre **10 et 70 bps** "
                    f"(sur {n_total} calculés).")

        _ISIN_EXACT  = {"ISINCODE","ISIN","CODEISIN","ISIN_CODE","INSTRISINOCODE"}
        _CODE_APPROX = {"INSTRCODE","INSTRUMENTCODE","INSTRNO","NEMOCODE","NEMO",
                        "SECURITYNO","CODE","INSTRID","INSTRUMENTID","INSTRIDENTIFIER"}
        _code_col = (
            next((c for c in df_tcn_bt.columns if c.upper() in _ISIN_EXACT),  None)
            or next((c for c in df_tcn_bt.columns if c.upper() in _CODE_APPROX), None)
            or next(
                (c for c in df_tcn_bt.columns
                 if df_tcn_bt[c].dropna().astype(str).str.match(r"^MA\d{10}$").any()),
                None,
            )
        )

        _exp_cols = (
            ([_code_col] if _code_col else [])
            + ["ENGLONGNAME", "ISSUEDT", "MATURITYDT_L", "Maturité (ans)", "Taux BDT"]
            + (["Spread"]             if "Spread"             in df_xls_filt.columns else [])
            + (["_taux_instr_pct"]    if "_taux_instr_pct"    in df_xls_filt.columns else [])
        )
        _exp_cols = [c for c in _exp_cols if c in df_xls_filt.columns]

        _col_labels: dict = {
            "ENGLONGNAME":    "DETAILS DU TITRE",
            "ISSUEDT":        "DATE D'EMISSION",
            "MATURITYDT_L":   "DATE D'ECHEANCE",
            "Maturité (ans)": "Maturite residuelle",
            "Taux BDT":       "TAUX BDT",
            "Spread":         "Spread",
            "_taux_instr_pct":"TAUX D'INTERET",
        }
        if _code_col:
            _col_labels[_code_col] = "CODE"

        def _style_ws(ws, n_data: int) -> None:
            max_col = ws.max_column
            for c in range(1, max_col + 1):
                cell = ws.cell(1, c)
                cell.fill = _YELLOW; cell.font = _BOLD
                cell.alignment = _CENTER; cell.border = _BORDER
            hl_cols = {c for c in range(1, max_col + 1)
                       if str(ws.cell(1, c).value or "") in _HIGHLIGHT_HDRS}
            for r in range(2, n_data + 2):
                for c in range(1, max_col + 1):
                    cell = ws.cell(r, c)
                    cell.fill   = _YELLOW if c in hl_cols else _GRAY
                    cell.border = _BORDER
            for col_letter, w in zip("ABCDEFGH", [18, 38, 14, 14, 14, 12, 10, 14]):
                ws.column_dimensions[col_letter].width = w

        def _apply_number_formats(ws, n_data: int) -> None:
            hdr = {str(ws.cell(1, c).value): c for c in range(1, ws.max_column + 1)}
            for r in range(2, n_data + 2):
                if "DATE D'EMISSION" in hdr:
                    ws.cell(r, hdr["DATE D'EMISSION"]).number_format  = "dd/mm/yyyy"
                if "DATE D'ECHEANCE" in hdr:
                    ws.cell(r, hdr["DATE D'ECHEANCE"]).number_format  = "dd/mm/yyyy"
                if "Maturite residuelle" in hdr:
                    ws.cell(r, hdr["Maturite residuelle"]).number_format = "0.00"
                if "TAUX BDT" in hdr:
                    ws.cell(r, hdr["TAUX BDT"]).number_format          = "0.00%"
                if "Spread" in hdr:
                    ws.cell(r, hdr["Spread"]).number_format             = "0"
                if "TAUX D'INTERET" in hdr:
                    ws.cell(r, hdr["TAUX D'INTERET"]).number_format     = "0.00"

        def _add_summary(ws, df_s: pd.DataFrame, n_data: int) -> None:
            start_r = n_data + 3
            for ci, hdr in enumerate(["MATURITE", "MOYENNE SPREAD", "SPREAD MAX", "SPREAD MIN"], 1):
                c = ws.cell(start_r, ci, hdr)
                c.fill = _BLUE; c.font = _BOLD
                c.alignment = _CENTER; c.border = _BORDER

            buckets: dict[str, list[float]] = {}
            for _, row in df_s.iterrows():
                lbl = _mat_label_from_name(row.get("DETAILS DU TITRE", ""))
                if lbl == "inconnue":
                    continue
                try:
                    sv = float(row.get("Spread", None))
                    if sv >= 0:
                        buckets.setdefault(lbl, []).append(sv)
                except (TypeError, ValueError):
                    pass

            rr = start_r + 1
            for lbl in sorted(buckets, key=_mat_sort_key):
                vals = buckets[lbl]
                avg = sum(vals) / len(vals)
                for ci, v in enumerate(
                    [lbl, f"{avg:.0f} bps", f"{max(vals):.0f} bps", f"{min(vals):.0f} bps"], 1
                ):
                    cell = ws.cell(rr, ci, v)
                    cell.fill = _GRAY; cell.border = _BORDER
                    cell.alignment = _CENTER
                    if ci == 1: cell.font = _BOLD
                rr += 1

        def _write_sheet_tcn(writer, df_s: pd.DataFrame, sheet_name: str) -> None:
            sn     = sheet_name[:31]
            df_out = df_s[_exp_cols].rename(columns=_col_labels).reset_index(drop=True)
            df_out.to_excel(writer, sheet_name=sn, index=False)
            ws     = writer.sheets[sn]
            n_data = len(df_out)
            _style_ws(ws, n_data)
            _apply_number_formats(ws, n_data)
            _add_summary(ws, df_out, n_data)

        output_tcn = io.BytesIO()
        with pd.ExcelWriter(output_tcn, engine="openpyxl") as writer:
            if not df_xls_filt.empty:
                _write_sheet_tcn(writer, df_xls_filt, "TOUT")
            df_xls_filt = df_xls_filt.copy()
            df_xls_filt["_type_nn"] = df_xls_filt["Type"].fillna("AUTRE")
            df_xls_filt["_bank_nn"] = df_xls_filt["_bank"].fillna("AUTRE")
            for (typ, bank), df_grp in df_xls_filt.groupby(["_type_nn", "_bank_nn"]):
                if not df_grp.empty:
                    _write_sheet_tcn(writer, df_grp, f"{typ}_{bank}")

        output_tcn.seek(0)
        st.download_button(
            label="⬇️  Télécharger CD / BSF / BT (Excel)",
            data=output_tcn.getvalue(),
            file_name=f"spread_cd_bsf_bt_{date.today().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    # ── Export B : OBLIG_ORDN ─────────────────────────────────────────────────
    df_oblig = df_work[df_work["Type"] == "OBLIG_ORDN"].copy()

    if not df_oblig.empty:
        _sec("📥 Export Excel — Obligations (OBLIG_ORDN)")

        _ISIN_EXACT_O  = {"ISINCODE","ISIN","CODEISIN","ISIN_CODE","INSTRISINOCODE"}
        _CODE_APPROX_O = {"INSTRCODE","INSTRUMENTCODE","INSTRNO","NEMOCODE","NEMO",
                          "SECURITYNO","INSTRID","INSTRUMENTID","INSTRIDENTIFIER"}
        _instrid_col = (
            next((c for c in df_oblig.columns if c.upper() in _ISIN_EXACT_O), None)
            or next((c for c in df_oblig.columns if c.upper() in _CODE_APPROX_O), None)
        )

        _oblig_src_cols: list[str] = []
        if _instrid_col:
            _oblig_src_cols.append(_instrid_col)
        _oblig_src_cols.append("ENGPREFERREDNAME")
        if "PREFERREDNAMEISSUER" in df_oblig.columns:
            _oblig_src_cols.append("PREFERREDNAMEISSUER")
        if "SECTEUR" in df_oblig.columns:
            _oblig_src_cols.append("SECTEUR")
        _oblig_src_cols += ["ISSUEDT", "MATURITYDT_L"]
        if rate_col and rate_col in df_oblig.columns:
            _oblig_src_cols.append(rate_col)
        _oblig_src_cols += ["Maturité (ans)", "Taux BDT"]
        if "Spread (bps)" in df_oblig.columns:
            _oblig_src_cols.append("Spread (bps)")
        _oblig_src_cols = [c for c in _oblig_src_cols if c in df_oblig.columns]

        _oblig_rename: dict = {
            "ENGPREFERREDNAME":   "NOM_INSTRUMENT",
            "PREFERREDNAMEISSUER":"EMETTEUR",
            "SECTEUR":            "SECTEUR",
            "ISSUEDT":            "DATE_EMISSION",
            "MATURITYDT_L":       "DATE_ECHEANCE",
            "Maturité (ans)":     "MATURITE_ANS",
            "Taux BDT":           "TAUX_BDT_INTERP",
            "Spread (bps)":       "SPREAD_BPS",
        }
        if _instrid_col:
            _oblig_rename[_instrid_col] = "INSTRID"
        if rate_col:
            _oblig_rename[rate_col] = "INTERESTRATE"

        def _style_ws_oblig(ws) -> None:
            max_col = ws.max_column
            max_row = ws.max_row
            for c in range(1, max_col + 1):
                cell = ws.cell(1, c)
                cell.fill      = _NAVY
                cell.font      = _WHITE_BOLD
                cell.alignment = _CENTER
                cell.border    = _BORDER
            for r in range(2, max_row + 1):
                for c in range(1, max_col + 1):
                    ws.cell(r, c).border = _BORDER
            for col_cells in ws.columns:
                col_letter = col_cells[0].column_letter
                max_len = max(
                    (len(str(cell.value)) if cell.value is not None else 0)
                    for cell in col_cells
                )
                ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 50)

        def _apply_number_formats_oblig(ws) -> None:
            hdr = {str(ws.cell(1, c).value): c for c in range(1, ws.max_column + 1)}
            max_row = ws.max_row
            for r in range(2, max_row + 1):
                if "DATE_EMISSION" in hdr:
                    ws.cell(r, hdr["DATE_EMISSION"]).number_format  = "dd/mm/yyyy"
                if "DATE_ECHEANCE" in hdr:
                    ws.cell(r, hdr["DATE_ECHEANCE"]).number_format  = "dd/mm/yyyy"
                if "MATURITE_ANS" in hdr:
                    ws.cell(r, hdr["MATURITE_ANS"]).number_format   = "0.00"
                if "TAUX_BDT_INTERP" in hdr:
                    ws.cell(r, hdr["TAUX_BDT_INTERP"]).number_format = "0.00%"
                if "SPREAD_BPS" in hdr:
                    ws.cell(r, hdr["SPREAD_BPS"]).number_format      = "0.0"
                if "INTERESTRATE" in hdr:
                    ws.cell(r, hdr["INTERESTRATE"]).number_format    = "0.00%"

        def _sheet_name_oblig(secteur: str) -> str:
            return secteur.replace("/", "-")[:31]

        def _write_oblig_sheet(writer, df_s: pd.DataFrame, sheet_name: str) -> None:
            sn = _sheet_name_oblig(sheet_name)
            df_out = df_s[_oblig_src_cols].rename(columns=_oblig_rename).reset_index(drop=True)
            if "SPREAD_BPS" in df_out.columns:
                df_out = df_out.sort_values("SPREAD_BPS", ascending=False)
            df_out.to_excel(writer, sheet_name=sn, index=False)
            ws = writer.sheets[sn]
            _style_ws_oblig(ws)
            _apply_number_formats_oblig(ws)

        st.info(f"Export OBLIG_ORDN : **{len(df_oblig)}** obligations (hors banques).")

        output_oblig = io.BytesIO()
        with pd.ExcelWriter(output_oblig, engine="openpyxl") as writer:
            _write_oblig_sheet(writer, df_oblig, "TOUTES_OBLIG")
            secteur_col = "SECTEUR" if "SECTEUR" in df_oblig.columns else None
            if secteur_col:
                for secteur, df_grp in df_oblig.groupby(secteur_col):
                    if not df_grp.empty:
                        _write_oblig_sheet(writer, df_grp, str(secteur))

        output_oblig.seek(0)
        st.download_button(
            label="⬇️  Télécharger Obligations OBLIG_ORDN (Excel)",
            data=output_oblig.getvalue(),
            file_name=f"spread_oblig_{date.today().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="auto",
    )
    st.markdown(STYLE, unsafe_allow_html=True)

    if not st.session_state.get("authenticated"):
        _show_login()
        return

    page = _render_sidebar()

    if page == "Accueil":
        _page_home()
    else:
        _page_spread()


if __name__ == "__main__":
    main()
