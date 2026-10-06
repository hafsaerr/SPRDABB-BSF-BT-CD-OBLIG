from __future__ import annotations

"""
Cache persistant Supabase pour les courbes BDT BAM.
Toutes les opérations sont enveloppées dans des try/except
pour ne pas bloquer l'app si Supabase est injoignable.
"""

from datetime import date
from typing import Optional

_client_cache = None   # singleton pour éviter de recréer le client à chaque appel


def _get_client():
    """Retourne le client supabase-py ou None si non disponible."""
    global _client_cache
    if _client_cache is not None:
        return _client_cache
    try:
        import streamlit as st
        from supabase import create_client
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
        _client_cache = create_client(url, key)
        print("[BAM-DIAG] Supabase client created OK", flush=True)
        return _client_cache
    except Exception as e:
        print(f"[BAM-DIAG] Supabase client unavailable: {type(e).__name__}: {e}", flush=True)
        return None


def get_curve(d: date) -> Optional[tuple[list[int], list[float]]]:
    """
    Cherche la courbe du jour `d` dans Supabase.
    Retourne (mt: list[int], tx: list[float]) ou None si absente.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("bdt_curves")
            .select("courbe_json")
            .eq("date_emission", d.isoformat())
            .execute()
        )
        if resp.data:
            courbe: dict = resp.data[0]["courbe_json"]
            items = sorted((int(k), float(v)) for k, v in courbe.items())
            if len(items) < 2:
                return None
            mt = [x[0] for x in items]
            tx = [x[1] for x in items]
            return mt, tx
    except Exception:
        pass
    return None


def save_curve(d: date, mt: list[int], tx: list[float]) -> None:
    """
    Upsert la courbe dans Supabase.
    Courbe stockée en JSON : {str(days): taux_decimal}
    """
    client = _get_client()
    if client is None:
        return
    try:
        courbe = {str(m): t for m, t in zip(mt, tx)}
        client.table("bdt_curves").upsert(
            {"date_emission": d.isoformat(), "courbe_json": courbe}
        ).execute()
    except Exception:
        pass


def get_all_cached_dates() -> set[date]:
    """
    Retourne l'ensemble de toutes les dates déjà en base.
    Utilisé pour éviter de re-scraper ce qui existe.
    """
    client = _get_client()
    if client is None:
        return set()
    try:
        resp = client.table("bdt_curves").select("date_emission").execute()
        result = set()
        for row in resp.data:
            try:
                result.add(date.fromisoformat(row["date_emission"]))
            except Exception:
                pass
        return result
    except Exception:
        return set()


def is_available() -> bool:
    """Vérifie que Supabase est joignable."""
    return _get_client() is not None
