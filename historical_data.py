from __future__ import annotations

"""
historical_data.py — Lecture dynamique de la data historique de spreads.

Les fichiers Excel générés par l'export CD/BSF/BT de l'app (voir app.py,
section "Export A") sont déposés dans historique_spreads/. Chaque onglet
détail (un par émetteur) a un nom du type "CD_ATW", "BSF_CONSO_EQDOM",
"BSF_BAIL_MAGHREBAIL" ou "BT_LABEL VIE". Les onglets RECAP_* et tout autre
onglet (ex: OBLIG) sont ignorés.

Aucun nom d'émetteur n'est codé en dur : ajouter un onglet (nouvel émetteur)
ou un fichier (nouvelle date d'export) dans historique_spreads/ est pris en
compte automatiquement au prochain chargement de page.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

HIST_DATA_DIR = Path(__file__).parent / "historique_spreads"

# Préfixes les plus spécifiques d'abord : "BSF_CONSO_"/"BSF_BAIL_" doivent être
# testés avant un éventuel préfixe générique "BSF_".
_ASSET_PREFIXES: list[tuple[str, str]] = [
    ("BSF_CONSO_", "BSF_CONSO"),
    ("BSF_BAIL_",  "BSF_BAIL"),
    ("CD_",        "CD"),
    ("BT_",        "BT"),
]

_EXPECTED_COLS = [
    "CODE", "DETAILS DU TITRE", "DATE D'EMISSION", "DATE D'ECHEANCE",
    "Maturite residuelle", "TAUX BDT", "Spread", "TAUX D'INTERET",
]

_EMPTY_COLS = _EXPECTED_COLS + ["Type", "Emetteur"]


def _match_asset_type(sheet_name: str) -> Optional[tuple[str, str]]:
    """Retourne (type_actif, nom_emetteur) si le nom d'onglet correspond à un
    préfixe connu, sinon None (onglet ignoré : RECAP_*, OBLIG, TOUT, ...)."""
    upper = sheet_name.upper()
    if upper.startswith("RECAP"):
        return None
    for prefix, asset_type in _ASSET_PREFIXES:
        if upper.startswith(prefix):
            emetteur = sheet_name[len(prefix):].strip()
            if emetteur:
                return asset_type, emetteur
    return None


@st.cache_data(show_spinner="Chargement de l'historique des spreads…")
def _load_historical_spreads(dir_mtime: float) -> pd.DataFrame:
    """
    Scanne historique_spreads/*.xlsx, lit tous les onglets détail
    (CD_*, BSF_CONSO_*, BSF_BAIL_*, BT_*) et les concatène en un seul
    DataFrame, avec les colonnes Type / Emetteur en plus.

    `dir_mtime` ne sert qu'à invalider le cache Streamlit quand un fichier du
    dossier est ajouté/modifié — voir get_historical_spreads().
    """
    frames: list[pd.DataFrame] = []

    if not HIST_DATA_DIR.exists():
        return pd.DataFrame(columns=_EMPTY_COLS)

    for xlsx_path in sorted(HIST_DATA_DIR.glob("*.xlsx")):
        try:
            xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
        except Exception:
            continue

        for sheet_name in xl.sheet_names:
            match = _match_asset_type(sheet_name)
            if match is None:
                continue
            asset_type, emetteur = match

            try:
                df = xl.parse(sheet_name)
            except Exception:
                continue
            if df.empty:
                continue

            # Colonnes attendues uniquement ; DATE D'EMISSION/D'ECHEANCE sont
            # indispensables (bucketing de maturité), le reste est optionnel.
            keep_cols = [c for c in _EXPECTED_COLS if c in df.columns]
            if "DATE D'EMISSION" not in keep_cols or "DATE D'ECHEANCE" not in keep_cols:
                continue

            df = df[keep_cols].copy()
            df["Type"] = asset_type
            df["Emetteur"] = emetteur
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=_EMPTY_COLS)

    out = pd.concat(frames, ignore_index=True, sort=False)

    out["DATE D'EMISSION"] = pd.to_datetime(out["DATE D'EMISSION"], errors="coerce")
    out["DATE D'ECHEANCE"] = pd.to_datetime(out["DATE D'ECHEANCE"], errors="coerce")
    for num_col in ("Maturite residuelle", "TAUX BDT", "Spread", "TAUX D'INTERET"):
        if num_col in out.columns:
            out[num_col] = pd.to_numeric(out[num_col], errors="coerce")

    # Lignes sans date exploitable = inutilisables pour le bucketing de
    # maturité -> écartées proprement plutôt que de faire planter les pages.
    out = out.dropna(subset=["DATE D'EMISSION", "DATE D'ECHEANCE"])

    return out.reset_index(drop=True)


def get_historical_spreads() -> pd.DataFrame:
    """Point d'entrée : recharge automatiquement si un fichier a été
    ajouté/modifié dans historique_spreads/ depuis le dernier chargement."""
    if not HIST_DATA_DIR.exists():
        return pd.DataFrame(columns=_EMPTY_COLS)
    mtimes = [f.stat().st_mtime for f in HIST_DATA_DIR.glob("*.xlsx")]
    dir_mtime = max(mtimes) if mtimes else 0.0
    return _load_historical_spreads(dir_mtime)
