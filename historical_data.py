from __future__ import annotations

"""
historical_data.py — Lecture dynamique de la data historique de spreads.

Les fichiers Excel déposés dans historique_spreads/ suivent deux formats :

- CD_*, BSF_CONSO_*, BSF_BAIL_*, BT_* : un onglet détail par émetteur, avec
  les colonnes CODE / DETAILS DU TITRE / DATE D'EMISSION / DATE D'ECHEANCE /
  Maturite residuelle / TAUX BDT / Spread / TAUX D'INTERET (export CD/BSF/BT
  de app.py). Le secteur n'existe pas pour ces types : on leur assigne un
  pseudo-secteur fixe (Banques / Sociétés de financement / Leasing /
  Corporates) selon leur catégorie.
- TOUTES_OBLIG (fichier OBLIG) : une ligne par obligation tous secteurs
  confondus, avec les colonnes INSTRID / NOM_INSTRUMENT / EMETTEUR / SECTEUR /
  DATE_EMISSION / DATE_ECHEANCE / INTERESTRATE / MATURITE_ANS /
  TAUX_BDT_INTERP / SPREAD_BPS (export OBLIG_ORDN de app.py). Le SECTEUR est
  déjà présent dans la donnée.

Les onglets RECAP_* (et les onglets par secteur d'OBLIG, redondants avec
TOUTES_OBLIG) sont ignorés. Aucun nom d'émetteur/secteur n'est codé en dur :
ajouter un onglet ou un fichier dans historique_spreads/ est pris en compte
automatiquement au prochain chargement.

Toutes les lignes sont ramenées à un schéma commun (mêmes noms de colonnes
que l'ancien export CD/BSF/BT, pour rester compatible avec la page
"Historique des Spreads") :
    CODE, DETAILS DU TITRE, DATE D'EMISSION, DATE D'ECHEANCE,
    Maturite residuelle, TAUX BDT, Spread, TAUX D'INTERET,
    Type, Emetteur, Secteur, Categorie
"""

import re
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

# Pseudo-secteur pour les types sans colonne SECTEUR native (tout sauf OBLIG).
PSEUDO_SECTEUR: dict[str, str] = {
    "CD":        "Banques",
    "BSF_CONSO": "Sociétés de financement",
    "BSF_BAIL":  "Leasing",
    "BT":        "Corporates",
}

_CANON_COLS = [
    "CODE", "DETAILS DU TITRE", "DATE D'EMISSION", "DATE D'ECHEANCE",
    "Maturite residuelle", "TAUX BDT", "Spread", "TAUX D'INTERET",
    "Type", "Emetteur", "Secteur", "Categorie",
]


def _match_asset_type(sheet_name: str) -> Optional[tuple[str, str]]:
    """Retourne (type_actif, nom_emetteur) si le nom d'onglet correspond à un
    préfixe CD_/BSF_CONSO_/BSF_BAIL_/BT_ connu, sinon None."""
    upper = sheet_name.upper()
    if upper.startswith("RECAP"):
        return None
    for prefix, asset_type in _ASSET_PREFIXES:
        if upper.startswith(prefix):
            emetteur = sheet_name[len(prefix):].strip()
            if emetteur:
                return asset_type, emetteur
    return None


def _oblig_categorie(nom_instrument) -> str:
    """Sous-catégorie d'obligation déduite du nom du titre (mot-clé) :
    OBL_SUBD (subordonnée), OBL_SUBD_PERP (subordonnée perpétuelle),
    OBL_ORDN_GREEN (obligation verte), sinon OBL_ORDN par défaut."""
    s = str(nom_instrument).upper()
    is_sub = bool(re.search(r"\bSUB\b", s)) or "SUBD" in s or "SUBORD" in s
    is_perp = "PERP" in s
    is_green = "GREEN" in s or "VERT" in s
    if is_sub and is_perp:
        return "OBL_SUBD_PERP"
    if is_sub:
        return "OBL_SUBD"
    if is_green:
        return "OBL_ORDN_GREEN"
    return "OBL_ORDN"


def _read_tcn_sheet(xl: pd.ExcelFile, sheet_name: str, asset_type: str, emetteur: str) -> Optional[pd.DataFrame]:
    """Lit un onglet CD_*/BSF_CONSO_*/BSF_BAIL_*/BT_* (schéma CD/BSF/BT)."""
    try:
        df = xl.parse(sheet_name)
    except Exception:
        return None
    if df.empty or "DATE D'EMISSION" not in df.columns or "DATE D'ECHEANCE" not in df.columns:
        return None

    out = pd.DataFrame(index=df.index)
    out["CODE"]              = df["CODE"] if "CODE" in df.columns else pd.NA
    out["DETAILS DU TITRE"]  = df["DETAILS DU TITRE"] if "DETAILS DU TITRE" in df.columns else pd.NA
    out["DATE D'EMISSION"]   = df["DATE D'EMISSION"]
    out["DATE D'ECHEANCE"]   = df["DATE D'ECHEANCE"]
    out["TAUX BDT"]          = pd.to_numeric(df.get("TAUX BDT"), errors="coerce")
    out["Spread"]            = pd.to_numeric(df.get("Spread"), errors="coerce")
    out["TAUX D'INTERET"]    = pd.to_numeric(df.get("TAUX D'INTERET"), errors="coerce")
    out["Type"]              = asset_type
    out["Emetteur"]          = emetteur
    out["Secteur"]           = PSEUDO_SECTEUR.get(asset_type, "AUTRES")
    out["Categorie"]         = "BSF" if asset_type in ("BSF_CONSO", "BSF_BAIL") else asset_type
    return out


def _read_oblig_sheet(xl: pd.ExcelFile) -> Optional[pd.DataFrame]:
    """Lit l'onglet TOUTES_OBLIG (schéma OBLIG_ORDN) — une ligne par
    obligation, secteur déjà présent dans la colonne SECTEUR."""
    sheet = next((s for s in xl.sheet_names if s.strip().upper() == "TOUTES_OBLIG"), None)
    if sheet is None:
        return None
    try:
        df = xl.parse(sheet)
    except Exception:
        return None
    required = {"DATE_EMISSION", "DATE_ECHEANCE", "EMETTEUR", "SECTEUR"}
    if df.empty or not required.issubset(df.columns):
        return None

    out = pd.DataFrame(index=df.index)
    out["CODE"]             = df["INSTRID"] if "INSTRID" in df.columns else pd.NA
    out["DETAILS DU TITRE"] = df["NOM_INSTRUMENT"] if "NOM_INSTRUMENT" in df.columns else pd.NA
    out["DATE D'EMISSION"]  = df["DATE_EMISSION"]
    out["DATE D'ECHEANCE"]  = df["DATE_ECHEANCE"]
    out["TAUX BDT"]         = pd.to_numeric(df.get("TAUX_BDT_INTERP"), errors="coerce")
    out["Spread"]           = pd.to_numeric(df.get("SPREAD_BPS"), errors="coerce")
    # INTERESTRATE est déjà décimal (ex: 0.0497) ; TAUX D'INTERET des autres
    # types est un nombre en pourcentage (ex: 2.66) -> on aligne l'unité.
    out["TAUX D'INTERET"]   = pd.to_numeric(df.get("INTERESTRATE"), errors="coerce") * 100
    out["Type"]             = "OBLIG"
    out["Emetteur"]         = df["EMETTEUR"]
    out["Secteur"]          = df["SECTEUR"]
    out["Categorie"] = (
        df["NOM_INSTRUMENT"].apply(_oblig_categorie) if "NOM_INSTRUMENT" in df.columns
        else "OBL_ORDN"
    )
    return out


@st.cache_data(show_spinner="Chargement de l'historique des spreads…")
def _load_historical_spreads(dir_mtime: float) -> pd.DataFrame:
    """
    Scanne historique_spreads/*.xlsx, lit les onglets CD_*/BSF_CONSO_*/
    BSF_BAIL_*/BT_* ainsi que TOUTES_OBLIG, et les concatène en un seul
    DataFrame au schéma commun (voir docstring du module).

    `dir_mtime` ne sert qu'à invalider le cache Streamlit quand un fichier du
    dossier est ajouté/modifié — voir get_historical_spreads().
    """
    frames: list[pd.DataFrame] = []

    if not HIST_DATA_DIR.exists():
        return pd.DataFrame(columns=_CANON_COLS)

    for xlsx_path in sorted(HIST_DATA_DIR.glob("*.xlsx")):
        try:
            xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
        except Exception:
            continue

        oblig_df = _read_oblig_sheet(xl)
        if oblig_df is not None:
            frames.append(oblig_df)

        for sheet_name in xl.sheet_names:
            match = _match_asset_type(sheet_name)
            if match is None:
                continue
            asset_type, emetteur = match
            tcn_df = _read_tcn_sheet(xl, sheet_name, asset_type, emetteur)
            if tcn_df is not None:
                frames.append(tcn_df)

    if not frames:
        return pd.DataFrame(columns=_CANON_COLS)

    out = pd.concat(frames, ignore_index=True, sort=False)

    out["DATE D'EMISSION"] = pd.to_datetime(out["DATE D'EMISSION"], errors="coerce")
    out["DATE D'ECHEANCE"] = pd.to_datetime(out["DATE D'ECHEANCE"], errors="coerce")

    # Lignes sans date exploitable = inutilisables pour le bucketing de
    # maturité -> écartées proprement plutôt que de faire planter les pages.
    out = out.dropna(subset=["DATE D'EMISSION", "DATE D'ECHEANCE"])

    # Maturité recalculée uniformément depuis les dates (les colonnes source
    # "Maturite residuelle"/"MATURITE_ANS" ont des arrondis légèrement
    # différents selon le fichier d'origine).
    out["Maturite residuelle"] = (
        (out["DATE D'ECHEANCE"] - out["DATE D'EMISSION"]).dt.days / 365.25
    ).round(2)

    return out.reset_index(drop=True)


def get_historical_spreads() -> pd.DataFrame:
    """Point d'entrée : recharge automatiquement si un fichier a été
    ajouté/modifié dans historique_spreads/ depuis le dernier chargement."""
    if not HIST_DATA_DIR.exists():
        return pd.DataFrame(columns=_CANON_COLS)
    mtimes = [f.stat().st_mtime for f in HIST_DATA_DIR.glob("*.xlsx")]
    dir_mtime = max(mtimes) if mtimes else 0.0
    return _load_historical_spreads(dir_mtime)
