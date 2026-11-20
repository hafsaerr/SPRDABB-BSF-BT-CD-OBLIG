"""Export Excel des spreads : détail, récapitulatifs par instrument et par secteur."""
from __future__ import annotations

import io

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from .maroclear import ordre_tranche
from .referentiels import LIBELLES

BLEU = PatternFill("solid", start_color="12355B")
GRIS = PatternFill("solid", start_color="EEF1F5")
BLANC_GRAS = Font(bold=True, color="FFFFFF")
GRAS = Font(bold=True)
TRAIT = Side(style="thin", color="C9CED6")
BORD = Border(left=TRAIT, right=TRAIT, top=TRAIT, bottom=TRAIT)

DETAIL = ["isin", "libelle", "instrument", "nature", "type_taux", "remboursement", "emetteur",
          "secteur", "date_emission", "date_echeance", "maturite_initiale_ans", "tranche_maturite",
          "periodicite", "prochain_coupon", "prochaine_revision", "taux_facial", "methode",
          "duree_reference_ans", "date_courbe", "source_courbe", "taux_bdt", "spread_pb", "statut"]
LIB_SPREAD = {"methode": "Base de calcul", "duree_reference_ans": "Durée de référence (ans)",
              "source_courbe": "Source de la courbe",
              "date_courbe": "Date de la courbe BDT", "taux_bdt": "Taux BDT", "spread_pb": "Spread (pb)"}


def _feuille(wb, titre: str, df: pd.DataFrame, formats: dict | None = None) -> None:
    ws = wb.create_sheet(titre[:31])
    formats = formats or {}
    for j, col in enumerate(df.columns, 1):
        c = ws.cell(1, j, col)
        c.fill, c.font, c.border = BLEU, BLANC_GRAS, BORD
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for i, ligne in enumerate(df.itertuples(index=False), 2):
        for j, v in enumerate(ligne, 1):
            c = ws.cell(i, j, None if pd.isna(v) else v)
            c.border = BORD
            if i % 2:
                c.fill = GRIS
            if df.columns[j - 1] in formats:
                c.number_format = formats[df.columns[j - 1]]
    for j, col in enumerate(df.columns, 1):
        largeur = max([len(str(col))] + [len(str(v)) for v in df.iloc[:200, j - 1]])
        ws.column_dimensions[get_column_letter(j)].width = min(max(10, largeur + 2), 55)
    ws.row_dimensions[1].height = 32
    ws.freeze_panes = "B2"
    ws.auto_filter.ref = ws.dimensions


def _recap(df: pd.DataFrame, ligne: str) -> pd.DataFrame:
    v = df.dropna(subset=["spread_pb"])
    if v.empty:
        return pd.DataFrame()
    g = (v.groupby([ligne, "tranche_maturite"])["spread_pb"]
         .agg(["count", "mean", "min", "max"]).reset_index())
    g["_o"] = g["tranche_maturite"].map(ordre_tranche)
    g = g.sort_values([ligne, "_o"]).drop(columns="_o")
    g.columns = [LIBELLES.get(ligne, ligne), "Tranche de maturité", "Nombre", "Spread moyen (pb)",
                 "Spread min (pb)", "Spread max (pb)"]
    return g.round(1)


def classeur(df: pd.DataFrame) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)
    detail = df[[c for c in DETAIL if c in df.columns]].copy()
    for c in ("taux_facial", "taux_bdt"):
        detail[c] = detail[c].astype(float)
    libelles = {c: LIB_SPREAD.get(c, LIBELLES.get(c, c)) for c in detail.columns}
    detail = detail.rename(columns=libelles)
    _feuille(wb, "Spreads", detail, {
        libelles["taux_facial"]: "0.000%", libelles["taux_bdt"]: "0.000%",
        libelles["spread_pb"]: "0.0", libelles["date_emission"]: "dd/mm/yyyy",
        libelles["date_echeance"]: "dd/mm/yyyy", libelles["date_courbe"]: "dd/mm/yyyy",
        libelles["prochain_coupon"]: "dd/mm/yyyy", libelles["prochaine_revision"]: "dd/mm/yyyy"})
    r = _recap(df, "instrument")
    if len(r):
        _feuille(wb, "Récap par instrument", r)
    oblig = df[df["famille"].isin(["Obligations", "Titrisation"])]
    r = _recap(oblig, "secteur")
    if len(r):
        _feuille(wb, "Récap obligations par secteur", r)
    r = _recap(oblig, "nature")
    if len(r):
        _feuille(wb, "Récap obligations par nature", r)

    wb.properties.creator = "Hafsa Errachidi"
    wb.properties.lastModifiedBy = "Hafsa Errachidi"
    wb.properties.language = "fr-FR"
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()
