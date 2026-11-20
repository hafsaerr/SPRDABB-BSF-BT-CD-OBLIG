"""Exports Excel des spreads.

- Obligations : toutes les obligations, puis pour chaque secteur une feuille de détail et une feuille
  récapitulative (max / min / moyenne par émetteur et tranche de maturité), découpée par mode de
  remboursement.
- TCN : CD par émetteur, BSF séparés entre crédit à la consommation et leasing, BT ; chaque groupe a
  sa feuille récapitulative (tranche de maturité × émetteur) et une feuille par émetteur.
"""
from __future__ import annotations

import io
import re

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from .maroclear import ordre_tranche

BLEU = PatternFill("solid", start_color="1F3864")      # en-têtes des obligations
ORANGE = PatternFill("solid", start_color="C8501E")
JAUNE = PatternFill("solid", start_color="FFFF00")
GRIS_TCN = PatternFill("solid", start_color="D9D9D9")
BLEU_CLAIR = PatternFill("solid", start_color="DCE6F1")
SURLIGNEES = {"taux_facial", "taux_bdt", "spread_pb"}  # colonnes surlignées en jaune dans les TCN
TRANCHES_TCN = {"13 semaines": "3 mois", "26 semaines": "6 mois", "52 semaines": "1 an"}
GRIS = PatternFill("solid", start_color="EEF1F5")
BLANC_GRAS = Font(bold=True, color="FFFFFF")
GRAS = Font(bold=True)
CENTRE = Alignment(horizontal="center", vertical="center", wrap_text=True)
TRAIT = Side(style="thin", color="C9CED6")
BORD = Border(left=TRAIT, right=TRAIT, top=TRAIT, bottom=TRAIT)
FORMAT_PB = '0 "pb"'

COLONNES = {
    "isin": "ISIN", "libelle": "Libellé", "emetteur": "Émetteur", "secteur": "Secteur",
    "instrument": "Instrument", "nature": "Nature", "type_taux": "Type de taux", "remboursement": "Remboursement",
    "date_emission": "Date d'émission", "date_echeance": "Date d'échéance",
    "maturite_initiale_ans": "Maturité initiale (ans)", "tranche_maturite": "Tranche de maturité",
    "taux_facial": "Taux facial", "methode": "Base de calcul", "duree_reference_ans": "Durée de référence (ans)",
    "date_courbe": "Date de la courbe BDT", "source_courbe": "Source de la courbe", "taux_bdt": "Taux BDT",
    "spread_pb": "Spread (pb)", "prochain_coupon": "Prochain coupon", "prochaine_revision": "Prochaine révision",
    "encours": "Encours (MAD)",
}
DETAIL_OBLIG = ["isin", "libelle", "emetteur", "secteur", "nature", "type_taux", "remboursement", "date_emission",
                "date_echeance", "maturite_initiale_ans", "tranche_maturite", "taux_facial", "methode",
                "duree_reference_ans", "taux_bdt", "spread_pb", "date_courbe", "source_courbe", "prochain_coupon",
                "prochaine_revision", "encours"]
DETAIL_TCN = ["isin", "libelle", "emetteur", "type_taux", "remboursement", "date_emission", "date_echeance",
              "maturite_initiale_ans", "tranche_maturite", "taux_facial", "methode", "duree_reference_ans", "taux_bdt",
              "spread_pb", "date_courbe", "source_courbe", "encours"]
FORMATS = {"taux_facial": "0.000%", "taux_bdt": "0.000%", "spread_pb": "0.0", "maturite_initiale_ans": "0.00",
           "duree_reference_ans": "0.00", "date_emission": "dd/mm/yyyy", "date_echeance": "dd/mm/yyyy",
           "date_courbe": "dd/mm/yyyy", "prochain_coupon": "dd/mm/yyyy", "prochaine_revision": "dd/mm/yyyy",
           "encours": "#,##0"}

# BSF : crédit à la consommation ou leasing, d'après le nom de l'émetteur
BSF_CONSO = ("SOFAC", "SALAFIN", "AXA CREDIT", "WAFASALAF", "EQDOM", "SEDM", "EQUIPEMENT DOMESTIQUE", "TASLIF",
             "RCI", "JAIDA", "VIVALIS")
BSF_LEASING = ("LEASING", "LEASI", "MAGHREBAIL", "MAGHREB BAIL", "WAFABAIL", "BAIL", "LOCASOM")
NOMS_COURTS = {"SEDM": "EQDOM"}


def groupe_bsf(emetteur: str, secteur: str = "") -> str:
    e, s = str(emetteur).upper(), str(secteur).upper()
    if any(k in e for k in BSF_CONSO) or "CONSOMMATION" in s:
        return "Crédit à la consommation"
    if any(k in e for k in BSF_LEASING) or "LEASING" in s:
        return "Leasing"
    return "Autres sociétés de financement"


# ── Outils de mise en forme ──────────────────────────────────────────────────

class _Classeur:
    def __init__(self):
        self.wb = Workbook()
        self.wb.remove(self.wb.active)

    def feuille(self, nom: str):
        nom = re.sub(r"[\[\]:*?/\\]", "-", str(nom)).strip()[:31] or "Feuille"
        base, i = nom, 1
        while nom in self.wb.sheetnames:
            suffixe = f" ({i})"
            nom, i = base[:31 - len(suffixe)] + suffixe, i + 1
        return self.wb.create_sheet(nom)

    def octets(self) -> bytes:
        if not self.wb.sheetnames:
            self.feuille("Vide").cell(1, 1, "Aucun titre avec spread pour cette sélection.")
        p = self.wb.properties
        p.creator = p.lastModifiedBy = "Hafsa Errachidi"
        p.language = "fr-FR"
        out = io.BytesIO()
        self.wb.save(out)
        return out.getvalue()


def _cellule(ws, r, c, v, remplissage=None, police=None, fmt=None):
    cel = ws.cell(r, c, None if v is None or (not isinstance(v, str) and pd.isna(v)) else v)
    cel.border, cel.alignment = BORD, CENTRE
    if remplissage:
        cel.fill = remplissage
    if police:
        cel.font = police
    if fmt:
        cel.number_format = fmt
    return cel


def _largeurs(ws, mini=10, maxi=48):
    for j in range(1, ws.max_column + 1):
        long = max((len(str(c.value)) for c in ws[get_column_letter(j)] if c.value is not None), default=8)
        ws.column_dimensions[get_column_letter(j)].width = min(max(mini, long + 2), maxi)


def _tableau(ws, df: pd.DataFrame, colonnes: list[str], ligne: int = 1, style: str = "oblig") -> int:
    """Tableau de détail ; renvoie la dernière ligne écrite.
    style « oblig » : en-tête bleu marine ; « tcn » : en-tête jaune, lignes grises, taux et spread surlignés."""
    colonnes = [c for c in colonnes if c in df.columns]
    for j, c in enumerate(colonnes, 1):
        if style == "tcn":
            _cellule(ws, ligne, j, COLONNES.get(c, c).upper(), JAUNE, GRAS)
        else:
            _cellule(ws, ligne, j, COLONNES.get(c, c).upper(), BLEU, BLANC_GRAS)
    ws.row_dimensions[ligne].height = 32
    for i, t in enumerate(df[colonnes].itertuples(index=False), ligne + 1):
        for j, (c, v) in enumerate(zip(colonnes, t), 1):
            if c in ("taux_facial", "taux_bdt") and v is not None and not pd.isna(v):
                v = float(v)
            if c in ("emetteur",):
                v = NOMS_COURTS.get(v, v)
            if style == "tcn":
                fond = JAUNE if c in SURLIGNEES else GRIS_TCN
            else:
                fond = GRIS if i % 2 else None
            _cellule(ws, i, j, v, fond, GRAS if c == "spread_pb" else None, fmt=FORMATS.get(c))
    if ligne == 1:
        ws.freeze_panes = "C2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(colonnes))}{max(2, len(df) + 1)}"
    return ligne + len(df)


def _tranches(df: pd.DataFrame) -> list[str]:
    return sorted(df["tranche_maturite"].dropna().unique(), key=ordre_tranche)


def _croise(ws, df: pd.DataFrame, lignes: str, colonnes: str, titre: str, ligne: int = 1) -> int:
    """Trois tableaux empilés (max, min, moyenne des spreads) ; renvoie la ligne suivante libre."""
    if df.empty:
        return ligne
    val_l = _tranches(df) if lignes == "tranche_maturite" else sorted(df[lignes].dropna().unique())
    val_c = _tranches(df) if colonnes == "tranche_maturite" else sorted(df[colonnes].dropna().unique())
    entete = "Maturité" if lignes == "tranche_maturite" else "Émetteur"
    stats = df.groupby([lignes, colonnes])["spread_pb"].agg(["max", "min", "mean", "count"])
    for libelle, cle in [("MAXIMUM DES SPREADS", "max"), ("MINIMUM DES SPREADS", "min"),
                         ("MOYENNE DES SPREADS", "mean")]:
        n = 1 + len(val_c)
        _cellule(ws, ligne, 1, f"{titre} — {libelle}", ORANGE, BLANC_GRAS)
        for j in range(2, n + 1):
            _cellule(ws, ligne, j, None, ORANGE)
        if n > 1:
            ws.merge_cells(start_row=ligne, start_column=1, end_row=ligne, end_column=n)
        ligne += 1
        _cellule(ws, ligne, 1, entete, JAUNE, GRAS)
        for j, c in enumerate(val_c, 2):
            _cellule(ws, ligne, j, NOMS_COURTS.get(c, c), JAUNE, GRAS)
        ligne += 1
        for l in val_l:
            _cellule(ws, ligne, 1, NOMS_COURTS.get(l, l), police=GRAS)
            for j, c in enumerate(val_c, 2):
                v = stats[cle].get((l, c))
                _cellule(ws, ligne, j, "-" if v is None or pd.isna(v) else round(float(v), 1), fmt=FORMAT_PB)
            ligne += 1
        ligne += 2
    return ligne


def _resume_maturite(ws, df: pd.DataFrame, ligne: int) -> None:
    """Sous un détail : moyenne, max et min des spreads par tranche de maturité."""
    for j, h in enumerate(["MATURITÉ", "NOMBRE", "MOYENNE SPREAD", "SPREAD MAX", "SPREAD MIN"], 1):
        _cellule(ws, ligne, j, h, BLEU_CLAIR, GRAS)
    g = df.groupby("tranche_maturite")["spread_pb"].agg(["count", "mean", "max", "min"])
    for i, tr in enumerate(_tranches(df), ligne + 1):
        _cellule(ws, i, 1, tr, GRIS_TCN, GRAS)
        _cellule(ws, i, 2, int(g.loc[tr, "count"]), GRIS_TCN)
        for j, k in enumerate(["mean", "max", "min"], 3):
            _cellule(ws, i, j, round(float(g.loc[tr, k]), 1), GRIS_TCN, fmt=FORMAT_PB)


def _avec_spread(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["spread_pb"].notna()].sort_values("spread_pb", ascending=False)


# ── Obligations ──────────────────────────────────────────────────────────────

ORDRE_REMB = ["In fine", "Amortissable", "Par tranches"]


def classeur_obligations(df: pd.DataFrame) -> bytes:
    cl = _Classeur()
    df = _avec_spread(df)
    ws = cl.feuille("Toutes les obligations")
    _tableau(ws, df, DETAIL_OBLIG)
    _largeurs(ws)

    # Synthèse : spread moyen par secteur et mode de remboursement
    ws = cl.feuille("Synthèse par secteur")
    g = df.groupby(["secteur", "remboursement"])["spread_pb"].agg(["count", "mean", "min", "max"]).reset_index()
    g["_o"] = g["remboursement"].map(lambda r: ORDRE_REMB.index(r) if r in ORDRE_REMB else 9)
    g = g.sort_values(["secteur", "_o"]).drop(columns="_o")
    for j, h in enumerate(["Secteur", "Remboursement", "Nombre", "Spread moyen", "Spread min", "Spread max"], 1):
        _cellule(ws, 1, j, h, BLEU, BLANC_GRAS)
    for i, t in enumerate(g.itertuples(index=False), 2):
        for j, v in enumerate(t, 1):
            _cellule(ws, i, j, round(float(v), 1) if j >= 4 else v, GRIS if i % 2 else None,
                     fmt=FORMAT_PB if j >= 4 else None)
    _largeurs(ws)

    for secteur in df.groupby("secteur").size().sort_values(ascending=False).index:
        s = df[df["secteur"] == secteur]
        s = s.assign(_o=s["remboursement"].map(lambda r: ORDRE_REMB.index(r) if r in ORDRE_REMB else 9))
        s = s.sort_values(["_o", "spread_pb"], ascending=[True, False]).drop(columns="_o")
        ws = cl.feuille(secteur)
        _tableau(ws, s, DETAIL_OBLIG)
        _largeurs(ws)
        ws = cl.feuille(f"RECAP {secteur}")
        ligne = 1
        for remb in sorted(s["remboursement"].dropna().unique(),
                           key=lambda r: ORDRE_REMB.index(r) if r in ORDRE_REMB else 9):
            ligne = _croise(ws, s[s["remboursement"] == remb], "emetteur", "tranche_maturite",
                            f"{secteur} — {remb.upper()}", ligne)
        _largeurs(ws, mini=12)
    return cl.octets()


# ── TCN ──────────────────────────────────────────────────────────────────────

def _groupe_tcn(cl: _Classeur, df: pd.DataFrame, prefixe: str, titre: str) -> None:
    """Feuille récapitulative (maturité × émetteur), puis une feuille par émetteur."""
    if df.empty:
        return
    ws = cl.feuille(f"RECAP {prefixe}")
    _croise(ws, df, "tranche_maturite", "emetteur", titre)
    _largeurs(ws, mini=12)
    for em, d in df.groupby("emetteur"):
        ws = cl.feuille(f"{prefixe} {NOMS_COURTS.get(em, em)}")
        fin = _tableau(ws, d, DETAIL_TCN, style="tcn")
        _resume_maturite(ws, d, fin + 3)
        _largeurs(ws)


def classeur_tcn(df: pd.DataFrame) -> bytes:
    cl = _Classeur()
    df = _avec_spread(df)
    df = df.assign(tranche_maturite=df["tranche_maturite"].replace(TRANCHES_TCN))
    _groupe_tcn(cl, df[df["instrument"] == "CD"], "CD", "CERTIFICATS DE DÉPÔT")
    bsf = df[df["instrument"] == "BSF"]
    if len(bsf):
        grp = [groupe_bsf(e, s) for e, s in zip(bsf["emetteur"], bsf["secteur"])]
        bsf = bsf.assign(groupe=grp)
        _groupe_tcn(cl, bsf[bsf["groupe"] == "Crédit à la consommation"], "BSF CONSO", "BSF CRÉDIT À LA CONSOMMATION")
        _groupe_tcn(cl, bsf[bsf["groupe"] == "Leasing"], "BSF LEASING", "BSF LEASING")
        _groupe_tcn(cl, bsf[bsf["groupe"] == "Autres sociétés de financement"], "BSF AUTRES", "BSF AUTRES")
    _groupe_tcn(cl, df[df["instrument"] == "BT"], "BT", "BILLETS DE TRÉSORERIE")
    return cl.octets()


COLONNES.update({"mnemonique": "Mnémonique", "periodicite": "Périodicité du coupon", "statut": "Statut",
                 "maturite_residuelle_ans": "Maturité résiduelle (ans)", "coupons_restants": "Coupons restants",
                 "nominal": "Nominal (MAD)", "montant_emis": "Montant émis (MAD)", "anomalies": "Anomalies",
                 "domiciliataire": "Domiciliataire", "frequence_amort": "Fréquence d'amortissement"})
FORMATS.update({"maturite_residuelle_ans": "0.00", "nominal": "#,##0", "montant_emis": "#,##0"})
DETAIL_TITRES = ["isin", "mnemonique", "libelle", "instrument", "nature", "type_taux", "remboursement",
                 "frequence_amort", "emetteur", "secteur", "domiciliataire", "date_emission", "date_echeance",
                 "maturite_initiale_ans", "maturite_residuelle_ans", "tranche_maturite", "taux_facial", "periodicite",
                 "prochain_coupon", "prochaine_revision", "coupons_restants", "nominal", "montant_emis", "encours",
                 "taux_bdt", "spread_pb", "statut", "anomalies"]


def classeur_titres(df: pd.DataFrame) -> bytes:
    """Univers des titres : une feuille pour tous, puis une par famille et une synthèse."""
    cl = _Classeur()
    df = df.sort_values(["famille", "instrument", "emetteur", "date_echeance"])
    ws = cl.feuille("Synthèse")
    g = df.groupby(["famille", "instrument", "type_taux", "remboursement"], dropna=False).agg(
        titres=("isin", "count"), encours=("encours", "sum"), spread=("spread_pb", "mean")).reset_index()
    for j, h in enumerate(["FAMILLE", "INSTRUMENT", "TYPE DE TAUX", "REMBOURSEMENT", "TITRES", "ENCOURS (MAD)",
                           "SPREAD MOYEN"], 1):
        _cellule(ws, 1, j, h, BLEU, BLANC_GRAS)
    for i, t in enumerate(g.itertuples(index=False), 2):
        for j, v in enumerate(t, 1):
            _cellule(ws, i, j, None if j == 7 and pd.isna(v) else v, GRIS if i % 2 else None,
                     fmt={6: "#,##0", 7: FORMAT_PB}.get(j))
    _largeurs(ws)
    ws = cl.feuille("Tous les titres")
    _tableau(ws, df, DETAIL_TITRES)
    _largeurs(ws)
    for fam in ["Obligations", "TCN", "Trésor"]:
        d = df[df["famille"] == fam]
        if len(d):
            ws = cl.feuille(fam)
            _tableau(ws, d, DETAIL_TITRES)
            _largeurs(ws)
    return cl.octets()


def classeur(df: pd.DataFrame) -> bytes:
    """Classeur adapté au contenu : obligations ou TCN."""
    if df["famille"].eq("TCN").all():
        return classeur_tcn(df)
    return classeur_obligations(df[df["famille"].eq("Obligations")])
