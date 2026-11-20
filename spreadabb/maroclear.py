"""Lecture et nettoyage du référentiel Maroclear (REPV_*.zip / .xml / .xlsx)."""
from __future__ import annotations

import io
import re
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from . import referentiels as R

SEUIL_TAUX_FICTIF = 0.0005  # taux < 0,05 % : valeur de remplissage, pas un vrai coupon


@dataclass
class Rapport:
    fichier: str
    blocs: dict = field(default_factory=dict)        # nombre de lignes par bloc du XML
    etapes: list = field(default_factory=list)       # (étape, nombre de lignes touchées)
    anomalies: dict = field(default_factory=dict)    # type d'anomalie -> nombre

    def etape(self, libelle: str, n: int) -> None:
        self.etapes.append({"etape": libelle, "lignes": int(n)})

    def to_dict(self) -> dict:
        return {"fichier": self.fichier, "blocs": self.blocs,
                "etapes": self.etapes, "anomalies": self.anomalies}


# ── Lecture ──────────────────────────────────────────────────────────────────

def _lire_xml(contenu: bytes, rapport: Rapport) -> tuple[pd.DataFrame, pd.DataFrame]:
    racine = ET.fromstring(contenu)
    for bloc in racine:
        rapport.blocs[bloc.tag] = len(bloc)
    dettes = racine.find("Debts")
    if dettes is None:
        raise ValueError("Le fichier ne contient pas de bloc <Debts> (titres de dette).")
    lignes, coupons = [], []
    for d in dettes:
        ligne = {}
        for e in d:
            if e.tag == "CouponPayDates":
                isin = (d.findtext("INSTRID") or "").strip()
                coupons.extend((isin, (c.text or "").strip()) for c in e)
            else:
                ligne[e.tag] = e.text
        lignes.append(ligne)
    return pd.DataFrame(lignes), pd.DataFrame(coupons, columns=["isin", "date_coupon"])


def lire_fichier(nom: str, contenu: bytes) -> tuple[pd.DataFrame, pd.DataFrame, Rapport]:
    """Renvoie (titres bruts, dates de coupon, rapport) quel que soit le format reçu."""
    rapport = Rapport(fichier=nom)
    bas = nom.lower()
    if bas.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(contenu)) as z:
            internes = [n for n in z.namelist() if n.lower().endswith((".xml", ".xlsx"))]
            if not internes:
                raise ValueError("Aucun fichier XML ou Excel dans l'archive.")
            return lire_fichier(internes[0], z.read(internes[0]))
    if bas.endswith(".xml"):
        brut, coupons = _lire_xml(contenu, rapport)
    elif bas.endswith((".xlsx", ".xls")):
        brut = pd.read_excel(io.BytesIO(contenu), dtype=str)
        coupons = pd.DataFrame(columns=["isin", "date_coupon"])
        rapport.blocs["Excel"] = len(brut)
    else:
        raise ValueError("Format non reconnu : déposer le .zip Maroclear, le .xml ou un .xlsx.")
    rapport.etape("Titres de dette lus", len(brut))
    rapport.etape("Dates de coupon lues", len(coupons))
    return brut, coupons, rapport


# ── Nettoyage ────────────────────────────────────────────────────────────────

def _texte(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
    return s.replace("", pd.NA)


def _date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s.astype("string").str[:10], errors="coerce", format="%Y-%m-%d").dt.date


def _nombre(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype("string").str.replace(",", ".").str.replace(" ", ""), errors="coerce")


def nettoyer(brut: pd.DataFrame, coupons: pd.DataFrame, rapport: Rapport,
             aujourd_hui: date | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    auj = aujourd_hui or date.today()
    df = brut.copy()

    # 1. Renommage des colonnes en noms lisibles
    connues = {k: v[0] for k, v in R.COLONNES_MAROCLEAR.items() if k in df.columns}
    df = df[list(connues)].rename(columns=connues)
    for interne, _ in R.COLONNES_MAROCLEAR.values():
        if interne not in df.columns:
            df[interne] = pd.NA
    rapport.etape("Colonnes renommées", len(connues))

    # 2. Textes : espaces superflus, cellules vides
    for c in df.columns:
        df[c] = _texte(df[c])

    # 3. Types : dates et nombres
    for c in ("date_emission", "date_echeance", "date_jouissance"):
        df[c] = _date(df[c])
    for c in ("nominal", "nominal_actuel", "nb_titres", "montant_emis", "taux_facial"):
        df[c] = _nombre(df[c])

    # 4. Doublons d'ISIN
    avant = len(df)
    df = df.dropna(subset=["isin"]).drop_duplicates(subset=["isin"], keep="first")
    rapport.etape("Doublons ou ISIN vides supprimés", avant - len(df))

    anomalies: dict[str, list] = {i: [] for i in df.index}

    def noter(masque: pd.Series, texte: str) -> None:
        idx = df.index[masque.fillna(False).astype(bool)]
        for i in idx:
            anomalies[i].append(texte)
        rapport.anomalies[texte] = rapport.anomalies.get(texte, 0) + len(idx)

    # 5. Taux : saisis en points de base au lieu de pourcentages
    en_pb = df["taux_facial"] > 30
    df.loc[en_pb, "taux_facial"] = df.loc[en_pb, "taux_facial"] / 100
    noter(en_pb, "Taux saisi en points de base (corrigé)")
    df["taux_facial"] = df["taux_facial"] / 100  # pourcentage -> décimal

    # 6. Montant émis manquant : nominal x nombre de titres
    manque = df["montant_emis"].isna() & df["nominal"].notna() & df["nb_titres"].notna()
    df.loc[manque, "montant_emis"] = df.loc[manque, "nominal"] * df.loc[manque, "nb_titres"]
    rapport.etape("Montant émis reconstitué (nominal × titres)", manque.sum())

    # 7. Traduction des codes
    df["type_taux"] = df["type_taux_code"].map(R.TYPES_TAUX)
    df.loc[df["type_code"].eq("FLRT"), "type_taux"] = "Révisable"
    df.loc[df["type_code"].eq("ZCBD"), "type_taux"] = "Zéro-coupon"
    df["remboursement"] = df["remboursement_code"].map(R.REMBOURSEMENTS)
    df.loc[df["type_code"].eq("AMBD"), "remboursement"] = "Amortissable"
    df["periodicite"] = df["periodicite_code"].map(R.PERIODICITES)
    df["frequence_amort"] = df["frequence_amort_code"].map(R.PERIODICITES)
    df["forme"] = df["forme_code"].map(R.FORMES)
    df["cote"] = df["cote_code"].map({"Y": "Oui", "N": "Non"})
    df["statut"] = df["statut_code"].map(R.STATUTS).fillna(df["statut_code"])

    # 8. Instrument, famille, nature
    lib = (df["libelle"].fillna("") + " " + df["libelle_court"].fillna("")).str.upper()
    premier = df["libelle"].fillna("").str.upper().str.split().str[0]
    cat = df["categorie_code"].fillna("")
    instrument = cat.map(R.CATEGORIES).fillna("Autre")
    tcn = cat.eq("TCN")
    instrument[tcn & premier.eq("CD")] = "CD"
    instrument[tcn & premier.eq("BSF")] = "BSF"
    instrument[tcn & premier.eq("BT")] = "BT"
    instrument[tcn & ~premier.isin(["CD", "BSF", "BT"]) & lib.str.contains(r"\bBSF\b")] = "BSF"
    instrument[tcn & ~premier.isin(["CD", "BSF", "BT"]) & lib.str.contains(r"\bCD\b")] = "CD"
    instrument[cat.eq("BDT")] = "BDT"
    df["instrument"] = instrument
    df["famille"] = np.select(
        [tcn, cat.eq("BDT"), cat.isin(["OBL_ORDN", "OBL_SUBD", "OBL_CONV"]), cat.eq("FPCT")],
        ["TCN", "Trésor", "Obligations", "Titrisation"], "Autre")

    # Catégorie Maroclear « ordinaire » alors que le libellé indique une dette subordonnée
    sub_libelle = cat.eq("OBL_ORDN") & lib.str.contains(r"\bSUB\b|SUBORD")
    noter(sub_libelle, "Classé ordinaire, libellé subordonné (corrigé)")
    cat = cat.where(~sub_libelle, "OBL_SUBD")
    df["instrument"] = df["instrument"].where(~sub_libelle, R.CATEGORIES["OBL_SUBD"])
    perpetuel = lib.str.contains(r"\bPERP|PERPET|INDETERMIN|DUREE INDET")
    nature = cat.map({"OBL_ORDN": "Ordinaire", "OBL_SUBD": "Subordonnée", "OBL_CONV": "Convertible",
                      "FPCT": "Titrisation", "BDT": "Trésor", "TCN": "TCN"}).fillna("Autre")
    nature[cat.eq("OBL_SUBD") & perpetuel] = "Subordonnée perpétuelle"
    nature[~cat.eq("OBL_SUBD") & perpetuel & cat.str.startswith("OBL")] = "Perpétuelle"
    df["nature"] = nature

    # 9. Secteur, banque
    df["secteur"] = [R.secteur_de(e, c) for e, c in zip(df["emetteur"].fillna(""), cat)]
    df["banque"] = df["secteur"].eq("BANQUE")

    # 10. Maturités
    em = pd.to_datetime(df["date_emission"])
    ec = pd.to_datetime(df["date_echeance"])
    df["maturite_initiale_j"] = (ec - em).dt.days
    df["maturite_initiale_ans"] = (df["maturite_initiale_j"] / 365.25).round(2)
    df["maturite_residuelle_ans"] = ((ec - pd.Timestamp(auj)).dt.days / 365.25).clip(lower=0).round(2)
    df["en_vie"] = df["statut_code"].eq("ACTI") & (ec >= pd.Timestamp(auj))
    df["tranche_maturite"] = df["maturite_initiale_j"].map(tranche_maturite)

    # 11. Calendrier des coupons
    cp = coupons.copy()
    cp["date_coupon"] = _date(cp["date_coupon"])
    cp = cp.dropna().drop_duplicates()
    cp = cp[cp["isin"].isin(df["isin"])]
    rapport.etape("Dates de coupon retenues", len(cp))
    g = cp.groupby("isin")["date_coupon"]
    passes = cp[cp["date_coupon"] < auj].groupby("isin")["date_coupon"].max()
    futurs = cp[cp["date_coupon"] >= auj].groupby("isin")["date_coupon"]
    df["nb_coupons"] = df["isin"].map(g.size()).fillna(0).astype(int)
    df["dernier_coupon"] = df["isin"].map(passes)
    df["prochain_coupon"] = df["isin"].map(futurs.min())
    df["coupons_restants"] = df["isin"].map(futurs.size()).fillna(0).astype(int)

    # Sans calendrier : prochain coupon déduit de la périodicité
    mois = df["periodicite_code"].map(R.MOIS_PAR_PERIODE)
    sans = df["prochain_coupon"].isna() & df["en_vie"] & mois.notna() & df["date_emission"].notna()
    for i in df.index[sans]:
        df.at[i, "prochain_coupon"] = prochaine_date(df.at[i, "date_emission"], int(mois[i]), auj,
                                                     df.at[i, "date_echeance"])

    # Révisable : le taux est revu à chaque date de coupon
    rev = df["type_taux"].eq("Révisable") & df["en_vie"]
    df["prochaine_revision"] = None
    df.loc[rev, "prochaine_revision"] = df.loc[rev, "prochain_coupon"]

    # 12. Contrôles
    noter(df["date_echeance"].notna() & df["date_emission"].notna()
          & (df["maturite_initiale_j"] <= 0), "Échéance antérieure à l'émission")
    noter(df["date_emission"].isna() | df["date_echeance"].isna(), "Date manquante")
    noter(~df["type_taux"].eq("Zéro-coupon") & df["taux_facial"].isna(), "Taux facial manquant")
    noter(df["taux_facial"].notna() & (df["taux_facial"] < SEUIL_TAUX_FICTIF)
          & ~df["type_taux"].eq("Zéro-coupon"), "Taux fictif (< 0,05 %)")
    noter(df["en_vie"] & ~df["type_taux"].eq("Zéro-coupon") & df["nb_coupons"].eq(0),
          "Pas de calendrier de coupons")
    noter(df["remboursement"].eq("Amortissable") & df["frequence_amort_code"].isna(),
          "Amortissable sans fréquence d'amortissement")
    noter(df["statut_code"].eq("ACTI") & (ec < pd.Timestamp(auj)), "Actif mais déjà échu")

    df["anomalies"] = [" ; ".join(anomalies[i]) for i in df.index]
    rapport.etape("Titres nettoyés", len(df))
    rapport.etape("Titres en vie", int(df["en_vie"].sum()))
    return df.reset_index(drop=True), cp.reset_index(drop=True)


def tranche_maturite(jours) -> str | None:
    """Tranche de maturité standard du marché (13/26/52 semaines puis années)."""
    if jours is None or pd.isna(jours) or jours <= 0:
        return None
    if jours <= 100:
        return "13 semaines"
    if jours <= 200:
        return "26 semaines"
    if jours <= 400:
        return "52 semaines"
    ans = jours / 365.25
    if ans < 10.5:
        return f"{max(2, int(round(ans)))} ans"
    # Au-delà de 10 ans : pilier standard le plus proche
    return f"{min((15, 20, 25, 30), key=lambda p: abs(p - ans))} ans"


def ordre_tranche(t: str | None) -> float:
    if not t:
        return 1e9
    n = int(re.match(r"\d+", t).group())
    return n * 7 / 365 if "semaine" in t else float(n)


def prochaine_date(debut: date, mois: int, apres: date, fin: date | None) -> date | None:
    d, k = debut, 0
    while d < apres:
        k += 1
        d = debut + relativedelta(months=mois * k)
    return d if fin is None or d <= fin else fin


def charger(nom: str, contenu: bytes, aujourd_hui: date | None = None):
    brut, coupons, rapport = lire_fichier(nom, contenu)
    titres, coupons = nettoyer(brut, coupons, rapport, aujourd_hui)
    return titres, coupons, rapport
