"""Courbe BDT d'une date : historique des taux d'abord, site de BAM sinon."""
from __future__ import annotations

import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from .bam import SourceBAM, jour_ouvre
from .taux import calcul_taux, jours_ans

# Piliers de l'historique : (colonne, libellé, durée)
PILIERS = [
    ("s13", "13s", ("j", 91)), ("s26", "26s", ("j", 182)), ("s52", "52s", ("j", 364)),
    ("a2", "2 ans", ("a", 2)), ("a5", "5 ans", ("a", 5)), ("a10", "10 ans", ("a", 10)),
    ("a15", "15 ans", ("a", 15)), ("a20", "20 ans", ("a", 20)), ("a30", "30 ans", ("a", 30)),
]
COLONNES = [p[0] for p in PILIERS]
LIBELLES_PILIERS = {p[0]: p[1] for p in PILIERS}
RECUL_MAX = 10  # jours ouvrés essayés en reculant, comme le pricer
HISTORIQUE = Path(__file__).resolve().parent.parent / "data" / "historique_taux.csv"


# ── Historique ───────────────────────────────────────────────────────────────

def lire_historique(contenu: bytes, nom: str = "x.xlsx") -> pd.DataFrame:
    """Classeur « Historique des taux » (ou son export CSV) : une ligne par date."""
    if nom.lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(contenu), parse_dates=["date"])
        df["date"] = df["date"].dt.date
        return df.sort_values("date").reset_index(drop=True)
    brut = pd.read_excel(io.BytesIO(contenu), sheet_name=0, header=0)
    brut.columns = [str(c).strip() for c in brut.columns]
    renom = {brut.columns[0]: "date"}
    for col, lib, _ in PILIERS:
        cible = next((c for c in brut.columns if c.replace(" ", "").lower() == lib.replace(" ", "").lower()), None)
        if cible is None:
            raise ValueError(f"Colonne « {lib} » introuvable dans l'historique des taux.")
        renom[cible] = col
    td = next((c for c in brut.columns if "directeur" in c.lower()), None)
    if td:
        renom[td] = "taux_directeur"
    df = brut[list(renom)].rename(columns=renom)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    if "taux_directeur" not in df:
        df["taux_directeur"] = None
    for c in COLONNES + ["taux_directeur"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df.loc[df[c] > 1, c] = df.loc[df[c] > 1, c] / 100  # saisi en pourcentage
    return df.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)


def historique_du_depot() -> pd.DataFrame:
    if HISTORIQUE.exists():
        return lire_historique(HISTORIQUE.read_bytes(), HISTORIQUE.name)
    return pd.DataFrame(columns=["date", *COLONNES, "taux_directeur"])


# ── Courbe d'une date ────────────────────────────────────────────────────────

@dataclass
class Courbe:
    date_courbe: date
    date_base: date          # date à partir de laquelle les maturités sont comptées
    mt: list                 # maturités en jours
    tx: list                 # taux (décimal)
    source: str              # « Historique » ou « Site BAM »

    def taux(self, maturite_jours: float) -> float:
        return calcul_taux(maturite_jours, self.mt, self.tx, self.date_base)

    def piliers(self) -> list[dict]:
        out = []
        for _, lib, (u, n) in PILIERS:
            m = n if u == "j" else jours_ans(self.date_base, n)
            out.append({"pilier": lib, "taux": self.taux(m)})
        return out


class Courbes:
    """Donne la courbe à utiliser pour une date d'émission."""

    def __init__(self, histo: pd.DataFrame, bam: SourceBAM | None = None):
        self.histo = histo.set_index("date").sort_index() if len(histo) else histo
        self.dates_histo = set(self.histo.index) if len(histo) else set()
        self.bam = bam
        self._cache: dict[date, Courbe | None] = {}

    def __bool__(self) -> bool:
        return bool(self.dates_histo) or self.bam is not None

    def _depuis_histo(self, d: date) -> Courbe:
        ligne = self.histo.loc[d]
        mt, tx = [], []
        for col, _, (u, n) in PILIERS:
            if pd.notna(ligne[col]):
                mt.append(n if u == "j" else jours_ans(d, n))
                tx.append(float(ligne[col]))
        return Courbe(d, d, mt, tx, "Historique")

    def _candidats(self, d: date) -> list[date]:
        out, c = [], jour_ouvre(d)
        while len(out) < RECUL_MAX:
            out.append(c)
            c = jour_ouvre(c - timedelta(days=1))
        return out

    def courbe(self, d: date) -> Courbe | None:
        if d in self._cache:
            return self._cache[d]
        res = None
        for c in self._candidats(d):
            if c in self.dates_histo:
                res = self._depuis_histo(c)
                break
            if self.bam is not None:
                cb = self.bam.courbe(c)
                if cb is not None:
                    mt, tx = cb.maturites()
                    if len(mt) >= 2:
                        res = Courbe(c, cb.date_base, mt, tx, "Site BAM")
                        break
        self._cache[d] = res
        return res

    def preparer(self, dates, en_ligne: set | None = None, progression=None, delai_site: float | None = None) -> None:
        """Résout un lot de dates ; le site BAM n'est appelé que pour les dates de `en_ligne`,
        et pendant `delai_site` secondes au plus."""
        if self.bam is not None and delai_site is not None:
            self.bam.limite = time.time() + delai_site
        dates = sorted(set(dates) - set(self._cache))
        if self.bam is not None:
            hors = [c for d in dates for c in self._candidats(d) if c not in self.dates_histo]
            self.bam.precharger(sorted(set(hors)))
        hors_ligne = [d for d in dates if en_ligne is not None and d not in en_ligne]
        a_traiter = [d for d in dates if d not in hors_ligne]
        if self.bam is not None:
            etat = self.bam.en_ligne
            self.bam.en_ligne = False
            for d in hors_ligne:
                self.courbe(d)
            self.bam.en_ligne = etat
        with ThreadPoolExecutor(max_workers=2) as pool:
            for i, _ in enumerate(as_completed([pool.submit(self.courbe, d) for d in a_traiter]), 1):
                if progression:
                    progression(i, len(a_traiter))

    def taux(self, d0: date, maturite_jours: float):
        c = self.courbe(d0)
        if c is None or len(c.mt) < 2:
            return None, None, None
        return c.taux(maturite_jours), c.date_courbe, c.source
