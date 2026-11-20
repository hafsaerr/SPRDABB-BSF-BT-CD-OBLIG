"""Courbe des taux de référence des BDT publiée par Bank Al-Maghrib.

Même procédure que le pricer (module Courbe_BAM) :
- page BAM avec ?date=jj/mm/aaaa&block=... ;
- lignes « Date d'échéance | Transaction | Taux moyen pondéré | Date de la valeur » ;
- date de base = date de valeur la plus récente du tableau ;
- maturité de chaque point = échéance − date de base.
Cache : mémoire, disque, puis table Supabase « bdt_curves ».
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

URL = ("https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/"
       "Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor")
BLOC = "e1d6b9bbf87f86f8ba53e8518e882982"
ENTETES = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
}
# Courbes déjà téléchargées, conservées dans le dépôt (le site BAM refuse les serveurs d'hébergement)
DOSSIER_CACHE = Path(os.environ.get("SPREADABB_CACHE",
                                    Path(__file__).resolve().parent.parent / "data" / "courbes_bam"))
JOURS_RECENTS = 7  # une date récente sans publication n'est pas mémorisée comme « vide »

_RE_LIGNE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_RE_CELLULE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
_RE_DATE = re.compile(r"^\d{2}/\d{2}/\d{4}$")


@dataclass
class CourbeBAM:
    date_courbe: date
    date_base: date                    # date de valeur la plus récente
    points: list[tuple[date, float]]   # (échéance, taux moyen pondéré décimal), triés

    def maturites(self) -> tuple[list[int], list[float]]:
        # Deux lignes de même échéance : moyenne des taux
        par_jour: dict[int, list[float]] = {}
        for ech, t in self.points:
            j = (ech - self.date_base).days
            if j > 0:
                par_jour.setdefault(j, []).append(t)
        mt = sorted(par_jour)
        return mt, [sum(par_jour[j]) / len(par_jour[j]) for j in mt]

    def to_json(self) -> dict:
        return {"format": 2, "date_valeur": self.date_base.isoformat(),
                "points": [[e.isoformat(), t] for e, t in self.points]}

    @staticmethod
    def from_json(d_courbe: date, js: dict) -> "CourbeBAM | None":
        if not js or js.get("format") != 2 or not js.get("points"):
            return None
        return CourbeBAM(d_courbe, date.fromisoformat(js["date_valeur"]),
                         [(date.fromisoformat(e), float(t)) for e, t in js["points"]])


class SiteInjoignable(Exception):
    """Le site de BAM refuse la connexion (ex. HTTP 403 depuis un hébergeur)."""


# ── Lecture de la page ───────────────────────────────────────────────────────

def _texte(html: str) -> str:
    t = re.sub(r"<[^>]+>", " ", html).replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", t).strip()


def _nombre(s: str) -> float:
    return float(s.replace("%", "").replace(" ", "").replace(" ", "").replace(",", "."))


def lire_page(html: str, d_courbe: date) -> CourbeBAM | None:
    lignes = []
    for tr in _RE_LIGNE.findall(html):
        c = [_texte(x) for x in _RE_CELLULE.findall(tr)]
        if len(c) >= 4 and _RE_DATE.match(c[0]) and _RE_DATE.match(c[3]) and "%" in c[2]:
            lignes.append((datetime.strptime(c[0], "%d/%m/%Y").date(), _nombre(c[2]) / 100,
                           datetime.strptime(c[3], "%d/%m/%Y").date()))
    if not lignes:
        return None
    base = max(l[2] for l in lignes)
    # Garde-fou : BAM peut renvoyer une autre date que celle demandée
    if abs((base - d_courbe).days) > 10:
        return None
    lignes.sort(key=lambda l: l[0])
    return CourbeBAM(d_courbe, base, [(l[0], l[1]) for l in lignes])


class Lenteur(Exception):
    """Le site ne répond pas à temps : la date pourra être retentée plus tard."""


def telecharger(d: date, timeout: int = 30, essais: int = 3) -> CourbeBAM | None:
    derniere = None
    for k in range(essais):
        try:
            r = requests.get(URL, params={"date": d.strftime("%d/%m/%Y"), "block": BLOC},
                             headers=ENTETES, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            derniere = e
            time.sleep(2 * (k + 1))
            continue
        if r.status_code in (401, 403, 429):
            raise SiteInjoignable(f"HTTP {r.status_code}")
        if r.status_code >= 500:
            derniere = f"HTTP {r.status_code}"
            time.sleep(2 * (k + 1))
            continue
        r.raise_for_status()
        return lire_page(r.text, d)
    raise Lenteur(str(derniere))


# ── Supabase ─────────────────────────────────────────────────────────────────

def _identifiants() -> tuple[str, str] | None:
    url, cle = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if url and cle:
        return url, cle
    try:
        import streamlit as st
        return st.secrets["supabase"]["url"], st.secrets["supabase"]["key"]
    except Exception:
        return None


class _Supabase:
    def __init__(self):
        self.client = None
        ids = _identifiants()
        if ids:
            try:
                from supabase import create_client
                self.client = create_client(*ids)
            except Exception as e:
                print(f"[BAM-DIAG] Supabase indisponible : {e}", flush=True)

    def lire(self, dates: list[date]) -> dict:
        if not self.client or not dates:
            return {}
        out = {}
        try:
            for i in range(0, len(dates), 200):
                bloc = [d.isoformat() for d in dates[i:i + 200]]
                r = (self.client.table("bdt_curves").select("date_emission,courbe_json")
                     .in_("date_emission", bloc).execute())
                for ligne in r.data:
                    out[date.fromisoformat(ligne["date_emission"])] = ligne["courbe_json"]
        except Exception as e:
            print(f"[BAM-DIAG] lecture Supabase : {e}", flush=True)
        return out

    def ecrire(self, d: date, js: dict) -> None:
        if not self.client:
            return
        try:
            self.client.table("bdt_curves").upsert({"date_emission": d.isoformat(), "courbe_json": js}).execute()
        except Exception as e:
            print(f"[BAM-DIAG] écriture Supabase : {e}", flush=True)


# ── Source unique, avec cache ────────────────────────────────────────────────

VIDE = {"format": 2, "points": []}  # BAM n'a rien publié ce jour-là


class SourceBAM:
    def __init__(self, en_ligne: bool = True):
        self.memoire: dict[date, dict] = {}
        self.supabase = _Supabase()
        self.en_ligne = en_ligne
        self.bloque: str | None = None    # motif si le site refuse la connexion
        self.telechargees = 0
        self.echecs = 0
        self._verrou = threading.Lock()
        DOSSIER_CACHE.mkdir(exist_ok=True)

    def _disque(self, d: date) -> Path:
        return DOSSIER_CACHE / f"{d.isoformat()}.json"

    def precharger(self, dates: list[date]) -> None:
        """Lit en une fois le disque et Supabase pour une liste de dates."""
        manque = []
        for d in dates:
            if d in self.memoire:
                continue
            f = self._disque(d)
            if f.exists():
                try:
                    self.memoire[d] = json.loads(f.read_text("utf-8"))
                    continue
                except ValueError:
                    f.unlink(missing_ok=True)
            manque.append(d)
        for d, js in self.supabase.lire(manque).items():
            if isinstance(js, dict) and js.get("format") == 2:
                self.memoire[d] = js
                self._disque(d).write_text(json.dumps(js), "utf-8")

    def courbe(self, d: date) -> CourbeBAM | None:
        if d not in self.memoire:
            self.precharger([d])
        if d in self.memoire:
            return CourbeBAM.from_json(d, self.memoire[d])
        if not self.en_ligne or self.bloque:
            return None
        try:
            c = telecharger(d)
        except SiteInjoignable as e:
            self.bloque = str(e)
            return None
        except Lenteur:
            with self._verrou:
                self.echecs += 1
                # Trop d'échecs successifs : on considère le site injoignable pour cette session
                if self.echecs >= 5:
                    self.bloque = "le site ne répond pas"
            return None
        self.echecs = 0
        with self._verrou:
            self.telechargees += 1
        js = c.to_json() if c else VIDE
        if c or (date.today() - d).days > JOURS_RECENTS:
            self.memoire[d] = js
            self._disque(d).write_text(json.dumps(js), "utf-8")
            self.supabase.ecrire(d, js)
        return c


def jour_ouvre(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d
