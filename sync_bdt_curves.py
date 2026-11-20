"""Complète le cache Supabase des courbes BAM depuis un poste qui accède au site de Bank Al-Maghrib.

Le site de BAM refuse les connexions venant des serveurs d'hébergement : l'application en ligne
lit donc les courbes déjà enregistrées dans Supabase. Ce script télécharge les dates absentes de
l'historique des taux (dates d'émission du fichier Maroclear, ou une période) et les enregistre.

Exemples :
    python sync_bdt_curves.py --maroclear REPV_0001_ALL_13112026.zip
    python sync_bdt_curves.py --du 2008-01-01 --au 2014-12-31
Identifiants : .streamlit/secrets.toml ([supabase] url, key) ou variables SUPABASE_URL / SUPABASE_KEY.
"""
from __future__ import annotations

import argparse
import os
import time
import tomllib
from datetime import date, timedelta
from pathlib import Path

from spreadabb import maroclear, spreads
from spreadabb.bam import SiteInjoignable, SourceBAM, jour_ouvre
from spreadabb.courbe import Courbes, historique_du_depot

PAUSE_TOUS_LES = 150   # le site coupe après quelques centaines de requêtes
PAUSE_SECONDES = 10


def _identifiants() -> None:
    f = Path(__file__).parent / ".streamlit" / "secrets.toml"
    if not os.environ.get("SUPABASE_URL") and f.exists():
        s = tomllib.loads(f.read_text("utf-8")).get("supabase", {})
        os.environ.setdefault("SUPABASE_URL", s.get("url", ""))
        os.environ.setdefault("SUPABASE_KEY", s.get("key", ""))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--maroclear", help="fichier REPV_….zip : dates d'émission des titres")
    p.add_argument("--tous", action="store_true", help="avec --maroclear : inclure les titres échus")
    p.add_argument("--du", type=date.fromisoformat)
    p.add_argument("--au", type=date.fromisoformat)
    a = p.parse_args()
    _identifiants()

    if a.maroclear:
        titres, _, _ = maroclear.charger(Path(a.maroclear).name, Path(a.maroclear).read_bytes())
        if not a.tous:
            titres = titres[titres["en_vie"]]
        dates = {t.date_emission for t in titres.itertuples() if spreads.base_de_calcul(t)[2] is None}
    elif a.du and a.au:
        dates, d = set(), a.du
        while d <= a.au:
            if d.weekday() < 5:
                dates.add(d)
            d += timedelta(days=1)
    else:
        p.error("indiquer --maroclear ou --du et --au")

    bam = SourceBAM()
    if bam.supabase.client is None:
        print("Supabase non configuré : les courbes sont enregistrées dans data/courbes_bam/ "
              "(à envoyer sur GitHub pour que l'application en ligne les utilise).")
    courbes = Courbes(historique_du_depot(), bam)
    a_chercher = sorted(d for d in dates if jour_ouvre(d) not in courbes.dates_histo)
    print(f"{len(dates)} dates, dont {len(a_chercher)} absentes de l'historique")
    bam.precharger(a_chercher)
    for i, d in enumerate(a_chercher, 1):
        if i % PAUSE_TOUS_LES == 0:
            time.sleep(PAUSE_SECONDES)
        try:
            c = courbes.courbe(d)
        except SiteInjoignable as e:
            print(f"Site BAM injoignable : {e}")
            break
        if bam.bloque:
            print(f"Site BAM injoignable : {bam.bloque}")
            break
        print(f"[{i}/{len(a_chercher)}] {d} -> " + (f"courbe du {c.date_courbe} ({c.source})" if c else "aucune courbe"))
    print(f"Terminé : {bam.telechargees} page(s) téléchargée(s).")


if __name__ == "__main__":
    main()
