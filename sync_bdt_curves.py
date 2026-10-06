"""
sync_bdt_curves.py — Pré-remplit le cache Supabase des courbes BDT BAM.

bkam.ma bloque (HTTP 403) les requêtes venant des IP datacenter de
Streamlit Community Cloud. L'app déployée ne peut donc jamais scraper
bkam.ma elle-même : elle ne peut lire que ce qui est déjà en cache
Supabase. Ce script fait le scraping depuis une machine non bloquée
(ta machine) et remplit Supabase à l'avance — l'app Cloud n'a ensuite
plus qu'à lire les courbes déjà en base.

Usage :
    python sync_bdt_curves.py --maroclear chemin/vers/fichier.xlsx --sheet OBL_ORDN
    python sync_bdt_curves.py --start 2010-01-01 --end 2026-12-31

À relancer périodiquement (ou à chaque nouveau fichier Maroclear) pour
couvrir les nouvelles dates d'émission. Les dates déjà en cache (local
ou Supabase) sont sautées automatiquement — le script est sûr à
interrompre et relancer.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

import pandas as pd

from bam_curve_fetcher import BamCurveFetcher


def _business_days(start: date, end: date) -> list[date]:
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri
            days.append(d)
        d += timedelta(days=1)
    return days


def _dates_from_maroclear(path: str, sheet: str) -> list[date]:
    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    if "ISSUEDT" not in df.columns:
        sys.exit(f"Colonne ISSUEDT introuvable dans la feuille '{sheet}'. "
                  f"Colonnes disponibles : {list(df.columns)}")
    dts = pd.to_datetime(df["ISSUEDT"], errors="coerce").dropna().dt.date
    return sorted(set(dts))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--maroclear", help="Fichier Maroclear .xlsx : ne synchronise que les dates ISSUEDT qu'il contient")
    p.add_argument("--sheet", default="OBL_ORDN", help="Nom de la feuille à lire (défaut: OBL_ORDN)")
    p.add_argument("--start", help="Date de début (YYYY-MM-DD) — synchronise tous les jours ouvrés de la période")
    p.add_argument("--end", help="Date de fin (YYYY-MM-DD), défaut: aujourd'hui")
    p.add_argument("--workers", type=int, default=10, help="Requêtes parallèles (défaut: 10)")
    args = p.parse_args()

    if args.maroclear:
        dates = _dates_from_maroclear(args.maroclear, args.sheet)
    elif args.start:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end) if args.end else date.today()
        dates = _business_days(start, end)
    else:
        p.error("Fournis soit --maroclear <fichier.xlsx>, soit --start (et optionnellement --end).")
        return

    print(f"{len(dates)} dates uniques a synchroniser "
          f"({dates[0]} -> {dates[-1]}).")

    fetcher = BamCurveFetcher(cache_dir="cache_bam_curves")

    def progress(done, total, n_cache, n_net, eta):
        print(f"\r  {done}/{total}  (cache: {n_cache} | reseau: {n_net})  ETA {eta:.0f}s   ", end="", flush=True)

    results = fetcher.get_curves_parallel(dates, max_workers=args.workers, progress_callback=progress)
    print()

    ok = sum(1 for v in results.values() if v)
    fail = sorted(d for d, v in results.items() if not v)
    print(f"\n[OK] {ok}/{len(dates)} courbes disponibles (cache local + Supabase).")
    if fail:
        print(f"[MANQUANT] {len(fail)} dates sans courbe BAM (jours feries/non ouvres probablement) :")
        print(", ".join(d.isoformat() for d in fail[:30]) + (" ..." if len(fail) > 30 else ""))


if __name__ == "__main__":
    main()
