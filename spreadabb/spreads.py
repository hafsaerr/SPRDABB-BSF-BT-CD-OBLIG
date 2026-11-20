"""Spread d'émission de chaque titre, selon sa nature.

Spread (pb) = taux facial − taux BDT à la date d'émission, pour une durée qui dépend
du titre :
- taux fixe, in fine            : durée jusqu'à l'échéance ;
- taux fixe, amortissable       : durée de vie moyenne (amortissement constant) ;
- taux révisable                : période de révision (= périodicité du coupon) ;
- zéro-coupon ou taux fictif    : pas de spread ;
- bon du Trésor                 : pas de spread (c'est la courbe de référence).
Aucune borne n'est appliquée aux spreads obtenus.
"""
from __future__ import annotations

import math
from datetime import date

import pandas as pd
from dateutil.relativedelta import relativedelta

from . import referentiels as R
from .courbe import Courbes
from .maroclear import SEUIL_TAUX_FICTIF


def vie_moyenne_jours(d0: date, echeance: date, mois_periode: int) -> float:
    """Durée de vie moyenne d'un titre remboursé par parts égales à chaque période."""
    total = (echeance - d0).days
    if total <= 0:
        return 0.0
    n = max(1, round(total / (mois_periode * 30.4375)))
    dates = [min(d0 + relativedelta(months=mois_periode * k), echeance) for k in range(1, n + 1)]
    dates[-1] = echeance
    return sum((d - d0).days for d in dates) / n


def _code(v) -> str:
    return "" if v is None or pd.isna(v) else str(v)


def base_de_calcul(t) -> tuple[str | None, float | None, str | None]:
    """(méthode, durée en jours, motif d'exclusion) pour une ligne de titre."""
    d0, ech = t.date_emission, t.date_echeance
    if t.instrument == "BDT":
        return None, None, "Bon du Trésor : c'est la courbe de référence"
    if d0 is None or ech is None or pd.isna(d0) or pd.isna(ech) or (ech - d0).days <= 0:
        return None, None, "Dates incomplètes"
    if t.type_taux == "Zéro-coupon":
        return None, None, "Zéro-coupon (pas de taux facial)"
    if t.taux_facial is None or pd.isna(t.taux_facial):
        return None, None, "Taux facial manquant"
    if t.taux_facial < SEUIL_TAUX_FICTIF:
        return None, None, "Taux fictif"
    if t.type_taux == "Révisable":
        mois = R.MOIS_PAR_PERIODE.get(_code(t.periodicite_code), 12)
        jours = min(((d0 + relativedelta(months=mois)) - d0).days, (ech - d0).days)
        return f"Révisable : période de {mois} mois", jours, None
    if t.remboursement in ("Amortissable", "Par tranches"):
        mois = R.MOIS_PAR_PERIODE.get(_code(t.frequence_amort_code) or _code(t.periodicite_code), 12)
        return "Amortissable : vie moyenne", vie_moyenne_jours(d0, ech, mois), None
    return "Maturité jusqu'à l'échéance", (ech - d0).days, None


def calculer(titres: pd.DataFrame, courbes: Courbes) -> pd.DataFrame:
    lignes = []
    for t in titres.itertuples(index=False):
        methode, jours, motif = base_de_calcul(t)
        bdt = dc = src = None
        if motif is None:
            bdt, dc, src = courbes.taux(t.date_emission, jours)
            if bdt is None:
                motif = "Courbe introuvable (historique et site BAM)"
        spread = (t.taux_facial - bdt) * 10_000 if bdt is not None else None
        lignes.append({
            "isin": t.isin,
            "methode": methode,
            "duree_reference_ans": None if jours is None else round(jours / 365.25, 3),
            "date_courbe": dc,
            "source_courbe": src,
            "taux_bdt": bdt,
            "spread_pb": None if spread is None or math.isnan(spread) else round(spread, 2),
            "motif_exclusion": motif,
        })
    return pd.DataFrame(lignes)
