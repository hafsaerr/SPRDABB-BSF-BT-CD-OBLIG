"""Interpolation d'un taux sur la courbe BDT, selon la méthode de Bank Al-Maghrib.

Taux monétaires (base 360) jusqu'à un an, taux actuariels au-delà ; le segment
qui chevauche un an passe par une conversion monétaire -> actuariel.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Sequence

from dateutil.relativedelta import relativedelta


def _en_date(v: date | datetime) -> date:
    return v.date() if isinstance(v, datetime) else v


def _base_annee(d: date) -> int:
    """Nombre de jours de l'année qui précède d (365 ou 366)."""
    return (d - (d + relativedelta(years=-1))).days


def jours_ans(d0: date, n: int) -> int:
    return ((d0 + relativedelta(years=n)) - d0).days


def _lin(x, x0, y0, x1, y1):
    return y0 if x1 == x0 else (x - x0) * (y1 - y0) / (x1 - x0) + y0


def calcul_taux(maturite: float, mt: Sequence[float], tx: Sequence[float], date_debut: date | datetime) -> float:
    """Taux BDT pour une maturité en jours, à partir des points (mt jours, tx décimal)."""
    if len(mt) != len(tx) or len(mt) < 2:
        raise ValueError("La courbe doit contenir au moins deux points.")
    d0 = _en_date(date_debut)
    un_an = jours_ans(d0, 1)
    n = len(mt) - 1

    if maturite <= mt[0]:
        return tx[0]
    if maturite > mt[n]:
        return _lin(maturite, mt[n - 1], tx[n - 1], mt[n], tx[n])

    for i in range(n):
        b, a = mt[i], mt[i + 1]
        if not (b <= maturite <= a):
            continue
        if a <= un_an or b > un_an:
            return _lin(maturite, b, tx[i], a, tx[i + 1])
        if maturite > un_an:
            base = _base_annee(d0 + timedelta(days=int(b)))
            actu_b = (1 + tx[i] * b / 360) ** (base / b) - 1
            return _lin(maturite, b, actu_b, a, tx[i + 1])
        base = _base_annee(d0 + timedelta(days=int(a)))
        mon_a = (360 / a) * ((1 + tx[i + 1]) ** (a / base) - 1)
        return _lin(maturite, b, tx[i], a, mon_a)
    return tx[n]
