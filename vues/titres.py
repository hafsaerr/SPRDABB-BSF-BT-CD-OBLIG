from datetime import date

import pandas as pd
import streamlit as st

from spreadabb import export
from vues.commun import entete, exiger_titres, fiche, titres


@st.cache_data(show_spinner=False, max_entries=10)
def _xlsx(df: pd.DataFrame) -> bytes:
    return export.classeur_titres(df)


entete("Univers des titres",
       "Référentiel Maroclear nettoyé : nature, type de taux, remboursement, coupons et révisions. "
       "Cliquer sur une ligne pour ouvrir la fiche du titre.")
exiger_titres()

h1, h2 = st.columns([3, 1])
q = h1.text_input("Rechercher", placeholder="ISIN, émetteur, libellé…", label_visibility="collapsed")
tous = h2.toggle("Inclure les titres radiés ou échus")
t = titres(tous)

with st.container(border=True):
    filtres = {}
    for cle, lib in [("famille", "Famille"), ("nature", "Nature"), ("type_taux", "Type de taux"),
                     ("remboursement", "Remboursement")]:
        valeurs = t[cle].dropna().value_counts().index.tolist()
        filtres[cle] = st.pills(lib, valeurs, selection_mode="multi", key=f"t_{cle}")

for cle, sel in filtres.items():
    if sel:
        t = t[t[cle].isin(sel)]
if q:
    m = t[["isin", "libelle", "emetteur", "secteur"]].fillna("").apply(lambda c: c.str.contains(q, case=False, regex=False))
    t = t[m.any(axis=1)]

a, b = st.columns([3, 1])
a.caption(f"{len(t):,} titres".replace(",", " "))
b.download_button("Exporter en Excel", _xlsx(t), f"Univers_des_titres_{date.today():%Y%m%d}.xlsx",
                  width="stretch", type="primary", icon=":material/download:",
                  help="Synthèse, tous les titres, puis une feuille par famille (sélection affichée)")
colonnes = ["isin", "libelle", "instrument", "nature", "type_taux", "remboursement", "emetteur", "secteur",
            "date_emission", "date_echeance", "maturite_residuelle_ans", "taux_facial", "periodicite",
            "prochain_coupon", "prochaine_revision", "spread_pb", "statut", "anomalies"]
ev = st.dataframe(
    t[colonnes], hide_index=True, height=620, width="stretch", on_select="rerun", selection_mode="single-row",
    column_config={
        "isin": st.column_config.TextColumn("ISIN", pinned=True), "libelle": st.column_config.TextColumn("Libellé", width="large"),
        "instrument": "Instrument", "nature": "Nature", "type_taux": "Taux", "remboursement": "Remboursement",
        "emetteur": "Émetteur", "secteur": "Secteur",
        "date_emission": st.column_config.DateColumn("Émission", format="DD/MM/YYYY"),
        "date_echeance": st.column_config.DateColumn("Échéance", format="DD/MM/YYYY"),
        "maturite_residuelle_ans": st.column_config.NumberColumn("Mat. résiduelle (ans)", format="%.2f"),
        "taux_facial": st.column_config.NumberColumn("Taux facial", format="percent"),
        "periodicite": "Coupon",
        "prochain_coupon": st.column_config.DateColumn("Prochain coupon", format="DD/MM/YYYY"),
        "prochaine_revision": st.column_config.DateColumn("Prochaine révision", format="DD/MM/YYYY"),
        "spread_pb": st.column_config.NumberColumn("Spread (pb)", format="%.1f"),
        "statut": "Statut", "anomalies": "Anomalies",
    })
if ev.selection.rows:
    fiche(t.iloc[ev.selection.rows[0]]["isin"])
