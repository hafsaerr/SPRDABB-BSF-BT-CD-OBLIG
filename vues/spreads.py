from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from spreadabb import export
from spreadabb.maroclear import ordre_tranche
from vues.commun import couleur, entete, exiger_titres, fiche, kpis, mise_en_page, nb, titres

entete("Spreads d'émission",
       "Taux facial − taux BDT à la date d'émission, selon la nature de chaque titre. Aucune borne n'est appliquée.")
exiger_titres()

TYPES = {"Obligations": "Obligations", "CD": "Certificats de dépôt", "BSF": "Bons de sociétés de financement",
         "BT": "Billets de trésorerie"}
a, b = st.columns([3, 1])
choix = a.segmented_control("Type de titre", list(TYPES), default="Obligations", format_func=lambda k: TYPES[k],
                            key="s_type") or "Obligations"
tous = b.toggle("Inclure les titres échus")
base = titres(tous)
base = base[base["famille"].eq("Obligations")] if choix == "Obligations" else base[base["instrument"].eq(choix)]
base = base[base["spread_pb"].notna()].copy()
base["banque"] = base["banque"].map({True: "Oui", False: "Non"})
if choix != "Obligations":
    st.caption(f"{TYPES[choix]} : taux facial − taux BDT de même maturité (de l'émission à l'échéance), sur la courbe "
               "de la date d'émission ; taux monétaire en dessous d'un an, comme la courbe de Bank Al-Maghrib. "
               "Pour un titre révisable, la maturité retenue est la période de révision.")

with st.container(border=True):
    sel = {}
    filtres = [("type_taux", "Type de taux"), ("remboursement", "Remboursement"), ("banque", "Émetteur bancaire")]
    if choix == "Obligations":
        filtres = [("instrument", "Instrument"), ("nature", "Nature")] + filtres
    for cle, lib in filtres:
        sel[cle] = st.pills(lib, base[cle].value_counts().index.tolist(), selection_mode="multi", key=f"s_{choix}_{cle}")
    tranches = sorted(base["tranche_maturite"].dropna().unique(), key=ordre_tranche)
    sel["tranche_maturite"] = st.pills("Tranche de maturité", tranches, selection_mode="multi", key=f"s_{choix}_tranche")
    c1, c2, c3 = st.columns([2, 1, 1])
    secteurs = c1.multiselect("Secteur", sorted(base["secteur"].dropna().unique()), placeholder="Tous",
                              key=f"s_{choix}_secteur")
    emetteurs = c1.multiselect("Émetteur", sorted(base["emetteur"].dropna().unique()), placeholder="Tous",
                               key=f"s_{choix}_emetteur")
    du = c2.date_input("Émis à partir du", value=None, format="DD/MM/YYYY")
    au = c3.date_input("Émis jusqu'au", value=None, format="DD/MM/YYYY")

L = base
for cle, v in sel.items():
    if v:
        L = L[L[cle].isin(v)]
if secteurs:
    L = L[L["secteur"].isin(secteurs)]
if emetteurs:
    L = L[L["emetteur"].isin(emetteurs)]
if du:
    L = L[L["date_emission"] >= du]
if au:
    L = L[L["date_emission"] <= au]

s = L["spread_pb"]
kpis([("Titres", nb(len(L)), ""), ("Spread moyen", f"{nb(s.mean(), 1)} pb", ""), ("Médian", f"{nb(s.median(), 1)} pb", ""),
      ("Minimum", f"{nb(s.min(), 1)} pb", ""), ("Maximum", f"{nb(s.max(), 1)} pb", "")])
if L.empty:
    st.info("Aucun titre pour ces filtres.")
    st.stop()

g, d = st.columns(2)
with g.container(border=True):
    a, b = st.columns([1, 1])
    a.markdown("**Spread selon la durée de référence**")
    options = ["Instrument", "Nature", "Taux", "Remb."] if choix == "Obligations" else ["Taux", "Remb.", "Banque"]
    par = b.segmented_control("Couleur", options, default=options[0], label_visibility="collapsed",
                              key=f"s_{choix}_couleur") or options[0]
    cle = {"Instrument": "instrument", "Nature": "nature", "Taux": "type_taux", "Remb.": "remboursement",
           "Banque": "banque"}[par]
    fig = go.Figure()
    for i, (k, grp) in enumerate(L.groupby(cle)):
        fig.add_scatter(x=grp["duree_reference_ans"], y=grp["spread_pb"], mode="markers", name=k,
                        customdata=grp["isin"], text=grp["libelle"] + "<br>" + grp["methode"],
                        marker=dict(color=couleur(k, i), size=8, opacity=0.8),
                        hovertemplate="%{text}<br>Durée %{x:.2f} an(s) · <b>%{y:.1f} pb</b><extra></extra>")
    fig.update_xaxes(type="log", title="Durée de référence (ans), échelle logarithmique",
                     tickvals=[0.25, 0.5, 1, 2, 3, 5, 7, 10, 15, 20, 30],
                     ticktext=["3m", "6m", "1", "2", "3", "5", "7", "10", "15", "20", "30"])
    fig.update_yaxes(title="Spread (pb)")
    ev = st.plotly_chart(mise_en_page(fig, 420, legend=dict(orientation="h", y=-0.3)), width="stretch",
                         on_select="rerun", selection_mode="points", key="nuage")
    if ev and ev.selection.points:
        fiche(ev.selection.points[0]["customdata"])
with d.container(border=True):
    st.markdown("**Dispersion par tranche de maturité**")
    fig = go.Figure()
    for i, (k, grp) in enumerate(L.groupby("instrument" if choix == "Obligations" else "type_taux")):
        fig.add_box(x=grp["tranche_maturite"], y=grp["spread_pb"], name=k, marker=dict(color=couleur(k, i), size=4),
                    boxpoints="outliers", line=dict(width=1.4))
    ordre = sorted(L["tranche_maturite"].unique(), key=ordre_tranche)
    fig.update_layout(boxmode="group")
    fig.update_xaxes(categoryorder="array", categoryarray=ordre)
    fig.update_yaxes(title="Spread (pb)")
    st.plotly_chart(mise_en_page(fig, 470, legend=dict(orientation="h", y=-0.25)), width="stretch")

with st.container(border=True):
    ligne = "instrument" if choix == "Obligations" else "emetteur"
    st.markdown(f"**Spread moyen (pb) par {'instrument' if ligne == 'instrument' else 'émetteur'} et tranche de "
                "maturité** · nombre de titres entre parenthèses")
    moy = L.pivot_table(index=ligne, columns="tranche_maturite", values="spread_pb", aggfunc="mean")
    cpt = L.pivot_table(index=ligne, columns="tranche_maturite", values="spread_pb", aggfunc="count")
    ordre = [c for c in sorted(moy.columns, key=ordre_tranche)]
    tab = pd.DataFrame({c: [f"{nb(m, 1)} ({int(n)})" if pd.notna(m) else "" for m, n in zip(moy[c], cpt[c])]
                        for c in ordre}, index=moy.index)
    st.dataframe(tab, width="stretch")

with st.container(border=True):
    a, b = st.columns([3, 1])
    a.markdown("**Détail** · cliquer sur une ligne pour la fiche du titre")
    b.download_button("Exporter en Excel", export.classeur(L), f"Spreads_{date.today():%Y%m%d}.xlsx",
                      width="stretch", type="primary", icon=":material/download:")
    col = ["isin", "libelle", "instrument", "nature", "type_taux", "remboursement", "emetteur", "date_emission",
           "date_echeance", "tranche_maturite", "methode", "duree_reference_ans", "taux_facial", "taux_bdt",
           "spread_pb", "source_courbe"]
    ev = st.dataframe(L[col], hide_index=True, height=520, width="stretch", on_select="rerun",
                      selection_mode="single-row", column_config={
        "isin": st.column_config.TextColumn("ISIN", pinned=True), "libelle": "Libellé", "instrument": "Instrument",
        "nature": "Nature", "type_taux": "Taux", "remboursement": "Remb.", "emetteur": "Émetteur",
        "date_emission": st.column_config.DateColumn("Émission", format="DD/MM/YYYY"),
        "date_echeance": st.column_config.DateColumn("Échéance", format="DD/MM/YYYY"),
        "tranche_maturite": "Tranche", "methode": "Base de calcul",
        "duree_reference_ans": st.column_config.NumberColumn("Durée réf. (ans)", format="%.2f"),
        "taux_facial": st.column_config.NumberColumn("Taux facial", format="percent"),
        "taux_bdt": st.column_config.NumberColumn("Taux BDT", format="percent"),
        "spread_pb": st.column_config.NumberColumn("Spread (pb)", format="%.1f"),
        "source_courbe": "Source de la courbe"})
    if ev.selection.rows:
        fiche(L.iloc[ev.selection.rows[0]]["isin"])
