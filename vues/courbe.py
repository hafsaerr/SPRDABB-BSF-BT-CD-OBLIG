from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from spreadabb.courbe import Courbes, LIBELLES_PILIERS
from vues.commun import entete, historique, mise_en_page, nb, source_bam

entete("Courbe des taux BDT",
       "Historique des taux pour les dates connues ; pour une date absente, la courbe est téléchargée sur le site "
       "de Bank Al-Maghrib et interpolée comme dans le pricer.")
h = historique()
courbes = st.session_state.get("courbes") or Courbes(h, source_bam())
ANS = [0.25, 0.5, 1, 2, 5, 10, 15, 20, 30]
TICKS = ["13s", "26s", "52s", "2a", "5a", "10a", "15a", "20a", "30a"]

c1, c2, c3 = st.columns([1, 1, 2])
d1 = c1.date_input("Date", value=h["date"].iloc[-1] if len(h) else date.today(), format="DD/MM/YYYY")
d2 = c2.date_input("Comparer au", value=d1 - timedelta(days=365), format="DD/MM/YYYY")
c3.caption(f"Historique : {nb(len(h))} dates, du {h['date'].iloc[0]:%d/%m/%Y} au {h['date'].iloc[-1]:%d/%m/%Y}"
           if len(h) else "Historique absent")

with st.spinner("Recherche des courbes…"):
    k1, k2 = courbes.courbe(d1), courbes.courbe(d2)
bam = source_bam()
if bam.bloque and (k1 is None or k2 is None):
    st.warning(f"Le site de Bank Al-Maghrib refuse la connexion depuis ce serveur ({bam.bloque}). "
               "Les dates absentes de l'historique et du cache ne peuvent pas être téléchargées ici : "
               "lancer sync_bdt_curves.py depuis un poste du bureau pour les ajouter au cache.")

g, d = st.columns([1.3, 1])
with g.container(border=True):
    st.markdown("**Courbe à la date choisie**")
    fig = go.Figure()
    for k, style, coul in [(k1, "solid", "#231e1c"), (k2, "dot", "#7f6c66")]:
        if k:
            fig.add_scatter(x=ANS, y=[p["taux"] * 100 for p in k.piliers()], mode="lines+markers",
                            name=f"{k.date_courbe:%d/%m/%Y} ({k.source})", line=dict(dash=style, color=coul, width=2.4),
                            text=TICKS, hovertemplate="%{text} : %{y:.3f} %<extra></extra>")
    fig.update_xaxes(type="log", tickvals=ANS, ticktext=TICKS)
    fig.update_yaxes(ticksuffix=" %")
    st.plotly_chart(mise_en_page(fig, 380), width="stretch")
with d.container(border=True):
    st.markdown("**Taux et variation (pb)**")
    if k1:
        p1 = k1.piliers()
        p2 = k2.piliers() if k2 else [None] * len(p1)
        tab = pd.DataFrame({
            "Maturité": [p["pilier"] for p in p1],
            f"{k1.date_courbe:%d/%m/%Y}": [p["taux"] for p in p1],
            (f"{k2.date_courbe:%d/%m/%Y}" if k2 else "Comparaison"): [q["taux"] if q else None for q in p2],
        })
        tab["Variation (pb)"] = (tab.iloc[:, 1] - tab.iloc[:, 2]) * 1e4
        st.dataframe(tab, hide_index=True, width="stretch", column_config={
            tab.columns[1]: st.column_config.NumberColumn(format="percent"),
            tab.columns[2]: st.column_config.NumberColumn(format="percent"),
            "Variation (pb)": st.column_config.NumberColumn(format="%+.1f")})
    else:
        st.info("Aucune courbe trouvée pour cette date.")

if k1 and k1.source == "Site BAM":
    with st.expander("Points publiés par Bank Al-Maghrib"):
        st.dataframe(pd.DataFrame({"Maturité (jours)": k1.mt, "Taux moyen pondéré": k1.tx}), hide_index=True,
                     column_config={"Taux moyen pondéré": st.column_config.NumberColumn(format="percent")})

with st.container(border=True):
    st.markdown("**Historique par maturité**")
    choix = st.pills("Maturités", list(LIBELLES_PILIERS.values()) + ["Taux directeur"], selection_mode="multi",
                     default=["52s", "5 ans", "10 ans"], label_visibility="collapsed")
    fig = go.Figure()
    inv = {v: k for k, v in LIBELLES_PILIERS.items()} | {"Taux directeur": "taux_directeur"}
    for i, lib in enumerate(choix or []):
        col = inv[lib]
        ligne = dict(width=1.6, shape="hv", dash="dash", color="#7f6c66") if col == "taux_directeur" else dict(width=1.6)
        fig.add_scatter(x=h["date"], y=h[col] * 100, name=lib, mode="lines", line=ligne)
    fig.update_yaxes(ticksuffix=" %")
    fig.update_xaxes(rangeslider=dict(visible=True, thickness=0.06), rangeselector=dict(buttons=[
        dict(count=1, step="month", label="1M"), dict(count=6, step="month", label="6M"),
        dict(count=1, step="year", label="1A"), dict(count=5, step="year", label="5A"), dict(step="all", label="Tout")]))
    st.plotly_chart(mise_en_page(fig, 480, hovermode="x unified", legend=dict(orientation="h", y=1.1)),
                    width="stretch")
