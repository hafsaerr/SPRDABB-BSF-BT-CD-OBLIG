from datetime import date, timedelta

import plotly.graph_objects as go
import streamlit as st

from vues.commun import couleur, entete, exiger_titres, fiche, historique, kpis, mise_en_page, nb

entete("Tableau de bord", "Vue d'ensemble des titres en vie")
t = exiger_titres()

oblig = t[t["famille"].isin(["Obligations", "Titrisation"])]
s = oblig["spread_pb"].dropna()
kpis([
    ("Titres en vie", nb(len(t)), f"{nb((t['anomalies'] != '').sum())} avec anomalie"),
    ("Obligations et titrisations", nb(len(oblig)),
     f"{nb((t['famille'] == 'TCN').sum())} TCN · {nb((t['famille'] == 'Trésor').sum())} BDT"),
    ("Taux révisable", nb((t["type_taux"] == "Révisable").sum()), "révision à chaque coupon"),
    ("Amortissables", nb((t["remboursement"] == "Amortissable").sum()), "les autres sont in fine"),
    ("Subordonnées", nb(t["nature"].str.startswith("Subordonnée").sum()),
     f"dont {nb((t['nature'] == 'Subordonnée perpétuelle').sum())} perpétuelles"),
    ("Spread moyen des obligations", f"{nb(s.mean(), 1)} pb", f"{nb(len(s))} titres"),
])

g, d = st.columns(2)
with g.container(border=True):
    st.markdown("**Répartition par nature**")
    n = t["nature"].value_counts()
    fig = go.Figure(go.Pie(labels=n.index, values=n.values, hole=0.62, sort=False, textinfo="value",
                           marker=dict(colors=[couleur(x, i) for i, x in enumerate(n.index)], line=dict(color="#fff", width=2))))
    st.plotly_chart(mise_en_page(fig, 320, legend=dict(orientation="v", x=1, y=0.5)), width="stretch")
with d.container(border=True):
    st.markdown("**Encours émis en vie par secteur** (Md MAD, hors État)")
    sec = t[t["secteur"] != "ÉTAT"].groupby("secteur")["montant_emis"].sum().sort_values().tail(14) / 1e9
    fig = go.Figure(go.Bar(x=sec.values, y=sec.index, orientation="h", marker_color="#7f6c66",
                           hovertemplate="%{y}<br>%{x:.2f} Md MAD<extra></extra>"))
    st.plotly_chart(mise_en_page(fig, 320), width="stretch")

g, d = st.columns(2)
with g.container(border=True):
    st.markdown("**Courbe BDT : dernière date et il y a un an**")
    h = historique()
    if len(h):
        ans = [0.25, 0.5, 1, 2, 5, 10, 15, 20, 30]
        cols = ["s13", "s26", "s52", "a2", "a5", "a10", "a15", "a20", "a30"]
        der = h.iloc[-1]
        avant = h[h["date"] <= der["date"] - timedelta(days=365)].iloc[-1]
        fig = go.Figure()
        for ligne, style, c in [(der, "solid", "#231e1c"), (avant, "dot", "#7f6c66")]:
            fig.add_scatter(x=ans, y=[ligne[k] * 100 for k in cols], name=ligne["date"].strftime("%d/%m/%Y"),
                            mode="lines+markers", line=dict(dash=style, color=c, width=2.4))
        fig.update_xaxes(type="log", tickvals=ans, ticktext=["13s", "26s", "52s", "2a", "5a", "10a", "15a", "20a", "30a"])
        fig.update_yaxes(ticksuffix=" %")
        st.plotly_chart(mise_en_page(fig, 320), width="stretch")
with d.container(border=True):
    st.markdown("**Échéancier des 30 prochains jours**")
    choix = st.segmented_control("Événement", ["Coupons", "Révisions de taux", "Échéances"], default="Coupons",
                                 label_visibility="collapsed")
    cle = {"Coupons": "prochain_coupon", "Révisions de taux": "prochaine_revision", "Échéances": "date_echeance"}[choix or "Coupons"]
    auj = date.today()
    a = t[t[cle].notna()]
    a = a[(a[cle] >= auj) & (a[cle] <= auj + timedelta(days=30))].sort_values(cle)
    ev = st.dataframe(
        a[[cle, "libelle", "nature", "taux_facial", "isin"]], hide_index=True, height=290, width="stretch",
        on_select="rerun", selection_mode="single-row",
        column_config={cle: st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                       "libelle": "Titre", "nature": "Nature", "isin": None,
                       "taux_facial": st.column_config.NumberColumn("Taux", format="percent")})
    if ev.selection.rows:
        fiche(a.iloc[ev.selection.rows[0]]["isin"])
