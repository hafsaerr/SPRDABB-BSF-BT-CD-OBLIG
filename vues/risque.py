import numpy as np
import plotly.graph_objects as go
import streamlit as st

from spreadabb.maroclear import ordre_tranche
from vues.commun import entete, exiger_titres, mise_en_page

entete("Risque de crédit implicite",
       "Probabilité de défaut déduite du spread moyen des titres en vie : "
       "λ = spread / (1 − R) et PD(T) = 1 − e<sup>−λT</sup>")
t = exiger_titres()
L = t[t["spread_pb"].notna() & (t["instrument"] != "BDT")].copy()
L["emetteur_court"] = L["emetteur"].str.replace(r"\s+E$", "", regex=True)

with st.container(border=True):
    c1, c2, c3 = st.columns(3)
    instruments = sorted(L["instrument"].unique())
    instr = c1.selectbox("Instrument", instruments, index=instruments.index("CD") if "CD" in instruments else 0)
    S = L[L["instrument"] == instr]
    tr = sorted(S["tranche_maturite"].dropna().unique(), key=ordre_tranche)
    top = S["tranche_maturite"].value_counts().index[0] if len(S) else None
    options = ["Toutes (durée moyenne)"] + tr
    tranche = c2.selectbox("Tranche", options, index=options.index(top) if top in options else 0)
    R = c3.slider("Taux de recouvrement", 0.0, 0.9, 0.65, 0.05, format="%.2f")

T = S if tranche.startswith("Toutes") else S[S["tranche_maturite"] == tranche]
g = T.groupby("emetteur_court").agg(spread=("spread_pb", "mean"), n=("spread_pb", "size"),
                                    duree=("duree_reference_ans", "mean"))
duree = g["duree"] if tranche.startswith("Toutes") else ordre_tranche(tranche)
g["pd"] = 1 - np.exp(-(g["spread"] / 1e4 / (1 - R)) * duree)
g = g.sort_values("pd")

a, b = st.columns(2)
with a.container(border=True):
    st.markdown("**Probabilité de défaut par émetteur**")
    fig = go.Figure(go.Bar(x=g["pd"] * 100, y=g.index, orientation="h", marker_color="#7f6c66",
                           text=[f"{v * 100:.2f} %".replace(".", ",") for v in g["pd"]], textposition="outside",
                           customdata=np.stack([g["spread"], g["n"]], axis=1),
                           hovertemplate="%{y}<br>PD %{x:.2f} %<br>Spread moyen %{customdata[0]:.1f} pb · "
                                         "%{customdata[1]} titre(s)<extra></extra>"))
    fig.update_xaxes(ticksuffix=" %")
    st.plotly_chart(mise_en_page(fig, max(420, 26 * len(g))), width="stretch")
with b.container(border=True):
    st.markdown("**Spread moyen (pb) : émetteur × tranche**")
    p = S.pivot_table(index="emetteur_court", columns="tranche_maturite", values="spread_pb", aggfunc="mean")
    p = p[sorted(p.columns, key=ordre_tranche)]
    vals = p.values[~np.isnan(p.values)]
    zmax = max(1.0, float(np.percentile(vals, 95))) if len(vals) else 1.0
    fig = go.Figure(go.Heatmap(z=p.values, x=p.columns, y=p.index, zmin=0, zmax=zmax, xgap=2, ygap=2,
                               colorscale=[[0, "#f3efe9"], [0.5, "#e0a100"], [1, "#8f1d2c"]],
                               hovertemplate="%{y} · %{x}<br>%{z:.1f} pb<extra></extra>",
                               colorbar=dict(thickness=10, ticksuffix=" pb")))
    st.plotly_chart(mise_en_page(fig, max(420, 26 * len(p))), width="stretch")
