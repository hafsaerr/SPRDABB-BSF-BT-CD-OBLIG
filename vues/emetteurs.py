import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from vues.commun import couleur, entete, exiger_titres, fiche, kpis, mise_en_page, nb

entete("Émetteurs",
       "Encours en circulation de chaque émetteur (nominal actuel × nombre de titres) et instruments utilisés")
t = exiger_titres().copy()

with st.container(border=True):
    a, b = st.columns([3, 1])
    familles = a.pills("Instruments", ["Obligations", "TCN", "Trésor"], selection_mode="multi",
                       default=["Obligations", "TCN"], key="e_familles")
    banques = b.toggle("Émetteurs bancaires seulement", key="e_banques")
    secteurs = st.multiselect("Secteur", sorted(t["secteur"].dropna().unique()), placeholder="Tous", key="e_secteurs")
if familles:
    t = t[t["famille"].isin(familles)]
if banques:
    t = t[t["banque"]]
if secteurs:
    t = t[t["secteur"].isin(secteurs)]
if t.empty:
    st.info("Aucun titre pour ces filtres.")
    st.stop()

t["poids"] = t["encours"] * t["maturite_residuelle_ans"]
t["spread_x"] = t["encours"].where(t["spread_pb"].notna()) * t["spread_pb"]
g = t.groupby("emetteur").agg(
    secteur=("secteur", "first"), encours=("encours", "sum"), titres=("isin", "count"),
    instruments=("instrument", lambda s: ", ".join(sorted(s.unique()))),
    poids=("poids", "sum"), spread_x=("spread_x", "sum"),
    encours_spread=("spread_x", lambda s: t.loc[s.index, "encours"][s.notna()].sum()),
    prochaine_echeance=("date_echeance", "min"),
).sort_values("encours", ascending=False)
g["part"] = g["encours"] / g["encours"].sum()
g["maturite_res"] = g["poids"] / g["encours"]
g["spread_moyen"] = (g["spread_x"] / g["encours_spread"]).where(g["encours_spread"] > 0)
par_instr = t.pivot_table(index="emetteur", columns="instrument", values="encours", aggfunc="sum").fillna(0)

top = g.iloc[0]
kpis([
    ("Émetteurs", nb(len(g)), f"{nb(len(t))} titres en vie"),
    ("Encours total", f"{nb(g['encours'].sum() / 1e9, 1)} Md MAD", ""),
    ("Premier émetteur", top.name[:28], f"{nb(top['part'] * 100, 1)} % de l'encours"),
    ("5 premiers", f"{nb(g['part'].head(5).sum() * 100, 1)} %", "de l'encours"),
    ("Émetteurs bancaires", f"{nb(t.loc[t['banque'], 'encours'].sum() / g['encours'].sum() * 100, 1)} %",
     "de l'encours"),
])

gauche, droite = st.columns([3, 2])
with gauche.container(border=True):
    st.markdown("**20 premiers émetteurs** · encours par instrument (Md MAD)")
    noms = g.index[:20][::-1]
    fig = go.Figure()
    for i, ins in enumerate(par_instr.loc[g.index[:20]].sum().sort_values(ascending=False).index):
        v = par_instr.loc[noms, ins] / 1e9
        fig.add_bar(y=noms, x=v, orientation="h", name=ins, marker_color=couleur(ins, i),
                    hovertemplate="%{y}<br>" + ins + " : %{x:.2f} Md MAD<extra></extra>")
    fig.update_layout(barmode="stack")
    st.plotly_chart(mise_en_page(fig, 560, legend=dict(orientation="h", y=-0.08)), width="stretch")
with droite.container(border=True):
    st.markdown("**Encours par secteur et émetteur** · cliquer pour zoomer")
    sec = g.groupby("secteur")["encours"].sum()
    ids = list(sec.index) + [f"{s}/{e}" for e, s in zip(g.index, g["secteur"])]
    fig = go.Figure(go.Treemap(
        ids=ids, labels=list(sec.index) + list(g.index), parents=[""] * len(sec) + list(g["secteur"]),
        values=list(sec.values / 1e9) + list(g["encours"] / 1e9), branchvalues="total",
        marker=dict(colors=[couleur(None, i) for i in range(len(sec))]
                    + [couleur(None, list(sec.index).index(s)) for s in g["secteur"]],
                    line=dict(color="#fff", width=1)),
        hovertemplate="<b>%{label}</b><br>%{value:.2f} Md MAD<br>%{percentRoot:.1%} du total<extra></extra>",
        texttemplate="%{label}<br>%{value:.1f} Md"))
    st.plotly_chart(mise_en_page(fig, 560, margin=dict(l=0, r=0, t=0, b=0)), width="stretch")

with st.container(border=True):
    st.markdown("**Tous les émetteurs** · cliquer sur une ligne pour le détail")
    tab = g.reset_index()[["emetteur", "secteur", "encours", "part", "titres", "instruments", "maturite_res",
                           "spread_moyen", "prochaine_echeance"]]
    tab["encours"] = tab["encours"] / 1e6
    ev = st.dataframe(tab, hide_index=True, height=420, width="stretch", on_select="rerun",
                      selection_mode="single-row", key="e_table", column_config={
        "emetteur": st.column_config.TextColumn("Émetteur", pinned=True), "secteur": "Secteur",
        "encours": st.column_config.NumberColumn("Encours (M MAD)", format="localized"),
        "part": st.column_config.ProgressColumn("Part de l'encours", format="percent", min_value=0,
                                                max_value=float(tab["part"].max())),
        "titres": st.column_config.NumberColumn("Titres"), "instruments": "Instruments",
        "maturite_res": st.column_config.NumberColumn("Mat. résiduelle moy. (ans)", format="%.2f"),
        "spread_moyen": st.column_config.NumberColumn("Spread moyen (pb)", format="%.1f",
                                                      help="Moyenne pondérée par l'encours"),
        "prochaine_echeance": st.column_config.DateColumn("Prochaine échéance", format="DD/MM/YYYY")})

if not ev.selection.rows:
    st.stop()
nom = tab.iloc[ev.selection.rows[0]]["emetteur"]
e = t[t["emetteur"] == nom]
st.subheader(nom)
ligne = g.loc[nom]
kpis([
    ("Encours", f"{nb(ligne['encours'] / 1e6)} M MAD", f"{nb(ligne['part'] * 100, 2)} % de la sélection"),
    ("Titres en vie", nb(ligne["titres"]), ligne["instruments"]),
    ("Maturité résiduelle moyenne", f"{nb(ligne['maturite_res'], 2)} ans", "pondérée par l'encours"),
    ("Spread moyen", "–" if pd.isna(ligne["spread_moyen"]) else f"{nb(ligne['spread_moyen'], 1)} pb",
     "pondéré par l'encours"),
])
g1, g2, g3 = st.columns(3)
for col, cle, titre in [(g1, "instrument", "Par instrument"), (g2, "type_taux", "Par type de taux"),
                        (g3, "remboursement", "Par mode de remboursement")]:
    with col.container(border=True):
        st.markdown(f"**{titre}** (M MAD)")
        r = e.groupby(cle)["encours"].sum().sort_values(ascending=False) / 1e6
        fig = go.Figure(go.Pie(labels=r.index, values=r.values, hole=0.6, sort=False, textinfo="percent",
                               marker=dict(colors=[couleur(x, i) for i, x in enumerate(r.index)],
                                           line=dict(color="#fff", width=2))))
        st.plotly_chart(mise_en_page(fig, 260, legend=dict(orientation="h", y=-0.15)), width="stretch")
with st.container(border=True):
    st.markdown("**Échéancier de l'encours** (M MAD, année de l'échéance finale)")
    ech = e.assign(annee=pd.to_datetime(e["date_echeance"]).dt.year).pivot_table(
        index="annee", columns="instrument", values="encours", aggfunc="sum").fillna(0) / 1e6
    fig = go.Figure()
    for i, ins in enumerate(ech.columns):
        fig.add_bar(x=ech.index.astype(str), y=ech[ins], name=ins, marker_color=couleur(ins, i))
    fig.update_layout(barmode="stack")
    st.plotly_chart(mise_en_page(fig, 300), width="stretch")
with st.container(border=True):
    st.markdown("**Titres de l'émetteur** · cliquer sur une ligne pour la fiche")
    col = ["isin", "libelle", "instrument", "nature", "type_taux", "remboursement", "date_emission", "date_echeance",
           "taux_facial", "encours", "spread_pb"]
    liste = e.sort_values("date_echeance")[col].assign(encours=lambda x: x["encours"] / 1e6)
    ev2 = st.dataframe(liste, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row",
                       key="e_titres", column_config={
        "isin": "ISIN", "libelle": "Libellé", "instrument": "Instrument", "nature": "Nature", "type_taux": "Taux",
        "remboursement": "Remb.",
        "date_emission": st.column_config.DateColumn("Émission", format="DD/MM/YYYY"),
        "date_echeance": st.column_config.DateColumn("Échéance", format="DD/MM/YYYY"),
        "taux_facial": st.column_config.NumberColumn("Taux facial", format="percent"),
        "encours": st.column_config.NumberColumn("Encours (M MAD)", format="localized"),
        "spread_pb": st.column_config.NumberColumn("Spread (pb)", format="%.1f")})
    if ev2.selection.rows:
        fiche(liste.iloc[ev2.selection.rows[0]]["isin"])
