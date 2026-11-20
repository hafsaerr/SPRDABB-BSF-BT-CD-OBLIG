"""Éléments partagés par les pages : style, données de la session, fiche d'un titre."""
from __future__ import annotations

import base64
import hashlib
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from spreadabb import maroclear, spreads
from spreadabb.bam import SourceBAM
from spreadabb.courbe import Courbes, historique_du_depot

RACINE = Path(__file__).resolve().parent.parent

COULEURS = {
    "CD": "#3b6fb6", "BSF": "#e0a100", "BT": "#8e5db5", "BDT": "#7f6c66",
    "Obligation ordinaire": "#2f9e77", "Obligation subordonnée": "#d0463b",
    "Obligation convertible": "#2aa3b8", "Titrisation (FT / FPCT)": "#c26b2e",
    "Ordinaire": "#2f9e77", "Subordonnée": "#d0463b", "Subordonnée perpétuelle": "#8f1d2c",
    "Perpétuelle": "#b84f8a", "Convertible": "#2aa3b8", "Titrisation": "#c26b2e",
    "Trésor": "#7f6c66", "TCN": "#3b6fb6", "Fixe": "#3b6fb6", "Révisable": "#e0a100",
    "Zéro-coupon": "#7f6c66",
}
PALETTE = ["#3b6fb6", "#e0a100", "#2f9e77", "#d0463b", "#8e5db5", "#c26b2e", "#2aa3b8", "#b84f8a", "#5c8a2f"]

CSS = """
<style>
:root { --taupe:#7f6c66; --jaune:#ffd400; --ligne:#e7e2dd; --doux:#6f6562; }
.block-container { padding-top: 2rem; max-width: 1400px; }
[data-testid="stSidebar"] { background:#2b2422; }
[data-testid="stSidebar"] * { color:#e9e2dd; }
[data-testid="stSidebar"] [data-testid="stSidebarNavLink"][aria-current="page"] { background:var(--jaune); }
[data-testid="stSidebar"] [data-testid="stSidebarNavLink"][aria-current="page"] * { color:#231e1c; font-weight:600; }
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] { background:rgba(255,255,255,.05); border:1px dashed rgba(255,255,255,.25); }
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button,
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button * { color:#231e1c; }
h1 { letter-spacing:-.02em; font-weight:700; }
.sous-titre { color:var(--doux); margin:-.6rem 0 1.2rem; }
.kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; margin-bottom:1rem; }
.kpi { background:#fff; border:1px solid var(--ligne); border-radius:14px; padding:14px 16px; position:relative; overflow:hidden;
       box-shadow:0 1px 2px rgba(35,30,28,.05),0 4px 14px rgba(35,30,28,.04); }
.kpi::before { content:""; position:absolute; left:0; top:0; bottom:0; width:3px; background:var(--jaune); }
.kpi span { color:var(--doux); font-size:12.5px; } .kpi b { display:block; font-size:24px; letter-spacing:-.02em; color:#231e1c; }
.kpi small { color:var(--doux); font-size:11.5px; }
[data-testid="stVerticalBlockBorderWrapper"] { border-radius:14px; background:#fff; }
.marque { display:flex; gap:10px; align-items:center; padding:0 0 10px; }
.marque img { width:38px; height:38px; border-radius:10px; } .marque b { font-size:16px; color:#fff; display:block; }
.marque span { font-size:11.5px; color:#a99d98; }
.tag { display:inline-block; padding:1px 9px; border-radius:6px; font-size:12px; font-weight:500; margin:0 4px 4px 0; background:#f1ede9; }
.frise span { display:inline-block; font-size:12px; padding:3px 8px; border-radius:6px; background:#f1ede9; margin:0 4px 4px 0; }
.frise span.passe { opacity:.45; } .frise span.prochain { background:var(--jaune); font-weight:600; }
.calcul { background:#f6f4f1; border-radius:12px; padding:14px 16px; margin-bottom:14px; }
.calcul .grand { font-size:30px; font-weight:700; letter-spacing:-.02em; }
.calcul p { margin:2px 0 0; color:var(--doux); font-size:13px; }
</style>
"""


def entete(titre: str, sous_titre: str = "") -> None:
    st.markdown(CSS, unsafe_allow_html=True)
    st.title(titre)
    if sous_titre:
        st.markdown(f'<div class="sous-titre">{sous_titre}</div>', unsafe_allow_html=True)


def kpis(items: list[tuple[str, str, str]]) -> None:
    html = "".join(f'<div class="kpi"><span>{a}</span><b>{b}</b><small>{c}</small></div>' for a, b, c in items)
    st.markdown(f'<div class="kpis">{html}</div>', unsafe_allow_html=True)


def logo_html() -> str:
    p = RACINE / "assets" / "ALBARID.png"
    img = f'<img src="data:image/png;base64,{base64.b64encode(p.read_bytes()).decode()}">' if p.exists() else ""
    return f'<div class="marque">{img}<div><b>SpreadABB</b><span>Marché obligataire</span></div></div>'


def couleur(v, i=0) -> str:
    return COULEURS.get(v, PALETTE[i % len(PALETTE)])


def mise_en_page(fig: go.Figure, hauteur: int = 380, **kw) -> go.Figure:
    reglages = dict(height=hauteur, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)", font=dict(family="sans-serif", size=12),
                    legend=dict(orientation="h", y=-0.2), separators=", ")
    fig.update_layout(**(reglages | kw))
    fig.update_xaxes(gridcolor="#eee8e3", zerolinecolor="#e2dbd5")
    fig.update_yaxes(gridcolor="#eee8e3", zerolinecolor="#e2dbd5")
    return fig


# ── Formats ──────────────────────────────────────────────────────────────────

def pct(v, d=3):
    return "–" if v is None or pd.isna(v) else f"{v * 100:,.{d}f} %".replace(",", " ").replace(".", ",")


def nb(v, d=0):
    return "–" if v is None or pd.isna(v) else f"{v:,.{d}f}".replace(",", " ").replace(".", ",")


def dt(v):
    return "–" if v is None or pd.isna(v) else pd.Timestamp(v).strftime("%d/%m/%Y")


# ── Données ──────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def source_bam() -> SourceBAM:
    return SourceBAM()


@st.cache_data(show_spinner=False)
def historique() -> pd.DataFrame:
    return historique_du_depot()


@st.cache_resource(show_spinner=False, max_entries=3)
def _traiter(cle: str, nom: str, contenu: bytes, jour: date):
    """Nettoyage + courbes + spreads ; partagé entre les utilisateurs qui déposent le même fichier."""
    titres, coupons, rapport = maroclear.charger(nom, contenu, jour)
    rapport = rapport.to_dict()
    bam = source_bam()
    avant = bam.telechargees
    courbes = Courbes(historique(), bam)
    dates = {t.date_emission for t in titres.itertuples() if spreads.base_de_calcul(t)[2] is None}
    # Site BAM interrogé pour les titres en vie ; les échus se contentent de l'historique et du cache
    courbes.preparer(dates, en_ligne=set(titres.loc[titres["en_vie"], "date_emission"]))
    titres = titres.merge(spreads.calculer(titres, courbes), on="isin", how="left")
    rapport["courbes"] = {
        "historique": int((titres["source_courbe"] == "Historique").sum()),
        "site_bam": int((titres["source_courbe"] == "Site BAM").sum()),
        "sans_courbe": int(titres["motif_exclusion"].fillna("").str.startswith("Courbe").sum()),
        "telechargees": bam.telechargees - avant, "site_bloque": bam.bloque,
    }
    return titres, coupons, rapport, courbes


def charger_fichier(nom: str, contenu: bytes) -> None:
    cle = hashlib.md5(contenu).hexdigest()
    if st.session_state.get("cle_fichier") == cle:
        return
    with st.spinner("Traitement du fichier Maroclear : nettoyage, puis recherche des courbes BDT "
                    "(historique, puis site de Bank Al-Maghrib pour les dates absentes)…"):
        titres, coupons, rapport, courbes = _traiter(cle, nom, contenu, date.today())
    st.session_state.update(cle_fichier=cle, titres=titres, coupons=coupons, rapport=rapport,
                            courbes=courbes, nom_fichier=nom)


def titres(tous: bool = False) -> pd.DataFrame | None:
    t = st.session_state.get("titres")
    if t is None:
        return None
    return t if tous else t[t["en_vie"]]


def exiger_titres() -> pd.DataFrame:
    t = titres()
    if t is None:
        st.info("Déposer le fichier Maroclear reçu (REPV_….zip) dans le panneau de gauche pour commencer.")
        st.stop()
    return t


# ── Fiche d'un titre ─────────────────────────────────────────────────────────

@st.dialog("Fiche du titre", width="large")
def fiche(isin: str) -> None:
    t = st.session_state["titres"].set_index("isin").loc[isin]
    cp = st.session_state["coupons"]
    dates_cp = sorted(cp.loc[cp["isin"] == isin, "date_coupon"])
    auj = date.today()
    st.caption(f"{isin} · {t['mnemonique'] or ''}")
    st.subheader(t["libelle"])
    tags = [t["instrument"], t["nature"], t["type_taux"], t["remboursement"], t["statut"]]
    st.markdown("".join(f'<span class="tag">{x}</span>' for x in tags if isinstance(x, str)), unsafe_allow_html=True)
    if pd.notna(t["spread_pb"]):
        st.markdown(
            f'<div class="calcul"><span>Spread d\'émission</span><div class="grand">{nb(t["spread_pb"], 1)} pb</div>'
            f'<p>Taux facial {pct(t["taux_facial"])} − taux BDT {pct(t["taux_bdt"])}<br>{t["methode"]} · durée de '
            f'référence {nb(t["duree_reference_ans"], 2)} an(s) · courbe du {dt(t["date_courbe"])} ({t["source_courbe"]})</p></div>',
            unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="calcul"><span>Spread d\'émission</span><div class="grand">–</div>'
                    f'<p>{t["motif_exclusion"] or "Non calculé"}</p></div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    champs = [
        ("Émetteur", t["emetteur"]), ("Secteur", t["secteur"]), ("Domiciliataire", t["domiciliataire"]),
        ("Date d'émission", dt(t["date_emission"])), ("Date d'échéance", dt(t["date_echeance"])),
        ("Maturité initiale", f'{nb(t["maturite_initiale_ans"], 2)} ans'),
        ("Taux facial", pct(t["taux_facial"])), ("Périodicité du coupon", t["periodicite"] or "–"),
        ("Maturité résiduelle", f'{nb(t["maturite_residuelle_ans"], 2)} ans'),
        ("Prochain coupon", dt(t["prochain_coupon"])), ("Prochaine révision", dt(t["prochaine_revision"])),
        ("Coupons restants", nb(t["coupons_restants"])),
        ("Nominal", f'{nb(t["nominal"])} MAD'), ("Montant émis", f'{nb(t["montant_emis"] / 1e6, 1)} M MAD'),
        ("Fréquence d'amortissement", t["frequence_amort"] or "–"),
    ]
    for i, (lib, val) in enumerate(champs):
        [c1, c2, c3][i % 3].metric(lib, "–" if val is None or (isinstance(val, float) and pd.isna(val)) else str(val))
    courbes = st.session_state.get("courbes")
    if pd.notna(t["spread_pb"]) and courbes:
        c = courbes.courbe(t["date_emission"])
        if c:
            ans = [0.25, 0.5, 1, 2, 5, 10, 15, 20, 30]
            fig = go.Figure()
            fig.add_scatter(x=ans, y=[p["taux"] * 100 for p in c.piliers()], mode="lines+markers", name="Courbe BDT",
                            line=dict(color="#7f6c66", width=2))
            fig.add_scatter(x=[t["duree_reference_ans"]], y=[t["taux_bdt"] * 100], mode="markers", name="Taux BDT retenu",
                            marker=dict(size=11, color="#3b6fb6"))
            fig.add_scatter(x=[t["duree_reference_ans"]], y=[t["taux_facial"] * 100], mode="markers", name="Taux facial",
                            marker=dict(size=12, color="#ffd400", line=dict(color="#231e1c", width=1)))
            fig.update_xaxes(type="log", tickvals=ans, ticktext=["3m", "6m", "1", "2", "5", "10", "15", "20", "30"],
                             title="Maturité (ans)")
            fig.update_yaxes(ticksuffix=" %")
            st.markdown(f"**Position sur la courbe BDT du {dt(c.date_courbe)}** · source : {c.source}")
            st.plotly_chart(mise_en_page(fig, 260), width="stretch")
    st.markdown(f"**Calendrier des coupons** · {len(dates_cp)} date(s)")
    prochain = next((d for d in dates_cp if d >= auj), None)
    st.markdown('<div class="frise">' + "".join(
        f'<span class="{"passe" if d < auj else ""}{" prochain" if d == prochain else ""}">{dt(d)}</span>'
        for d in dates_cp) + "</div>" if dates_cp else "Aucune date transmise par Maroclear", unsafe_allow_html=True)
    if t["anomalies"]:
        st.warning("Anomalies relevées : " + t["anomalies"])
