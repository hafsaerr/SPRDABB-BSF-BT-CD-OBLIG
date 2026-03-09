from __future__ import annotations

import base64
import io
import logging
from datetime import date
from pathlib import Path
from statistics import median
from typing import Optional

import pandas as pd
import streamlit as st

from bam_curve_fetcher import BamCurveFetcher
from vba_equivalent_rates import calcul_taux

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
CREDENTIALS = {"spreadABB": "albarid2026"}
APP_TITLE   = "Spread Manager — BSF & CD | Al Barid Bank"
CACHE_DIR   = Path(__file__).parent / "cache_bam_curves"
ASSETS_DIR  = Path(__file__).parent / "assets"
CACHE_DIR.mkdir(parents=True, exist_ok=True)   # créer le cache au démarrage

# ─────────────────────────────────────────────────────────────────────────────
# CSS  —  palette Al Barid Bank (thème sombre, orange brûlé)
# ─────────────────────────────────────────────────────────────────────────────
STYLE = """
<style>
[data-testid="stAppViewContainer"] {
    background: #100804 !important;
    color: #EDE0D4 !important;
}
[data-testid="stAppViewContainer"] > .main {
    background: #100804 !important;
    color: #EDE0D4 !important;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1E0A03 0%, #130602 100%) !important;
}
[data-testid="stSidebar"] * { color: #EDE0D4 !important; }

/* inputs */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input {
    background: rgba(255,255,255,0.06) !important;
    color: #EDE0D4 !important;
    border: 1px solid rgba(200,80,30,0.35) !important;
    border-radius: 8px !important;
}
[data-baseweb="select"] > div {
    background: rgba(255,255,255,0.06) !important;
    color: #EDE0D4 !important;
    border: 1px solid rgba(200,80,30,0.35) !important;
}
[data-baseweb="popover"],
[data-baseweb="menu"],
[data-baseweb="option"] { background: #1E0A03 !important; color: #EDE0D4 !important; }
[data-baseweb="option"]:hover { background: rgba(200,80,30,0.25) !important; }
[data-testid="stCheckbox"] label { color: #EDE0D4 !important; }

/* file uploader */
[data-testid="stFileUploaderDropzone"] {
    background: rgba(255,255,255,0.05) !important;
    border: 1px dashed rgba(200,80,30,0.5) !important;
    border-radius: 10px !important;
}
[data-testid="stFileUploaderDropzone"] * { color: #EDE0D4 !important; }

/* dataframe */
[data-testid="stDataFrame"] {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(200,80,30,0.2) !important;
    border-radius: 10px !important;
}

/* progress */
[data-testid="stProgressBar"] > div > div {
    background: linear-gradient(90deg, #C8501E, #FF9060) !important;
}

/* alerts */
.stAlert   { background: rgba(200,80,30,0.10) !important; border: 1px solid rgba(200,80,30,0.35) !important; }
.stInfo    { background: rgba(80,120,200,0.10) !important; border: 1px solid rgba(80,120,200,0.3) !important; }
.stSuccess { background: rgba(60,160,60,0.10)  !important; border: 1px solid rgba(60,160,60,0.3) !important; }
[data-testid="stAlertContainer"] * { color: #EDE0D4 !important; }

/* nav buttons */
.nav-btn button {
    background: linear-gradient(90deg, #C8501E 0%, #7A2E08 100%) !important;
    color: #FFFFFF !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    font-size: 14px !important; padding: 10px !important;
    margin-bottom: 6px !important; width: 100% !important;
}
.nav-btn button:hover { opacity: 0.88 !important; }
.nav-btn-active button {
    background: linear-gradient(90deg, #E07030 0%, #C8501E 100%) !important;
    color: #FFFFFF !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    font-size: 14px !important; padding: 10px !important;
    margin-bottom: 6px !important; width: 100% !important;
    box-shadow: 0 0 0 2px #F5C518 !important;
}

/* generic buttons */
.stButton > button {
    background: linear-gradient(90deg, #C8501E 0%, #7A2E08 100%) !important;
    color: #FFFFFF !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    padding: 10px !important; width: 100% !important;
}
.stButton > button:hover { opacity: 0.88 !important; }
.stDownloadButton > button {
    background: linear-gradient(90deg, #1A6B2E 0%, #0D4019 100%) !important;
    color: white !important; border: none !important;
    border-radius: 10px !important; font-weight: 700 !important;
    padding: 10px !important; width: 100% !important;
}
.stDownloadButton > button:hover { opacity: 0.88 !important; }

/* KPI cards */
.kpi-card {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(200,80,30,0.3);
    border-top: 3px solid #C8501E;
    border-radius: 10px; padding: 14px 16px;
    text-align: center; margin-bottom: 10px;
}
.kpi-title { font-size: 11px; color: #A89070; font-weight: 600;
             letter-spacing: 0.8px; text-transform: uppercase; }
.kpi-value { font-size: 1.45rem; font-weight: 800; color: #FF9060; }
.kpi-sub   { font-size: 0.75rem; color: #8A7060; margin-top: 2px; }

/* section headers */
.section-hdr {
    background: linear-gradient(90deg, #C8501E 0%, #7A2E08 100%);
    color: #FFFFFF !important; padding: 9px 18px;
    border-radius: 8px; font-weight: 700; font-size: 0.95rem;
    margin: 20px 0 12px 0;
}

/* instruction cards */
.instr-card {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(200,80,30,0.25);
    border-left: 4px solid #C8501E;
    border-radius: 10px; padding: 16px 20px; margin-bottom: 14px;
}
.instr-card h4 { color: #FF9060; margin: 0 0 8px; font-size: 0.97rem; }
.instr-card ul { margin: 0; padding-left: 18px; color: #C8A888; font-size: 0.88rem; }
.instr-card li { margin-bottom: 4px; }

/* login */
[data-testid="stSidebar"].login-hide { display: none !important; }
[data-testid="stForm"] {
    background: rgba(10,5,2,0.75) !important;
    border: 1px solid rgba(200,80,30,0.25) !important;
    border-radius: 20px !important; padding: 10px 20px 20px !important;
}
[data-testid="stForm"] input {
    background: rgba(255,255,255,0.08) !important; color: #EDE0D4 !important;
    border: 1px solid rgba(200,80,30,0.3) !important; border-radius: 8px !important;
}
[data-testid="stForm"] label { color: #C8A888 !important; font-weight: 600 !important; }
[data-testid="stFormSubmitButton"] > button {
    background: linear-gradient(90deg, #C8501E, #7A2E08) !important;
    color: white !important; border: none !important; border-radius: 10px !important;
    font-weight: 700 !important; font-size: 15px !important;
    padding: 10px !important; width: 100% !important;
}

#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
</style>
"""

# ─────────────────────────────────────────────────────────────────────────────
# LOGO
# ─────────────────────────────────────────────────────────────────────────────
def _logo_b64() -> str:
    p = ASSETS_DIR / "ALBARID.png"
    if p.exists():
        return base64.b64encode(p.read_bytes()).decode()
    return ""

def _logo_img(size: int = 70) -> str:
    b = _logo_b64()
    if b:
        return (f'<img src="data:image/png;base64,{b}" '
                f'style="width:{size}px;height:{size}px;object-fit:contain;'
                f'border-radius:12px;box-shadow:0 2px 12px rgba(0,0,0,0.5);" />')
    return '<div style="font-size:42px;">🏦</div>'

# ─────────────────────────────────────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────────────────────────────────────
def _show_login() -> None:
    st.markdown("""
    <style>
    [data-testid="stSidebar"] { display: none !important; }
    .login-wrapper { max-width: 420px; margin: 0 auto; padding: 10px; }
    </style>
    <div class="login-wrapper">
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style="text-align:center; padding:34px 0 20px 0;">
        {_logo_img(90)}
        <div style="font-size:26px;font-weight:900;color:#FFFFFF;
                    letter-spacing:0.5px;margin:14px 0 4px 0;">Spread Manager</div>
        <div style="font-size:16px;font-weight:700;color:#F5C518;margin-bottom:4px;">
            Al Barid Bank</div>
        <div style="font-size:12px;color:#A89888;margin-bottom:20px;">
            Calcul du spread BSF &amp; CD — Connexion sécurisée</div>
    </div>
    """, unsafe_allow_html=True)

    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Username", placeholder="spreadABB",  key="lu")
        password = st.text_input("Code", type="password", placeholder="••••••••", key="lp")
        submit   = st.form_submit_button("Se connecter", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if submit:
        if CREDENTIALS.get(username) == password:
            st.session_state["authenticated"] = True
            st.session_state["username"] = username
            st.rerun()
        else:
            st.error("Identifiants incorrects. Réessayez.")

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
def _render_sidebar() -> str:
    with st.sidebar:
        st.markdown(f"""
        <div style="text-align:center;padding:14px 0 6px 0;">{_logo_img(72)}</div>
        <div style="text-align:center;padding:6px 0 16px 0;">
            <div style="font-size:15px;font-weight:900;color:#F5C518;letter-spacing:2px;">
                Al Barid Bank</div>
            <div style="font-size:11px;color:#8A7060;margin-top:2px;">Spread Manager</div>
            <hr style="border-color:rgba(200,80,30,0.3);margin:12px 0 8px 0;">
        </div>
        """, unsafe_allow_html=True)

        current = st.session_state.get("_nav", "Accueil")
        for item in ["Accueil", "Calculateur de Spread"]:
            css = "nav-btn-active" if item == current else "nav-btn"
            st.markdown(f'<div class="{css}">', unsafe_allow_html=True)
            if st.button(item, key=f"nav_{item}", use_container_width=True):
                st.session_state["_nav"] = item
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<hr style='border-color:rgba(200,80,30,0.2);margin:14px 0;'>",
                    unsafe_allow_html=True)
        st.markdown(
            f"<span style='font-size:11px;color:#8A7060;'>"
            f"Connecté : {st.session_state.get('username','')}</span>",
            unsafe_allow_html=True)
        if st.button("🔓 Déconnexion", use_container_width=True):
            st.session_state.clear()
            st.rerun()

    return st.session_state.get("_nav", "Accueil")

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _kpi(title: str, value: str, sub: str = "") -> str:
    return (f"<div class='kpi-card'><div class='kpi-title'>{title}</div>"
            f"<div class='kpi-value'>{value}</div>"
            + (f"<div class='kpi-sub'>{sub}</div>" if sub else "")
            + "</div>")

def _sec(title: str) -> None:
    st.markdown(f"<div class='section-hdr'>{title}</div>", unsafe_allow_html=True)

def _to_decimal(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, str):
        val = val.strip().replace("%","").replace(",",".").replace(" ","")
    try:
        v = float(val)
    except (ValueError, TypeError):
        return None
    return v / 100.0 if abs(v) > 1 else v

_RATE_KW = ["COUPONRT","TAUX","RATE","RENDEMENT","INTRATE","REND","EMISSION"]

def _detect_rate_cols(cols: list[str]) -> list[str]:
    return [c for c in cols if any(k in c.upper() for k in _RATE_KW)]

def _detect_type(row_name: str) -> str:
    n = row_name.upper()
    if "BSF" in n: return "BSF"
    if "CD"  in n: return "CD"
    return "Autre"

# ─────────────────────────────────────────────────────────────────────────────
# PAGE ACCUEIL
# ─────────────────────────────────────────────────────────────────────────────
def _page_home() -> None:
    st.markdown(f"""
    <div style="background:linear-gradient(90deg,#1E0A03 0%,#130602 100%);
                border:1px solid rgba(200,80,30,0.4);border-radius:14px;
                padding:22px 28px;margin-bottom:22px;
                display:flex;align-items:center;gap:20px;">
        {_logo_img(60)}
        <div>
            <div style="font-size:1.55rem;font-weight:900;color:#FFFFFF;">Spread Manager</div>
            <div style="font-size:1rem;font-weight:700;color:#F5C518;">Al Barid Bank</div>
            <div style="font-size:0.85rem;color:#8A7060;margin-top:2px;">
                Calcul du spread BSF &amp; CD vs courbe BDT Bank Al-Maghrib</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    _sec("📋 Guide d'utilisation")
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown("""
        <div class="instr-card">
            <h4>📁 Étape 1 — Charger le fichier Maroclear</h4>
            <ul>
                <li>Exportez votre fichier depuis <b>Maroclear</b> en format <b>.xlsx</b></li>
                <li>Glissez-déposez le fichier dans la zone d'upload</li>
                <li>L'application détecte automatiquement toutes les <b>feuilles disponibles</b>
                    — plus besoin de connaître le nom exact</li>
                <li>Sélectionnez la feuille contenant les TCN (ex : OBL_ORDN)</li>
            </ul>
        </div>
        <div class="instr-card">
            <h4>🔍 Étape 2 — Filtrer les instruments</h4>
            <ul>
                <li>L'application détecte automatiquement les <b>CD</b>
                    (Certificats de Dépôt) et les <b>BSF</b>
                    (Bons de Sociétés Financières)</li>
                <li>Filtrage sur la catégorie <b>TCN</b></li>
                <li>Cochez / décochez les types dans le panneau
                    de paramètres qui apparaît après chargement</li>
                <li>Ajustez la <b>maturité résiduelle min/max en années</b> (ex : 0 à 5 ans)</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="instr-card">
            <h4>📈 Étape 3 — Calculer les spreads</h4>
            <ul>
                <li>Récupération automatique des <b>courbes BDT BAM</b>
                    (bkam.ma) pour chaque date d'émission</li>
                <li>Les courbes téléchargées sont <b>mises en cache</b>
                    dans <code>cache_bam_curves/</code> pour accélérer
                    les prochaines sessions</li>
                <li>Interpolation du taux BDT à la maturité exacte
                    (logique VBA officielle BAM)</li>
                <li><b>Spread (bps) = Taux instrument − Taux BDT</b></li>
            </ul>
        </div>
        <div class="instr-card">
            <h4>📥 Étape 4 — Exporter les résultats</h4>
            <ul>
                <li>Tableau interactif : date émission, échéance,
                    nom, type, Taux BDT, Spread</li>
                <li>KPIs : spread moyen, médian, min, max —
                    global et par type (CD / BSF)</li>
                <li>Export <b>Excel</b> en 3 onglets :
                    <b>Global</b>, <b>CD</b>, <b>BSF</b></li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    _sec("🏦 Instruments couverts")
    c1, c2 = st.columns(2, gap="large")
    with c1:
        st.markdown("""
        <div class="instr-card">
            <h4>CD — Certificats de Dépôt</h4>
            <ul>
                <li>Émis par les établissements de crédit agréés</li>
                <li>Maturité : 10 jours à 5 ans</li>
                <li>Négociables sur le marché monétaire secondaire</li>
            </ul>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown("""
        <div class="instr-card">
            <h4>BSF — Bons de Sociétés Financières</h4>
            <ul>
                <li>Émis par les sociétés de financement</li>
                <li>Maturité : 2 ans à 5 ans</li>
                <li>Rendement généralement supérieur aux BDT</li>
            </ul>
        </div>""", unsafe_allow_html=True)

    st.info("**Connexion internet requise** pour récupérer les courbes BDT. "
            "Les courbes déjà téléchargées sont réutilisées automatiquement (cache local).")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CALCULATEUR
# ─────────────────────────────────────────────────────────────────────────────
def _page_spread() -> None:
    st.markdown(f"""
    <div style="background:linear-gradient(90deg,#1E0A03 0%,#130602 100%);
                border:1px solid rgba(200,80,30,0.4);border-radius:14px;
                padding:18px 26px;margin-bottom:20px;
                display:flex;align-items:center;gap:18px;">
        {_logo_img(52)}
        <div>
            <div style="font-size:1.35rem;font-weight:900;color:#FFFFFF;">
                Calculateur de Spread</div>
            <div style="font-size:0.85rem;color:#C8A888;">
                BSF &amp; CD — Courbe BDT Bank Al-Maghrib</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── ÉTAPE 1 : Upload ──────────────────────────────────────────────────────
    _sec("📁 Étape 1 — Charger le fichier")
    uploaded = st.file_uploader(
        "Fichier Maroclear (.xlsx)",
        type=["xlsx"],
        help="Fichier exporté depuis Maroclear contenant les instruments TCN",
    )
    if uploaded is None:
        st.info("Chargez un fichier Excel pour commencer.")
        return

    # ── ÉTAPE 2 : Détection auto des feuilles ─────────────────────────────────
    _sec("📄 Étape 2 — Sélectionner la feuille")
    try:
        xl = pd.ExcelFile(uploaded, engine="openpyxl")
        sheet_names = xl.sheet_names
    except Exception as e:
        st.error(f"Impossible de lire le fichier : {e}")
        return

    sheet = st.selectbox(
        "Feuille Excel",
        options=sheet_names,
        help="Toutes les feuilles disponibles dans votre fichier sont listées ici.",
    )

    try:
        uploaded.seek(0)
        df = pd.read_excel(uploaded, sheet_name=sheet, engine="openpyxl")
    except Exception as e:
        st.error(f"Erreur de lecture de la feuille '{sheet}' : {e}")
        return

    st.success(f"✅ Feuille **{sheet}** chargée — {len(df)} lignes, {len(df.columns)} colonnes.")

    # ── Vérification colonnes requises ─────────────────────────────────────────
    REQUIRED = ["ISSUEDT", "MATURITYDT_L", "INSTRCTGRY", "ENGLONGNAME", "ENGPREFERREDNAME"]
    missing  = [c for c in REQUIRED if c not in df.columns]
    if missing:
        st.error(f"Colonnes manquantes : **{missing}**\n\n"
                 f"Colonnes disponibles : {list(df.columns)}")
        return

    # ── ÉTAPE 3 : Paramètres de filtrage ──────────────────────────────────────
    _sec("⚙️ Étape 3 — Paramètres de filtrage")

    # Détecter les dates min/max de ISSUEDT dans le fichier
    _issuedt_series = pd.to_datetime(df["ISSUEDT"], errors="coerce").dropna()
    _date_min_file  = _issuedt_series.min().date() if not _issuedt_series.empty else date(2020, 1, 1)
    _date_max_file  = _issuedt_series.max().date() if not _issuedt_series.empty else date.today()

    # Ligne 1 — Types d'instruments + Maturité
    fc1, fc2, fc3, fc4 = st.columns([1, 1, 1, 1])
    with fc1:
        inc_cd  = st.checkbox("CD — Certificats de Dépôt",      value=True)
    with fc2:
        inc_bsf = st.checkbox("BSF — Bons de Soc. Financières", value=True)
    with fc3:
        res_min_y = st.number_input("Maturité min (ans)", value=0.0, min_value=0.0,
                                    max_value=30.0, step=0.5, format="%.1f")
    with fc4:
        res_max_y = st.number_input("Maturité max (ans)", value=5.0, min_value=0.0,
                                    max_value=30.0, step=0.5, format="%.1f")

    # Ligne 2 — Date d'émission (ISSUEDT)
    st.markdown(
        "<span style='font-size:0.85rem;color:#C8A888;font-weight:600;'>"
        "📅 Filtrer par date d'émission (ISSUEDT)</span>",
        unsafe_allow_html=True,
    )
    # Bornes dynamiques basées sur les dates réelles du fichier
    _bound_min = min(_date_min_file, date(1990, 1, 1))
    _bound_max = max(_date_max_file, date.today())

    dc1, dc2 = st.columns(2)
    with dc1:
        date_iss_min = st.date_input(
            "Date d'émission — Début",
            value=_date_min_file,
            min_value=_bound_min,
            max_value=_bound_max,
            key="iss_min",
        )
    with dc2:
        date_iss_max = st.date_input(
            "Date d'émission — Fin",
            value=_date_max_file,
            min_value=_bound_min,
            max_value=_bound_max,
            key="iss_max",
        )

    # Conversion maturité en jours
    res_min = int(res_min_y * 365)
    res_max = int(res_max_y * 365) if res_max_y > 0 else 36500

    instrument_types = []
    if inc_cd:  instrument_types.append("CD")
    if inc_bsf: instrument_types.append("BSF")
    if not instrument_types:
        st.warning("Sélectionnez au moins un type d'instrument.")
        return

    # ── Filtrage ───────────────────────────────────────────────────────────────
    dff = df.copy()
    dff["_idt"] = pd.to_datetime(dff["ISSUEDT"],      errors="coerce").dt.date
    dff["_mdt"] = pd.to_datetime(dff["MATURITYDT_L"], errors="coerce").dt.date

    name_mix = (dff["ENGLONGNAME"].fillna("").astype(str)
                + " " + dff["ENGPREFERREDNAME"].fillna("").astype(str)).str.upper()

    mask_tcn    = dff["INSTRCTGRY"].fillna("").astype(str).str.upper().eq("TCN")
    mask_type   = pd.Series(False, index=dff.index)
    for t in instrument_types:
        mask_type |= name_mix.str.contains(t.upper(), regex=False)

    # Filtre date d'émission
    mask_issuedt = dff["_idt"].between(date_iss_min, date_iss_max)

    # Filtre maturité résiduelle
    residual     = (pd.to_datetime(dff["MATURITYDT_L"], errors="coerce")
                    - pd.to_datetime(dff["ISSUEDT"],    errors="coerce")).dt.days
    mask_resid   = residual.between(res_min, res_max)

    mask = mask_tcn & mask_type & mask_issuedt & mask_resid
    selected_idx = df.index[mask].tolist()

    # ── KPIs avant calcul ─────────────────────────────────────────────────────
    type_counts = {
        t: int((mask & name_mix.str.contains(t, regex=False)).sum())
        for t in instrument_types
    }
    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(_kpi("Lignes totales",      str(len(df))),            unsafe_allow_html=True)
    k2.markdown(_kpi("Instruments retenus", str(len(selected_idx))),  unsafe_allow_html=True)
    tstr = " / ".join(f"{k}:{v}" for k, v in type_counts.items())
    k3.markdown(_kpi("Répartition", tstr),                            unsafe_allow_html=True)
    k4.markdown(_kpi("Période émission",
                     f"{date_iss_min.strftime('%d/%m/%Y')}",
                     f"→ {date_iss_max.strftime('%d/%m/%Y')}"),       unsafe_allow_html=True)

    if not selected_idx:
        st.warning("Aucun instrument trouvé avec ces filtres.")
        return

    # ── Colonne taux instrument (optionnelle) ──────────────────────────────────
    rate_candidates = _detect_rate_cols(list(df.columns))
    rate_col: Optional[str] = None
    if rate_candidates:
        choice   = st.selectbox(
            "Colonne taux instrument (pour calcul spread)",
            ["— aucune —"] + rate_candidates,
            index=1,
        )
        rate_col = None if choice == "— aucune —" else choice
    else:
        st.info("Aucune colonne taux détectée — seul le Taux BDT sera calculé.")

    # ── Bouton calcul ──────────────────────────────────────────────────────────
    if not st.button("⚡ Calculer les spreads", use_container_width=True):
        return

    # ── Préparer le df filtré ──────────────────────────────────────────────────
    df_work = df.loc[selected_idx].copy()
    df_work["ISSUEDT"]      = pd.to_datetime(df_work["ISSUEDT"],      errors="coerce")
    df_work["MATURITYDT_L"] = pd.to_datetime(df_work["MATURITYDT_L"], errors="coerce")

    # Maturité en années (arrondie à 2 décimales)
    df_work["Maturité (ans)"] = (
        (df_work["MATURITYDT_L"] - df_work["ISSUEDT"]).dt.days / 365.25
    ).round(2)

    unique_dates = sorted({d.date() for d in df_work["ISSUEDT"].dropna()})

    # ── Récupération courbes BAM ───────────────────────────────────────────────
    _sec("🌐 Récupération des courbes BDT BAM")
    pbar    = st.progress(0.0)
    stxt    = st.empty()
    fetcher = BamCurveFetcher(cache_dir=str(CACHE_DIR))

    total_dates = len(unique_dates)

    def _progress(done: int, total: int, n_cache: int, n_net: int, eta: float) -> None:
        pbar.progress(done / max(total, 1))
        eta_str = f" — ETA : {eta:.0f}s" if eta > 1 else ""
        stxt.markdown(
            f"<span style='color:#C8A888;font-size:0.85rem;'>"
            f"Courbes BAM : <b>{done}/{total}</b>"
            f" &nbsp;(cache : {n_cache} | réseau : {n_net}){eta_str}</span>",
            unsafe_allow_html=True,
        )

    curves = fetcher.get_curves_parallel(
        unique_dates,
        max_workers=10,
        progress_callback=_progress,
    )

    pbar.empty(); stxt.empty()
    ok = sum(1 for v in curves.values() if v is not None)
    st.success(f"✅ {ok}/{total_dates} courbes BDT récupérées.")

    # ── Calcul taux BDT + spread ───────────────────────────────────────────────
    bdt_rates:   list[Optional[float]] = []
    instr_rates: list[Optional[float]] = []
    spreads_bps: list[Optional[float]] = []

    for _, row in df_work.iterrows():
        idt = row["ISSUEDT"]
        mdt = row["MATURITYDT_L"]

        if pd.isna(idt) or pd.isna(mdt):
            bdt_rates.append(None); instr_rates.append(None); spreads_bps.append(None)
            continue

        mat_days = int((mdt - idt).days)
        if mat_days <= 0:
            bdt_rates.append(None); instr_rates.append(None); spreads_bps.append(None)
            continue

        curve = curves.get(idt.date())
        if not curve:
            bdt_rates.append(None); instr_rates.append(None); spreads_bps.append(None)
            continue

        mt, tx = curve
        try:
            bdt = calcul_taux(mat_days, mt, tx, idt.date())
            bdt_rates.append(bdt)
        except Exception:
            bdt_rates.append(None); instr_rates.append(None); spreads_bps.append(None)
            continue

        ir     = _to_decimal(row.get(rate_col)) if rate_col else None
        spread = (ir - bdt) * 10_000 if (ir is not None and bdt is not None) else None
        instr_rates.append(ir)
        spreads_bps.append(spread)

    # ── Attacher les colonnes calculées ───────────────────────────────────────
    df_work["Taux BDT"] = bdt_rates
    if rate_col:
        df_work["Taux instrument"] = instr_rates
        df_work["Spread (bps)"]   = spreads_bps

    nm = (df_work["ENGLONGNAME"].fillna("") + " " + df_work["ENGPREFERREDNAME"].fillna("")).astype(str)
    df_work["Type"] = nm.apply(_detect_type)

    # ── KPIs spread ───────────────────────────────────────────────────────────
    if rate_col:
        valid = [s for s in spreads_bps if s is not None]
        if valid:
            _sec("📈 KPIs — Spread")
            s1, s2, s3, s4 = st.columns(4)
            s1.markdown(_kpi("Spread moyen",  f"{sum(valid)/len(valid):.1f}", "bps"), unsafe_allow_html=True)
            s2.markdown(_kpi("Spread médian", f"{median(valid):.1f}",         "bps"), unsafe_allow_html=True)
            s3.markdown(_kpi("Spread max",    f"{max(valid):.1f}",            "bps"), unsafe_allow_html=True)
            s4.markdown(_kpi("Spread min",    f"{min(valid):.1f}",            "bps"), unsafe_allow_html=True)

            for t in instrument_types:
                sub = df_work[df_work["Type"] == t]["Spread (bps)"].dropna().tolist()
                if sub:
                    st.markdown(
                        f"<span style='color:#C8A888;font-size:0.85rem;'>"
                        f"<b style='color:#FF9060;'>{t}</b> — "
                        f"Spread moyen : <b>{sum(sub)/len(sub):.1f} bps</b> | "
                        f"Médian : <b>{median(sub):.1f} bps</b> | "
                        f"n = {len(sub)}</span>",
                        unsafe_allow_html=True,
                    )

    # ── Tableau ───────────────────────────────────────────────────────────────
    _sec("📋 Tableau des résultats")
    base_cols  = ["ISSUEDT", "MATURITYDT_L", "Maturité (ans)", "ENGLONGNAME", "Type", "INSTRCTGRY"]
    extra_cols = (["Taux instrument", "Taux BDT", "Spread (bps)"] if rate_col else ["Taux BDT"])
    disp = [c for c in base_cols + extra_cols if c in df_work.columns]

    fmt: dict = {}
    if "Maturité (ans)"   in disp: fmt["Maturité (ans)"]   = "{:.2f}"
    if "Taux BDT"         in disp: fmt["Taux BDT"]         = "{:.4%}"
    if "Taux instrument"  in disp: fmt["Taux instrument"]  = "{:.4%}"
    if "Spread (bps)"     in disp: fmt["Spread (bps)"]     = "{:.1f}"

    st.dataframe(
        df_work[disp].reset_index(drop=True).style.format(fmt),
        use_container_width=True,
        height=420,
    )

    # ── Export Excel ──────────────────────────────────────────────────────────
    _sec("📥 Export Excel")
    import re as _re
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers export
    # ─────────────────────────────────────────────────────────────────────────
    _BANK_ALIASES = {"SGMB": "SAHAM"}   # SGMB et SAHAM → même feuille

    def _bank_tag(name: str) -> str:
        parts = str(name).strip().split()
        raw = parts[1].upper() if len(parts) >= 2 else "AUTRE"
        return _BANK_ALIASES.get(raw, raw)

    def _mat_label_from_name(details) -> str:
        """Extrait le label maturité depuis le nom de l'instrument.
        Ex: 'CD ATW 21032024 3 35 a 52 sem' → '52 semaines'
        """
        s = "" if details is None else str(details).strip().lower()
        if not s:
            return "inconnue"
        s = _re.sub(r"\bsem\.\b", "sem", s)
        matches = _re.findall(
            r"(\d+)\s*(semaines?|semaine|sem|s|mois|ans?|an|jours?|jrs?|jr|j)\b", s
        )
        if not matches:
            return "inconnue"
        num, unit = matches[-1]
        unit = unit.lower()
        if unit in {"s", "sem"} or unit.startswith("semaine"):
            unit_norm = "semaine" if num == "1" else "semaines"
        elif unit in {"j", "jr", "jrs"} or unit.startswith("jour"):
            unit_norm = "jour" if num == "1" else "jours"
        elif unit.startswith("mois"):
            unit_norm = "mois"
        else:
            unit_norm = "an" if num == "1" else "ans"
        return f"{num} {unit_norm}"

    def _mat_sort_key(label: str):
        m = _re.match(r"^\s*(\d+)\s*(jour|jours|semaine|semaines|mois|an|ans)\s*$",
                      str(label).lower())
        if not m:
            return (99, 10**9)
        n = int(m.group(1)); u = m.group(2)
        if u.startswith("jour"):     return (0, n)
        if u.startswith("semaine"):  return (1, n)
        if u.startswith("mois"):     return (2, n)
        return (3, n)

    # ── Préparer df export ────────────────────────────────────────────────────
    df_xls = df_work.copy()
    df_xls["_bank"] = df_xls["ENGLONGNAME"].fillna("").apply(_bank_tag)

    # Spread reste en bps (entier) — filtre 10-70 bps
    has_spread = "Spread (bps)" in df_xls.columns
    if has_spread:
        df_xls["Spread"] = df_xls["Spread (bps)"]          # bps, format "0"
        df_xls_filt = df_xls[df_xls["Spread (bps)"].between(10, 70)].copy()
    else:
        df_xls_filt = df_xls.copy()

    # TAUX D'INTÉRÊT en % points (ex: 3.35, pas 0.0335)
    if "Taux instrument" in df_xls_filt.columns:
        df_xls_filt["_taux_instr_pct"] = df_xls_filt["Taux instrument"].apply(
            lambda v: round(float(v) * 100, 4) if pd.notna(v) and v is not None else None
        )

    n_export = len(df_xls_filt)
    n_total  = len(df_xls)
    if has_spread:
        st.info(f"Export : **{n_export}** instruments avec spread entre **10 et 70 bps** "
                f"(sur {n_total} calculés).")

    # ── Colonne CODE / ISIN (priorité : ISIN exact, puis codes Maroclear) ────
    _ISIN_EXACT  = {"ISINCODE","ISIN","CODEISIN","ISIN_CODE","INSTRISINOCODE"}
    _CODE_APPROX = {"INSTRCODE","INSTRUMENTCODE","INSTRNO","NEMOCODE","NEMO",
                    "SECURITYNO","CODE","INSTRID","INSTRUMENTID","INSTRIDENTIFIER"}
    _code_col = (
        next((c for c in df_work.columns if c.upper() in _ISIN_EXACT),  None)
        or next((c for c in df_work.columns if c.upper() in _CODE_APPROX), None)
        # Dernier recours : colonne dont la valeur ressemble à un ISIN (MA…)
        or next(
            (c for c in df_work.columns
             if df_work[c].dropna().astype(str).str.match(r"^MA\d{10}$").any()),
            None,
        )
    )

    # ── Colonnes export ───────────────────────────────────────────────────────
    _exp_cols = (
        ([_code_col] if _code_col else [])
        + ["ENGLONGNAME", "ISSUEDT", "MATURITYDT_L", "Maturité (ans)", "Taux BDT"]
        + (["Spread"]             if "Spread"             in df_xls_filt.columns else [])
        + (["_taux_instr_pct"]    if "_taux_instr_pct"    in df_xls_filt.columns else [])
    )
    _exp_cols = [c for c in _exp_cols if c in df_xls_filt.columns]

    _col_labels: dict = {
        "ENGLONGNAME":    "DETAILS DU TITRE",
        "ISSUEDT":        "DATE D'EMISSION",
        "MATURITYDT_L":   "DATE D'ECHEANCE",
        "Maturité (ans)": "Maturite residuelle",
        "Taux BDT":       "TAUX BDT",
        "Spread":         "Spread",
        "_taux_instr_pct":"TAUX D'INTERET",
    }
    if _code_col:
        _col_labels[_code_col] = "CODE"

    # ── Styles Excel (inspiré de l'ancien site) ───────────────────────────────
    _YELLOW = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    _GRAY   = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
    _BLUE   = PatternFill(start_color="DCE6F1", end_color="DCE6F1", fill_type="solid")
    _BOLD   = Font(bold=True)
    _THIN   = Side(border_style="thin", color="000000")
    _BORDER = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)
    _CENTER = Alignment(horizontal="center", vertical="center")
    _HIGHLIGHT_HDRS = {"TAUX BDT", "Spread", "TAUX D'INTERET"}

    def _style_ws(ws, n_data: int) -> None:
        max_col = ws.max_column
        # Ligne 1 : en-têtes jaunes
        for c in range(1, max_col + 1):
            cell = ws.cell(1, c)
            cell.fill = _YELLOW; cell.font = _BOLD
            cell.alignment = _CENTER; cell.border = _BORDER
        # Colonnes highlight (BDT, Spread, Taux) en jaune; autres en gris
        hl_cols = {c for c in range(1, max_col + 1)
                   if str(ws.cell(1, c).value or "") in _HIGHLIGHT_HDRS}
        for r in range(2, n_data + 2):
            for c in range(1, max_col + 1):
                cell = ws.cell(r, c)
                cell.fill   = _YELLOW if c in hl_cols else _GRAY
                cell.border = _BORDER
        # Largeurs colonnes
        for col_letter, w in zip("ABCDEFGH", [18, 38, 14, 14, 14, 12, 10, 14]):
            ws.column_dimensions[col_letter].width = w

    def _apply_number_formats(ws, n_data: int) -> None:
        hdr = {str(ws.cell(1, c).value): c for c in range(1, ws.max_column + 1)}
        for r in range(2, n_data + 2):
            if "DATE D'EMISSION" in hdr:
                ws.cell(r, hdr["DATE D'EMISSION"]).number_format  = "dd/mm/yyyy"
            if "DATE D'ECHEANCE" in hdr:
                ws.cell(r, hdr["DATE D'ECHEANCE"]).number_format  = "dd/mm/yyyy"
            if "Maturite residuelle" in hdr:
                ws.cell(r, hdr["Maturite residuelle"]).number_format = "0.00"
            if "TAUX BDT" in hdr:
                ws.cell(r, hdr["TAUX BDT"]).number_format          = "0.00%"
            if "Spread" in hdr:
                ws.cell(r, hdr["Spread"]).number_format             = "0"
            if "TAUX D'INTERET" in hdr:
                ws.cell(r, hdr["TAUX D'INTERET"]).number_format     = "0.00"

    def _add_summary(ws, df_s: pd.DataFrame, n_data: int) -> None:
        """Table synthèse par maturité (labels extraits du nom de l'instrument)."""
        start_r = n_data + 3
        for ci, hdr in enumerate(["MATURITE", "MOYENNE SPREAD", "SPREAD MAX", "SPREAD MIN"], 1):
            c = ws.cell(start_r, ci, hdr)
            c.fill = _BLUE; c.font = _BOLD
            c.alignment = _CENTER; c.border = _BORDER

        buckets: dict[str, list[float]] = {}
        for _, row in df_s.iterrows():
            lbl = _mat_label_from_name(row.get("DETAILS DU TITRE", ""))
            if lbl == "inconnue":
                continue
            try:
                sv = float(row.get("Spread", None))
                if sv >= 0:
                    buckets.setdefault(lbl, []).append(sv)
            except (TypeError, ValueError):
                pass

        rr = start_r + 1
        for lbl in sorted(buckets, key=_mat_sort_key):
            vals = buckets[lbl]
            avg = sum(vals) / len(vals)
            for ci, v in enumerate(
                [lbl, f"{avg:.0f} bps", f"{max(vals):.0f} bps", f"{min(vals):.0f} bps"], 1
            ):
                cell = ws.cell(rr, ci, v)
                cell.fill = _GRAY; cell.border = _BORDER
                cell.alignment = _CENTER
                if ci == 1: cell.font = _BOLD
            rr += 1

    def _write_sheet(writer, df_s: pd.DataFrame, sheet_name: str) -> None:
        sn     = sheet_name[:31]
        df_out = df_s[_exp_cols].rename(columns=_col_labels).reset_index(drop=True)
        df_out.to_excel(writer, sheet_name=sn, index=False)
        ws     = writer.sheets[sn]
        n_data = len(df_out)
        _style_ws(ws, n_data)
        _apply_number_formats(ws, n_data)
        _add_summary(ws, df_out, n_data)

    # ── Écriture du fichier ───────────────────────────────────────────────────
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if not df_xls_filt.empty:
            _write_sheet(writer, df_xls_filt, "TOUT")
        df_xls_filt = df_xls_filt.copy()
        df_xls_filt["_type_nn"] = df_xls_filt["Type"].fillna("AUTRE")
        df_xls_filt["_bank_nn"] = df_xls_filt["_bank"].fillna("AUTRE")
        for (typ, bank), df_grp in df_xls_filt.groupby(["_type_nn", "_bank_nn"]):
            if not df_grp.empty:
                _write_sheet(writer, df_grp, f"{typ}_{bank}")

    output.seek(0)
    st.download_button(
        label="⬇️  Télécharger les résultats (Excel)",
        data=output.getvalue(),
        file_name=f"spread_bsf_cd_{date.today().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="auto",
    )
    st.markdown(STYLE, unsafe_allow_html=True)

    if not st.session_state.get("authenticated"):
        _show_login()
        return

    page = _render_sidebar()

    if page == "Accueil":
        _page_home()
    else:
        _page_spread()

if __name__ == "__main__":
    main()
