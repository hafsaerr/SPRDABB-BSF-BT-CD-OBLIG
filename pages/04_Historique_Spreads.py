"""
Page Historique des Spreads — SpreadABB
Importe les fonctions de rendu depuis app.py (point d'entrée principal).
"""
from __future__ import annotations

import sys
from pathlib import Path

# S'assurer que le répertoire racine est dans sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

# ── Auth guard ────────────────────────────────────────────────────────────────
if not st.session_state.get("authenticated"):
    st.switch_page("app.py")
    st.stop()

# ── Import depuis app.py ──────────────────────────────────────────────────────
from app import APP_TITLE, STYLE, _page_historique, _render_sidebar  # noqa: E402

st.set_page_config(
    page_title=f"Historique des Spreads — {APP_TITLE}",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto",
)
st.markdown(STYLE, unsafe_allow_html=True)
_render_sidebar()
_page_historique()
