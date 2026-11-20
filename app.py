"""SpreadABB : spreads d'émission du marché obligataire marocain contre la courbe BDT."""
import hmac

import streamlit as st

from vues.commun import CSS, charger_fichier, logo_html

st.set_page_config(page_title="SpreadABB", page_icon="assets/ALBARID.png", layout="wide")
st.markdown(CSS, unsafe_allow_html=True)


def _secret(cle: str):
    try:
        return st.secrets[cle]
    except Exception:
        return None


def connexion() -> bool:
    """Identifiant unique défini dans les secrets ; aucun contrôle si rien n'est défini."""
    utilisateur, mot_de_passe = _secret("USERNAME"), _secret("PASSWORD")
    if not utilisateur or st.session_state.get("connecte"):
        return True
    _, milieu, _ = st.columns([1, 1.2, 1])
    with milieu:
        st.markdown("<div style='height:12vh'></div>", unsafe_allow_html=True)
        st.markdown(logo_html().replace("color:#fff", "color:#231e1c"), unsafe_allow_html=True)
        with st.form("connexion"):
            st.subheader("Connexion")
            u = st.text_input("Identifiant")
            p = st.text_input("Mot de passe", type="password")
            if st.form_submit_button("Se connecter", width="stretch", type="primary"):
                if hmac.compare_digest(u, str(utilisateur)) and hmac.compare_digest(p, str(mot_de_passe)):
                    st.session_state["connecte"] = True
                    st.rerun()
                st.error("Identifiant ou mot de passe incorrect.")
    return False


if not connexion():
    st.stop()

pages = [
    st.Page("vues/tableau.py", title="Tableau de bord", icon=":material/space_dashboard:", default=True),
    st.Page("vues/titres.py", title="Univers des titres", icon=":material/table_rows:"),
    st.Page("vues/spreads.py", title="Spreads", icon=":material/scatter_plot:"),
    st.Page("vues/courbe.py", title="Courbe BDT", icon=":material/show_chart:"),
    st.Page("vues/risque.py", title="Risque de crédit", icon=":material/warning:"),
    st.Page("vues/import_donnees.py", title="Import et nettoyage", icon=":material/upload_file:"),
]
navigation = st.navigation(pages)

with st.sidebar:
    st.markdown(logo_html(), unsafe_allow_html=True)
    fichier = st.file_uploader("Fichier Maroclear (REPV_….zip)", type=["zip", "xml", "xlsx"])
    resume = st.empty()

if fichier is not None:
    try:
        charger_fichier(fichier.name, fichier.getvalue())
    except Exception as e:
        st.error(f"Lecture impossible : {e}")
if "nom_fichier" in st.session_state:
    r = st.session_state["rapport"]
    resume.caption(f"**{st.session_state['nom_fichier']}**  \n"
                   f"{r['etapes'][-1]['lignes']:,} titres en vie".replace(",", " "))

navigation.run()
