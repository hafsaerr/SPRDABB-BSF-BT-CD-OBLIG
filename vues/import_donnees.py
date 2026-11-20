import pandas as pd
import streamlit as st

from vues.commun import entete, nb

entete("Import et nettoyage",
       "Le fichier Maroclear se dépose tel qu'il est reçu (REPV_….zip) dans le panneau de gauche.")

r = st.session_state.get("rapport")
if not r:
    st.info("Aucun fichier traité pour l'instant.")
    st.stop()

NOMS = {"Debts": "Titres de dette", "Eqties": "Actions", "CPNS": "Coupons et dividendes", "RHTS": "Droits",
        "MUFUNDS": "OPCVM"}


def grille(lignes):
    st.dataframe(pd.DataFrame(lignes, columns=["", "Nombre"]), hide_index=True, width="stretch",
                 column_config={"Nombre": st.column_config.NumberColumn(format="%d")})


st.subheader(r["fichier"])
a, b = st.columns(2)
with a.container(border=True):
    st.markdown("**Contenu du fichier**")
    grille([(NOMS.get(k, k), v) for k, v in r["blocs"].items()])
    st.markdown("**Étapes du traitement**")
    grille([(e["etape"], e["lignes"]) for e in r["etapes"]])
with b.container(border=True):
    st.markdown("**Anomalies relevées** (titres signalés, non supprimés)")
    grille([(k, v) for k, v in r["anomalies"].items() if v])
    c = r.get("courbes", {})
    st.markdown("**Origine des courbes BDT utilisées**")
    grille([("Historique des taux", c.get("historique", 0)), ("Site Bank Al-Maghrib", c.get("site_bam", 0)),
            ("Sans courbe", c.get("sans_courbe", 0)), ("Courbes téléchargées pendant ce traitement", c.get("telechargees", 0))])
    if c.get("site_bloque"):
        st.warning(f"Le site de Bank Al-Maghrib a refusé la connexion ({c['site_bloque']}) : seules les courbes de "
                   "l'historique et du cache ont été utilisées. Lancer sync_bdt_curves.py depuis un poste du bureau "
                   "pour compléter le cache.")

t = st.session_state["titres"]
anom = t[t["anomalies"] != ""]
with st.expander(f"Titres signalés ({nb(len(anom))})"):
    st.dataframe(anom[["isin", "libelle", "statut", "anomalies"]], hide_index=True, width="stretch")
