<<<<<<< HEAD
# SpreadABB — Spread Manager · Al Barid Bank

Application Streamlit de calcul de spread taux pour les instruments du marché obligataire marocain (BSF, CD, BT, Obligations) contre la courbe BDT de Bank Al-Maghrib.

---

## Fonctionnalités

- **CD** (Certificats de Dépôt) et **BSF** (Bons de Sociétés Financières) — `INSTRCTGRY = TCN`
- **BT** (Billets de Trésorerie) — `INSTRCTGRY = BDT`
- **OBLIG_ORDN** (Obligations Ordinaires hors secteur bancaire) — `INSTRCTGRY = OBL_ORDN`
- Récupération automatique des courbes BDT depuis [bkam.ma](https://www.bkam.ma) avec cache local
- Interpolation selon la logique VBA officielle BAM
- Détection automatique du secteur (ENERGIE, TRANSPORT, IMMOBILIER…)
- Export Excel structuré : feuilles par type/banque (CD/BSF/BT) ou par secteur (OBLIG_ORDN)

---

## Installation locale

```bash
git clone https://github.com/<votre-org>/spreadABB.git
cd spreadABB
pip install -r requirements.txt
py -m streamlit run app.py
```

L'application sera accessible sur **http://localhost:8501**.

### Authentification locale

Créez `.streamlit/secrets.toml` (non commité) :

```toml
USERNAME = "spreadABB"
PASSWORD = "albarid2026"
```

---

## Déploiement sur Streamlit Cloud

1. Poussez le repo sur GitHub
2. Connectez-vous sur [share.streamlit.io](https://share.streamlit.io)
3. Nouveau déploiement → sélectionnez le repo + branche + fichier : `app.py`
4. Dans **Secrets**, ajoutez :
   ```toml
   USERNAME = "spreadABB"
   PASSWORD = "albarid2026"
   ```
5. Déployez

---

## Structure du projet

```
spread_manager/
├── app.py                        # Point d'entrée Streamlit (login + navigation)
├── pages/
│   ├── 01_Accueil.py             # Page d'accueil (multi-page Streamlit)
│   └── 02_Calculateur_Spread.py  # Page calculateur
├── core/
│   ├── bam_curve_fetcher.py      # Re-export BamCurveFetcher
│   ├── spread_calculator.py      # Re-export calcul_taux
│   ├── sector_mapper.py          # SECTEUR_MAP + detect_sector
│   └── excel_exporter.py         # Fonctions export Excel pures
├── bam_curve_fetcher.py          # Scraping BAM + cache async
├── vba_equivalent_rates.py       # Interpolation taux (logique VBA BAM)
├── maroclear_reader.py           # Lecture/filtrage fichier Maroclear
├── assets/
│   └── ALBARID.png               # Logo Al Barid Bank
├── cache_bam_curves/             # Cache des courbes BDT (auto-créé)
├── cache/
│   └── .gitkeep
├── requirements.txt
├── .streamlit/
│   ├── config.toml               # Thème (orange/noir)
│   └── secrets.toml              # Credentials (NE PAS COMMITER)
└── .gitignore
```

---

## Format du fichier Maroclear attendu

Fichier Excel `.xlsx` exporté depuis Maroclear (format `REPVS*.xlsx`).

Colonnes obligatoires :

| Colonne | Description |
|---|---|
| `INSTRID` | Identifiant unique de l'instrument |
| `INSTRCTGRY` | Catégorie : `TCN`, `BDT`, `OBL_ORDN`… |
| `ENGLONGNAME` | Nom long de l'instrument |
| `ENGPREFERREDNAME` | Nom court de l'instrument |
| `PREFERREDNAMEISSUER` | Nom de l'émetteur |
| `ISSUEDT` | Date d'émission |
| `MATURITYDT_L` | Date d'échéance |
| `INTERESTRATE` | Taux d'intérêt nominal |

---

## Variables d'environnement / Secrets

| Variable | Description | Valeur par défaut |
|---|---|---|
| `USERNAME` | Login de l'application | `spreadABB` |
| `PASSWORD` | Mot de passe de l'application | *(non communiqué)* |

---

## Dépendances principales

| Package | Usage |
|---|---|
| `streamlit` | Interface web |
| `pandas` | Manipulation des données |
| `openpyxl` | Lecture/écriture Excel |
| `aiohttp` | Téléchargement async des courbes BAM |
| `beautifulsoup4` | Scraping bkam.ma |
| `python-dateutil` | Calculs de maturité |
=======
# SPRDABB-BSF-BT-CD-OBLIG
SPREAD DES BT BSF CD OBLIG
>>>>>>> 0598971e930530cdec79064431656aaa9a65f772
