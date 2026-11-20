# SpreadABB

Application Streamlit de calcul des **spreads d'émission** des titres de dette marocains (CD, BSF, BT, BDT,
obligations ordinaires, subordonnées, perpétuelles, convertibles, titrisations) contre la courbe des taux
BDT de Bank Al-Maghrib.

## Lancement local

```bash
pip install -r requirements.txt
streamlit run app.py
```

Secrets (`.streamlit/secrets.toml`, jamais commité) :

```toml
USERNAME = "..."
PASSWORD = "..."

[supabase]
url = "https://xxxx.supabase.co"
key = "..."
```

## Organisation

```
app.py                 connexion, navigation, dépôt du fichier Maroclear
vues/                  pages : tableau de bord, univers des titres, spreads, émetteurs, courbe, risque, import
spreadabb/
  maroclear.py         lecture du REPV_*.zip / .xml, nettoyage, renommage, classement des titres
  referentiels.py      libellés des codes Maroclear, secteurs, émetteurs bancaires
  courbe.py            courbe d'une date : historique des taux, sinon site de Bank Al-Maghrib
  bam.py               téléchargement de la courbe BAM et cache (disque + table Supabase bdt_curves)
  taux.py              interpolation (méthode Bank Al-Maghrib : monétaire < 1 an, actuariel au-delà)
  spreads.py           spread de chaque titre selon sa nature
  export.py            classeur Excel (détail + récapitulatifs)
data/historique_taux.csv   historique des taux BDT (piliers 13 semaines à 30 ans, depuis fin 2014)
sync_bdt_curves.py     remplit le cache Supabase depuis un poste qui accède au site de BAM
tests/                 contrôles des règles de calcul
```

## Traitement du fichier Maroclear

Le fichier est déposé tel qu'il est reçu (`REPV_0001_ALL_jjmmaaaa.zip`). Le bloc `<Debts>` est lu avec
ses dates de coupon, puis : colonnes renommées en français, espaces et cellules vides nettoyés, dates et
nombres typés, doublons d'ISIN supprimés, taux saisis en points de base corrigés, montant émis
reconstitué, codes traduits (type de taux, remboursement, périodicité, statut), nature déduite
(ordinaire, subordonnée, perpétuelle, convertible, titrisation), prochain coupon et prochaine révision.
Les anomalies sont signalées sans supprimer la ligne.

## Courbe BDT

Pour chaque date d'émission :
1. l'historique des taux (`data/historique_taux.csv`) si la date y figure ;
2. sinon la courbe publiée par Bank Al-Maghrib pour cette date (cache `data/courbes_bam/`, table
   Supabase `bdt_curves`, puis site BAM),
   interpolée comme dans le pricer : date de base = date de valeur la plus récente du tableau,
   maturité = échéance − date de base ;
3. si BAM n'a rien publié (week-end, jour férié), jour ouvré précédent, jusqu'à 10 jours.

Le site de BAM refuse les connexions venant de Streamlit Cloud (HTTP 403) : l'application en ligne lit
alors les caches, que `sync_bdt_curves.py` complète depuis un poste du bureau :

```bash
python sync_bdt_curves.py --maroclear REPV_0001_ALL_13112026.zip
```

## Spread selon la nature du titre

Spread (pb) = taux facial − taux BDT de la courbe de la date d'émission, pour la durée suivante :

| Titre | Durée de référence |
|---|---|
| Taux fixe, in fine | de l'émission à l'échéance |
| Taux fixe, amortissable | durée de vie moyenne (remboursement par parts égales) |
| Taux révisable | période de révision (= périodicité du coupon) |
| CD, BSF, BT | de l'émission à l'échéance (taux monétaire sous un an) ; période de révision si révisable |
| Zéro-coupon, taux fictif | pas de spread |
| Bon du Trésor | pas de spread (c'est la courbe de référence) |

Aucune borne n'est appliquée aux spreads. Les fonds de titrisation (FT / FPCT) sont retirés au chargement.

## Émetteurs

Encours en circulation = nominal actuel × nombre de titres. La page Émetteurs donne pour chaque émetteur
son encours, sa part, ses instruments, sa maturité résiduelle et son spread moyen pondérés par l'encours,
puis le détail : répartition par instrument, type de taux et mode de remboursement, échéancier et titres.
