"""Référentiels : libellés des codes Maroclear, colonnes renommées, secteurs, banques."""

# Colonnes du bloc <Debts> : code Maroclear -> (nom interne, libellé affiché)
COLONNES_MAROCLEAR = {
    "INSTRID": ("isin", "Code ISIN"),
    "MNEMONIQUE": ("mnemonique", "Mnémonique"),
    "ENGLONGNAME": ("libelle", "Libellé"),
    "ENGPREFERREDNAME": ("libelle_court", "Libellé court"),
    "INSTRCTGRY": ("categorie_code", "Catégorie (code)"),
    "INSTRTYPE": ("type_code", "Type (code)"),
    "ISSUERCD": ("emetteur_code", "Code émetteur"),
    "PREFERREDNAMEISSUER": ("emetteur", "Émetteur"),
    "PREFERREDNAMEREGISTRAR": ("domiciliataire", "Établissement domiciliataire"),
    "AGENTID": ("domiciliataire_code", "Code domiciliataire"),
    "ISSUEDT": ("date_emission", "Date d'émission"),
    "MATURITYDT_L": ("date_echeance", "Date d'échéance"),
    "PRMYDTLSDUMMYDATE1": ("date_jouissance", "Date de jouissance"),
    "PARVALUE": ("nominal", "Nominal d'origine"),
    "NEWPARVALUE": ("nominal_actuel", "Nominal actuel"),
    "ISSUESIZE": ("nb_titres", "Nombre de titres émis"),
    "ISSUECAPITAL": ("montant_emis", "Montant émis (MAD)"),
    "INTERESTTYPE": ("type_taux_code", "Type de taux (code)"),
    "INTERESTRATE": ("taux_facial", "Taux facial"),
    "INTERESTPERIODCTY": ("periodicite_code", "Périodicité coupon (code)"),
    "REDEMPTIONTYPE": ("remboursement_code", "Remboursement (code)"),
    "AMORTFREQ": ("frequence_amort_code", "Fréquence d'amortissement (code)"),
    "GUARANTEE": ("garantie_code", "Garantie (code)"),
    "FORM": ("forme_code", "Forme (code)"),
    "EXCHIND": ("cote_code", "Coté en bourse (code)"),
    "INSTRSTATUS": ("statut_code", "Statut (code)"),
}

# Colonnes calculées : nom interne -> libellé affiché
COLONNES_CALCULEES = {
    "instrument": "Instrument",
    "famille": "Famille",
    "nature": "Nature",
    "type_taux": "Type de taux",
    "remboursement": "Remboursement",
    "periodicite": "Périodicité coupon",
    "frequence_amort": "Fréquence d'amortissement",
    "forme": "Forme",
    "cote": "Coté en bourse",
    "statut": "Statut",
    "en_vie": "En vie",
    "secteur": "Secteur",
    "banque": "Émetteur bancaire",
    "maturite_initiale_j": "Maturité initiale (jours)",
    "maturite_initiale_ans": "Maturité initiale (ans)",
    "maturite_residuelle_ans": "Maturité résiduelle (ans)",
    "tranche_maturite": "Tranche de maturité",
    "nb_coupons": "Nombre de coupons",
    "dernier_coupon": "Dernier coupon payé",
    "prochain_coupon": "Prochain coupon",
    "coupons_restants": "Coupons restants",
    "prochaine_revision": "Prochaine révision du taux",
    "anomalies": "Anomalies relevées",
}

LIBELLES = {interne: lib for interne, lib in COLONNES_MAROCLEAR.values()} | COLONNES_CALCULEES

CATEGORIES = {
    "TCN": "Titre de créance négociable",
    "BDT": "Bon du Trésor",
    "OBL_ORDN": "Obligation ordinaire",
    "OBL_SUBD": "Obligation subordonnée",
    "OBL_CONV": "Obligation convertible",
    "FPCT": "Titrisation (FT / FPCT)",
}

TYPES_TAUX = {"FIXD": "Fixe", "FLTG": "Révisable", "FLOT": "Révisable", "DISC": "Zéro-coupon"}

REMBOURSEMENTS = {"BLET": "In fine", "AMOR": "Amortissable", "INST": "Par tranches"}

PERIODICITES = {
    "ANLY": "Annuelle", "HFLY": "Semestrielle", "SMAL": "Semestrielle",
    "QTLY": "Trimestrielle", "BMLY": "Bimestrielle",
    "MNLY": "Mensuelle", "MNTH": "Mensuelle", "ONRD": "À l'échéance",
}

# Nombre de mois entre deux paiements
MOIS_PAR_PERIODE = {"ANLY": 12, "HFLY": 6, "SMAL": 6, "QTLY": 3, "BMLY": 2, "MNLY": 1, "MNTH": 1}

FORMES = {"BR": "Au porteur", "RGD": "Nominatif", "BRDR": "Porteur ou nominatif"}

STATUTS = {"ACTI": "Actif", "DLET": "Radié", "INVD": "Invalide", "ICPT": "En création"}

# Émetteurs bancaires (codes PREFERREDNAMEISSUER)
BANQUES = {
    "ATW E", "BCP E", "CIH E", "BOA", "CAM E", "CDM", "CDG E", "CDG K E",
    "CFG BANK", "CFC", "CFM", "SAHAM BANK E", "SAHAM FINANCES", "BMCI",
    "BMCI LEASI", "AL BARID BANK E", "CREDIPER FT", "BAD",
}

# Regroupement des établissements domiciliataires sous un nom court
ALIAS_BANQUES = [
    ("CREDIT AGRICOLE", "CAM"), ("CREDIT AG", "CAM"), ("CAM E", "CAM"),
    ("CREDIT DU MAROC", "CDM"), ("ATTIJARIWAFA", "ATW"), ("ATW", "ATW"),
    ("BANK OF AFRICA", "BOA"), ("BMCE", "BOA"), ("BOZ", "BOA"),
    ("SGMB", "SAHAM"), ("SAHAM", "SAHAM"), ("BCP", "BCP"), ("CIH", "CIH"),
]

SECTEURS = {
    # Énergie
    "ONEE": "ÉNERGIE", "TAQA MOROCCO": "ÉNERGIE", "MASEN": "ÉNERGIE", "FEC": "ÉNERGIE",
    "LYDEC": "ÉNERGIE", "SAMIR": "ÉNERGIE", "SEDM": "ÉNERGIE", "TMPA": "ÉNERGIE",
    "AFRIQUIA GAZ": "ÉNERGIE", "AFIQUIA LUB": "ÉNERGIE", "MAGHREB OXYGENE": "ÉNERGIE",
    "PHOSPH BOUCRAA": "ÉNERGIE",
    # BTP
    "TGCC": "BTP", "JET CONTRACTORS": "BTP", "ADI": "BTP", "SETTAVEX": "BTP",
    "CMGP GROUP": "BTP", "ASSIFILL BUILD": "BTP", "BUILDING LOGIST": "BTP",
    "NADOR WEST MED": "BTP", "SOMAGEC": "BTP",
    # Cimenterie
    "HOLCIM MAROC": "CIMENTERIE", "CIMAT": "CIMENTERIE",
    # Immobilier
    "CGI": "IMMOBILIER", "ALLIANCES DARNA": "IMMOBILIER", "DOUJA PROM ADD": "IMMOBILIER",
    "AL OMRANE": "IMMOBILIER", "IMMOLOG": "IMMOBILIER", "ARADEI CAPITAL": "IMMOBILIER",
    "BEST REAL ESTAT": "IMMOBILIER", "HAYAN IMMO SA": "IMMOBILIER", "PALME DEV": "IMMOBILIER",
    "CMR LSTONE": "IMMOBILIER", "Immorent SC": "IMMOBILIER", "RDS": "IMMOBILIER",
    "ALH REAL ESTATE": "IMMOBILIER", "CDG Developp": "IMMOBILIER",
    # Transport
    "ONCF": "TRANSPORT", "ONDA": "TRANSPORT", "CTM SA": "TRANSPORT", "COMANAV": "TRANSPORT",
    "TANGER MED SA": "TRANSPORT", "TMSA": "TRANSPORT", "ANP": "TRANSPORT", "ADM": "TRANSPORT",
    "MEDIACO MAROC": "TRANSPORT", "CMT": "TRANSPORT",
    # Télécoms
    "MEDI TELCOM SA": "TÉLÉCOMS", "IAM": "TÉLÉCOMS",
    # Chimie / mines
    "OCP SA": "CHIMIE / MINES", "OCP NUTRICROPS": "CHIMIE / MINES", "MANAGEM": "CHIMIE / MINES",
    # Agroalimentaire
    "OULMES": "AGROALIMENTAIRE", "UNIMER": "AGROALIMENTAIRE", "AFRICA FEED FOOD": "AGROALIMENTAIRE",
    "SOMACOVAM": "AGROALIMENTAIRE", "MUTANDIS SCA": "AGROALIMENTAIRE", "ZALAGH HOLDING": "AGROALIMENTAIRE",
    # Autres secteurs
    "MAGHREB STEEL": "SIDÉRURGIE", "AKDITAL": "SANTÉ",
    "RISMA SA": "TOURISME", "MADAEF": "TOURISME",
    "LABEL VIE": "GRANDE DISTRIBUTION", "MARJANE HOLDING": "GRANDE DISTRIBUTION",
    "RETAIL HOLDING": "GRANDE DISTRIBUTION",
    "AL MADA": "HOLDING", "HOLMARCOM FIN CO": "HOLDING", "FINANCECOM": "HOLDING",
    "FINANCIER SEC": "HOLDING", "O CAPITAL GROUP": "HOLDING", "GROUP INVEST SA": "HOLDING",
    "GARAN": "HOLDING", "Hold GENE EDU": "HOLDING", "OLEA CAPITAL": "HOLDING",
    "MAGHREBAIL": "LEASING", "MA LEASING": "LEASING", "SAHAM LEASING": "LEASING",
    "WAFABAIL": "LEASING", "SALAFIN": "CRÉDIT À LA CONSOMMATION",
    "WAFASALAF": "CRÉDIT À LA CONSOMMATION", "TASLIF": "CRÉDIT À LA CONSOMMATION",
    "SOFAC CREDIT": "CRÉDIT À LA CONSOMMATION", "VIVALIS SALAF": "CRÉDIT À LA CONSOMMATION",
    "SALAF INVEST": "CRÉDIT À LA CONSOMMATION", "DAR SALAF": "CRÉDIT À LA CONSOMMATION",
    "AUTO MOBILITY": "CRÉDIT À LA CONSOMMATION",
    "SAHAM": "ASSURANCE",
    "JAIDA": "MICROFINANCE", "SFI": "MICROFINANCE",
    "SCIF": "PARAPUBLIC", "ORMVAD": "PARAPUBLIC", "COMMUNE AGADIR": "PARAPUBLIC",
    "BEST FINANCIERE": "PARAPUBLIC", "AGRI CAPITAL": "PARAPUBLIC", "Region de Casa": "PARAPUBLIC",
    "JIWD": "PARAPUBLIC", "DAMANETAMAYOUZ": "PARAPUBLIC",
    "RCI": "AUTOMOBILE", "UNIVERS MOTORS": "AUTOMOBILE", "AUTO NEJMA": "AUTOMOBILE", "OD MAROC": "AUTOMOBILE",
    "Univ Mohammed VI": "ÉDUCATION",
    "DISTRA-S.A": "TEXTILE", "MCM": "TEXTILE",
    "CMB PLA MAROC": "INDUSTRIE", "TC3PC": "INDUSTRIE", "FINAN HATT": "INDUSTRIE",
    "TRESOR": "ÉTAT",
}


def secteur_de(emetteur: str, categorie_code: str) -> str:
    """Secteur d'un émetteur ; les fonds de titrisation et les banques ont leur propre classe."""
    emetteur = " ".join((emetteur or "").split())
    if emetteur in BANQUES:
        return "BANQUE"
    if emetteur in SECTEURS:
        return SECTEURS[emetteur]
    if categorie_code == "FPCT" or emetteur.upper().startswith(("FT ", "MGT", "COMP", "COPMIFT", "ATT TITRI")):
        return "TITRISATION"
    return "AUTRES"


def nom_court_banque(nom: str) -> str:
    u = (nom or "").strip().upper()
    for cle, court in ALIAS_BANQUES:
        if cle in u:
            return court
    return (nom or "").strip()
