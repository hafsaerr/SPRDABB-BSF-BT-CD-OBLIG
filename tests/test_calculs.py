"""Contrôles des règles de nettoyage et de calcul."""
from datetime import date

import pandas as pd

from spreadabb import courbe, maroclear, spreads
from spreadabb.taux import calcul_taux

XML = b"""<REPV><Debts>
<Debt><INSTRID>MA0000000001</INSTRID><INSTRTYPE>FRBD</INSTRTYPE><INSTRCTGRY>OBL_ORDN</INSTRCTGRY>
<ENGPREFERREDNAME>OBL X</ENGPREFERREDNAME><ENGLONGNAME>Obl  X du 01012020 4 00 a 5 ans</ENGLONGNAME>
<PREFERREDNAMEISSUER>ONCF</PREFERREDNAMEISSUER><ISSUEDT>2020-01-02 00:00:00.0</ISSUEDT>
<MATURITYDT_L>2030-01-02 00:00:00.0</MATURITYDT_L><PARVALUE>100000.000</PARVALUE><ISSUESIZE>10.000</ISSUESIZE>
<INTERESTTYPE>FIXD</INTERESTTYPE><INTERESTRATE>400.000</INTERESTRATE><INTERESTPERIODCTY>ANLY</INTERESTPERIODCTY>
<REDEMPTIONTYPE>BLET</REDEMPTIONTYPE><INSTRSTATUS>ACTI</INSTRSTATUS>
<CouponPayDates><CouponPayDate>2025-01-02</CouponPayDate><CouponPayDate>2026-01-02</CouponPayDate></CouponPayDates></Debt>
<Debt><INSTRID>MA0000000002</INSTRID><INSTRTYPE>FLRT</INSTRTYPE><INSTRCTGRY>OBL_SUBD</INSTRCTGRY>
<ENGLONGNAME>Obl SUB Y 020120 PERP 5 00</ENGLONGNAME><PREFERREDNAMEISSUER>ATW E</PREFERREDNAMEISSUER>
<ISSUEDT>2020-01-02 00:00:00.0</ISSUEDT><MATURITYDT_L>2040-01-02 00:00:00.0</MATURITYDT_L>
<INTERESTTYPE>FLTG</INTERESTTYPE><INTERESTRATE>5.000</INTERESTRATE><INTERESTPERIODCTY>ANLY</INTERESTPERIODCTY>
<REDEMPTIONTYPE>BLET</REDEMPTIONTYPE><INSTRSTATUS>ACTI</INSTRSTATUS></Debt>
<Debt><INSTRID>MA0000000002</INSTRID><INSTRCTGRY>TCN</INSTRCTGRY><ENGLONGNAME>doublon</ENGLONGNAME></Debt>
</Debts></REPV>"""

AUJ = date(2025, 6, 1)


def _charger():
    return maroclear.charger("test.xml", XML, AUJ)


def test_nettoyage_et_classement():
    df, cp, rapport = _charger()
    assert len(df) == 2                                   # doublon supprimé
    a = df.set_index("isin").loc["MA0000000001"]
    assert a["libelle"] == "Obl X du 01012020 4 00 a 5 ans"   # espaces superflus retirés
    assert abs(a["taux_facial"] - 0.04) < 1e-12           # 400 pb -> 4 %
    assert a["montant_emis"] == 1_000_000                 # nominal x titres
    assert a["prochain_coupon"] == date(2026, 1, 2)
    assert a["secteur"] == "TRANSPORT"
    b = df.set_index("isin").loc["MA0000000002"]
    assert b["nature"] == "Subordonnée perpétuelle"
    assert b["type_taux"] == "Révisable"
    assert b["banque"]
    assert "Taux saisi en points de base (corrigé)" in rapport.anomalies


def test_tranches():
    assert maroclear.tranche_maturite(91) == "13 semaines"
    assert maroclear.tranche_maturite(364) == "52 semaines"
    assert maroclear.tranche_maturite(365 * 7) == "7 ans"
    assert maroclear.tranche_maturite(365 * 13) == "15 ans"


def test_interpolation_bornes():
    mt, tx = [91, 182, 364, 730], [0.02, 0.021, 0.022, 0.025]
    assert calcul_taux(50, mt, tx, date(2025, 1, 2)) == 0.02
    assert abs(calcul_taux(136.5, mt, tx, date(2025, 1, 2)) - 0.0205) < 1e-9


def test_vie_moyenne():
    # 4 remboursements annuels égaux sur 4 ans : vie moyenne 2,5 ans
    j = spreads.vie_moyenne_jours(date(2020, 1, 1), date(2024, 1, 1), 12)
    assert abs(j / 365.25 - 2.5) < 0.01


def test_spread_selon_nature():
    df, _, _ = _charger()
    histo = pd.DataFrame([{"date": date(2020, 1, 2), "s13": 0.02, "s26": 0.021, "s52": 0.022, "a2": 0.024,
                           "a5": 0.027, "a10": 0.03, "a15": 0.033, "a20": 0.035, "a30": 0.038, "taux_directeur": 0.0225}])
    s = spreads.calculer(df, courbe.Courbes(histo)).set_index("isin")
    fixe = s.loc["MA0000000001"]
    assert fixe["methode"].startswith("Maturité")
    assert abs(fixe["spread_pb"] - 100) < 0.5             # 4 % contre 3 % à 10 ans
    rev = s.loc["MA0000000002"]
    assert rev["methode"].startswith("Révisable")
    assert abs(rev["duree_reference_ans"] - 1) < 0.01     # révision annuelle : courbe à 1 an
