import yfinance as yf
import psycopg2
import os
import re
import math
import requests
from bs4 import BeautifulSoup
from datetime import date

CAETANO_URL = "https://caetano.pt/blog/preco-dos-combustiveis-esta-semana/"

# limites de um preco ao litro plausivel; serve para nao apanhar o desconto ISP
# (ex: "7,55 centimos/L") em vez do preco real
PRECO_MIN = 0.9
PRECO_MAX = 3.5


def get_brent_price():
    try:
        hist = yf.Ticker("BZ=F").history(period="5d")
        if hist.empty:
            return None, None
        usd = float(hist["Close"].iloc[-1])
        if not math.isfinite(usd):     # dias sem cotacao vem como NaN
            return None, None
        usd = round(usd, 2)

        fx = yf.Ticker("EURUSD=X").history(period="5d")
        if fx.empty:
            return usd, None
        rate = float(fx["Close"].iloc[-1])
        if not math.isfinite(rate) or rate == 0:
            return usd, None
        return usd, round(usd / rate, 2)
    except Exception as e:
        print(f"Erro Brent: {e}")
        return None, None


def parse_preco(txt):
    m = re.search(r"(\d+[.,]\d+)", txt)
    if m:
        val = round(float(m.group(1).replace(",", ".")), 3)
        if PRECO_MIN <= val <= PRECO_MAX:
            return val
    return None


def primeiro_valido(candidatos):
    for txt in candidatos:
        val = parse_preco(txt)
        if val is not None:
            return val
    return None


def get_precos_pt():
    gasolina95 = gasoleo = None
    try:
        r = requests.get(CAETANO_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            val = parse_preco(cells[1].get_text(strip=True))
            if val is None:
                continue
            if ("gasolina 95" in label or "gasolina95" in label) and gasolina95 is None:
                gasolina95 = val
            elif ("gasóleo" in label or "gasoleo" in label or "diesel" in label) and gasoleo is None:
                gasoleo = val

        if gasolina95 is None or gasoleo is None:
            texto = soup.get_text()
            if gasolina95 is None:
                gasolina95 = primeiro_valido(
                    re.findall(r"gasolina\s*95[^\n]{0,40}?(\d+[.,]\d+)", texto, re.IGNORECASE))
            if gasoleo is None:
                gasoleo = primeiro_valido(
                    re.findall(r"gas[oó]leo[^\n]{0,40}?(\d+[.,]\d+)", texto, re.IGNORECASE))
    except Exception as e:
        print(f"Erro precos PT: {e}")

    return gasolina95, gasoleo


def get_fallback(cursor):
    cursor.execute("""
        SELECT brent_usd, brent_eur, gasolina95, gasoleo
        FROM combustivel_precos
        ORDER BY data DESC
        LIMIT 1
    """)
    return cursor.fetchone() or (None, None, None, None)


DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
hoje = date.today()

brent_usd, brent_eur = get_brent_price()
gasolina95, gasoleo = get_precos_pt()

# rede de seguranca: nunca gravar NaN/inf (o Postgres aceita NaN em numeric e
# depois estraga o site); um valor invalido passa a None e usa-se o fallback
def saneia(v):
    return v if (v is not None and math.isfinite(v)) else None
brent_usd, brent_eur = saneia(brent_usd), saneia(brent_eur)
gasolina95, gasoleo = saneia(gasolina95), saneia(gasoleo)

fb_brent_usd, fb_brent_eur, fb_gasolina, fb_gasoleo = get_fallback(cursor)
if brent_usd is None and fb_brent_usd:
    brent_usd, brent_eur = fb_brent_usd, fb_brent_eur
    print(f"Fallback Brent: {brent_usd} USD")
if gasolina95 is None and fb_gasolina:
    gasolina95 = fb_gasolina
    print(f"Fallback Gasolina95: {gasolina95}")
if gasoleo is None and fb_gasoleo:
    gasoleo = fb_gasoleo
    print(f"Fallback Gasoleo: {gasoleo}")

cursor.execute("DELETE FROM combustivel_precos WHERE data = %s", (hoje,))
cursor.execute("""
    INSERT INTO combustivel_precos (data, brent_usd, brent_eur, gasolina95, gasoleo)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (data) DO UPDATE SET
        brent_usd = EXCLUDED.brent_usd,
        brent_eur = EXCLUDED.brent_eur,
        gasolina95 = EXCLUDED.gasolina95,
        gasoleo = EXCLUDED.gasoleo
""", (hoje, brent_usd, brent_eur, gasolina95, gasoleo))

conn.commit()
conn.close()
print(f"Combustivel: Brent={brent_usd}$ / {brent_eur}EUR | G95={gasolina95} | Gasoleo={gasoleo} ({hoje})")
