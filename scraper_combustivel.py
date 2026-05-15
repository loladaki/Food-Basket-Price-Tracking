import yfinance as yf
import psycopg2
import os
import requests
from datetime import date

# DGEG combustivel IDs: 3 = Gasolina 95, 16 = Gasóleo simples
DGEG_URL = "https://www.precoscombustiveis.dgeg.gov.pt/api/PrecoComb/GetMediaNacional"


def get_brent_price():
    try:
        brent = yf.Ticker("BZ=F")
        hist = brent.history(period="5d")
        if hist.empty:
            return None, None
        price_usd = round(float(hist["Close"].iloc[-1]), 2)

        eurusd = yf.Ticker("EURUSD=X")
        fx_hist = eurusd.history(period="5d")
        if fx_hist.empty:
            return price_usd, None
        fx_rate = float(fx_hist["Close"].iloc[-1])
        price_eur = round(price_usd / fx_rate, 2)

        return price_usd, price_eur
    except Exception as e:
        print(f"Erro Brent: {e}")
        return None, None


def get_dgeg_price(combustivel_id):
    try:
        params = {"combustivel": combustivel_id, "pais": 0}
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        r = requests.get(DGEG_URL, params=params, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            # A resposta da DGEG vem com campo "resultado" ou "data" com o preço médio
            if isinstance(data, list) and data:
                preco = data[0].get("PrecoMedio") or data[0].get("preco") or data[0].get("Preco")
                if preco:
                    return round(float(str(preco).replace(",", ".")), 3)
            elif isinstance(data, dict):
                preco = data.get("PrecoMedio") or data.get("preco") or data.get("Preco")
                if preco:
                    return round(float(str(preco).replace(",", ".")), 3)
    except Exception as e:
        print(f"Erro DGEG (id={combustivel_id}): {e}")
    return None


def get_fallback(cursor):
    cursor.execute("""
        SELECT brent_usd, brent_eur, gasolina95, gasoleo
        FROM combustivel_precos
        ORDER BY data DESC
        LIMIT 1
    """)
    row = cursor.fetchone()
    if row:
        return row
    return None, None, None, None


DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
hoje = date.today()

brent_usd, brent_eur = get_brent_price()
gasolina95 = get_dgeg_price(3)
gasoleo = get_dgeg_price(16)

# FALLBACK se algum valor falhou
fb_brent_usd, fb_brent_eur, fb_gasolina, fb_gasoleo = get_fallback(cursor)

if brent_usd is None and fb_brent_usd:
    brent_usd, brent_eur = fb_brent_usd, fb_brent_eur
    print(f"Fallback Brent: {brent_usd} USD")

if gasolina95 is None and fb_gasolina:
    gasolina95 = fb_gasolina
    print(f"Fallback Gasolina95: {gasolina95} EUR/L")

if gasoleo is None and fb_gasoleo:
    gasoleo = fb_gasoleo
    print(f"Fallback Gasóleo: {gasoleo} EUR/L")

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
print(f"Combustível: Brent={brent_usd} USD / {brent_eur} EUR | Gasolina95={gasolina95} | Gasóleo={gasoleo} ({hoje})")
