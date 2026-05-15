import yfinance as yf
import psycopg2
import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import date

# Fonte: blog semanal com preços oficiais PT (atualizado todas as semanas)
CAETANO_URL = "https://caetano.pt/blog/preco-dos-combustiveis-esta-semana/"


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


def get_pt_fuel_prices():
    """
    Scrape preços semanais PT de caetano.pt.
    A página publica uma tabela com Gasolina 95 e Gasóleo todas as semanas.
    """
    gasolina95 = None
    gasoleo = None
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(CAETANO_URL, headers=headers, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Método 1: procura em linhas de tabela
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                valor_txt = cells[1].get_text(strip=True)
                match = re.search(r"(\d+[.,]\d+)", valor_txt)
                if match:
                    val = round(float(match.group(1).replace(",", ".")), 3)
                    if "gasolina 95" in label or "gasolina95" in label:
                        gasolina95 = val
                    elif "gasóleo" in label or "gasoleo" in label or "diesel" in label:
                        gasoleo = val

        # Método 2: fallback por regex no texto completo da página
        if gasolina95 is None or gasoleo is None:
            texto = soup.get_text()
            if gasolina95 is None:
                m = re.search(r"gasolina\s*95[^\d]*(\d+[.,]\d+)", texto, re.IGNORECASE)
                if m:
                    gasolina95 = round(float(m.group(1).replace(",", ".")), 3)
            if gasoleo is None:
                m = re.search(r"gas[oó]leo[^\d]*(\d+[.,]\d+)", texto, re.IGNORECASE)
                if m:
                    gasoleo = round(float(m.group(1).replace(",", ".")), 3)

    except Exception as e:
        print(f"Erro ao obter preços PT: {e}")

    return gasolina95, gasoleo


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
gasolina95, gasoleo = get_pt_fuel_prices()

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
