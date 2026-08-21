"""
Preenche o historico de combustivel desde uma semana antes do conflito do Irao.
Corre uma vez (via GitHub Actions > Backfill Combustivel). O Brent vem diario
do yfinance; os precos PT sao medias semanais e aplicam-se aos 7 dias da semana.
"""

import os
import re
import requests
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from datetime import date, timedelta
import yfinance as yf

DATABASE_URL = os.getenv("DATABASE_URL")

# preco semanal PT por semana (qualquer dia serve para identificar a semana ISO).
# valores das medias nacionais publicadas em razaoautomovel.com / caetano.pt
HISTORICO_PT = {
    "2026-02-16": (1.681, 1.589),   # semana antes do conflito (interpolado)
    "2026-02-23": (1.684, 1.598),   # conflito comeca a 28 fev
    "2026-03-02": (1.700, 1.628),
    "2026-03-09": (1.772, 1.807),   # primeiro grande salto no gasoleo
    "2026-03-16": (1.939, 1.964),
    "2026-03-23": (1.918, 2.037),   # gasoleo passa os 2 EUR
    "2026-03-30": (1.920, 2.075),
    "2026-04-06": (1.948, 2.145),   # pico historico do gasoleo
    "2026-04-13": (1.920, 2.090),
    "2026-04-20": (1.898, 1.988),
    "2026-04-27": (1.927, 1.958),
    "2026-05-04": (1.999, 2.058),
    "2026-05-11": (1.979, 1.968),
}

PRECO_MIN, PRECO_MAX = 0.9, 3.5


def precos_desta_semana():
    url = "https://caetano.pt/blog/preco-dos-combustiveis-esta-semana/"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        g95 = gas = None

        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            m = re.search(r"(\d+[.,]\d+)", cells[1].get_text(strip=True))
            if not m:
                continue
            val = round(float(m.group(1).replace(",", ".")), 3)
            if not (PRECO_MIN <= val <= PRECO_MAX):
                continue
            if "gasolina 95" in label and g95 is None:
                g95 = val
            elif ("gasóleo" in label or "gasoleo" in label) and gas is None:
                gas = val
        return g95, gas
    except Exception as e:
        print(f"Nao consegui os precos desta semana: {e}")
        return None, None


g95, gas = precos_desta_semana()
print(f"Esta semana: gasolina {g95} | gasoleo {gas}")

hoje = date.today()
HISTORICO_PT[str(hoje - timedelta(days=hoje.weekday()))] = (g95, gas)

# espalha cada preco semanal pelos 7 dias da respetiva semana
pt_por_dia = {}
for dia_str, precos in HISTORICO_PT.items():
    segunda = date.fromisoformat(dia_str)
    segunda -= timedelta(days=segunda.weekday())
    for i in range(7):
        pt_por_dia[segunda + timedelta(days=i)] = precos

print("A descarregar Brent (90 dias)...")
brent_df = yf.download("BZ=F", period="95d", auto_adjust=True, progress=False)
fx_df = yf.download("EURUSD=X", period="95d", auto_adjust=True, progress=False)

if brent_df.empty:
    print("yfinance nao devolveu Brent.")
    exit(1)

if isinstance(brent_df.columns, pd.MultiIndex):
    brent_df.columns = brent_df.columns.get_level_values(0)
if isinstance(fx_df.columns, pd.MultiIndex):
    fx_df.columns = fx_df.columns.get_level_values(0)

brent = brent_df["Close"].rename("brent_usd")
fx = fx_df["Close"].rename("eurusd").reindex(brent.index, method="ffill")

df = pd.concat([brent, fx], axis=1).dropna(subset=["brent_usd"])
df["brent_usd"] = df["brent_usd"].round(2)
df["brent_eur"] = (df["brent_usd"] / df["eurusd"]).round(2)
print(f"{len(df)} dias ({df.index[0].date()} a {df.index[-1].date()})")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# COALESCE para nunca sobrescrever um valor real com um NULL do backfill
for ts, row in df.iterrows():
    d = ts.date()
    brent_eur = None if pd.isna(row["brent_eur"]) else float(row["brent_eur"])
    g95, gas = pt_por_dia.get(d, (None, None))
    cur.execute("""
        INSERT INTO combustivel_precos (data, brent_usd, brent_eur, gasolina95, gasoleo)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (data) DO UPDATE SET
            brent_usd  = COALESCE(EXCLUDED.brent_usd,  combustivel_precos.brent_usd),
            brent_eur  = COALESCE(EXCLUDED.brent_eur,  combustivel_precos.brent_eur),
            gasolina95 = COALESCE(EXCLUDED.gasolina95, combustivel_precos.gasolina95),
            gasoleo    = COALESCE(EXCLUDED.gasoleo,    combustivel_precos.gasoleo)
    """, (d, float(row["brent_usd"]), brent_eur, g95, gas))

conn.commit()
conn.close()
print(f"Feito: {len(df)} dias gravados.")
