"""
backfill_combustivel.py
Run ONCE locally to populate historical fuel data for the last 60 days.

  python backfill_combustivel.py

Requires DATABASE_URL env var (same as the scrapers).
- Brent USD/EUR : daily data via yfinance (all 60 days)
- Gasolina95/Gasoleo PT : weekly from caetano.pt current page for the current
  week; remaining weeks are filled with NULL (they accumulate from now on).
  If you want to add historical PT prices manually, edit the HISTORICO dict below.
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

# ── OPTIONAL: add known historical weekly PT prices here ─────────────────────
# Format: "YYYY-MM-DD" (any day of that week) -> (gasolina95, gasoleo)
# The script will apply each price to all days of that ISO week.
# Values already in the DB (e.g. today) are never overwritten.
HISTORICO_PT = {
    # Example — uncomment and fill in if you have the data:
    # "2026-03-24": (1.889, 1.845),
    # "2026-03-31": (1.901, 1.860),
    # "2026-04-07": (1.923, 1.878),
    # "2026-04-14": (1.945, 1.912),
    # "2026-04-21": (1.958, 1.930),
    # "2026-04-28": (1.967, 1.950),
    # "2026-05-05": (1.972, 1.961),
}


def get_current_pt_prices():
    """Get this week's PT prices from caetano.pt (same logic as main scraper)."""
    PRECO_MIN, PRECO_MAX = 0.9, 3.5
    url = "https://caetano.pt/blog/preco-dos-combustiveis-esta-semana/"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        g95, gas = None, None

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

        if g95 is None or gas is None:
            texto = soup.get_text()
            for pattern, target in [(r"gasolina\s*95[^\n]{0,60}?(\d+[.,]\d+)", "g95"),
                                    (r"gas[oó]leo[^\n]{0,60}?(\d+[.,]\d+)", "gas")]:
                for hit in re.findall(pattern, texto, re.IGNORECASE):
                    val = round(float(hit.replace(",", ".")), 3)
                    if PRECO_MIN <= val <= PRECO_MAX:
                        if target == "g95" and g95 is None:
                            g95 = val
                        elif target == "gas" and gas is None:
                            gas = val
                        break
        return g95, gas
    except Exception as e:
        print(f"Aviso: não foi possível obter preços PT actuais: {e}")
        return None, None


# ── 1. Get this week's PT prices ─────────────────────────────────────────────
print("A obter preços PT desta semana...")
g95_current, gas_current = get_current_pt_prices()
print(f"  Gasolina 95: {g95_current}  |  Gasóleo: {gas_current}")

# Add current week to the history dict
today = date.today()
current_week_monday = today - timedelta(days=today.weekday())
HISTORICO_PT[str(current_week_monday)] = (g95_current, gas_current)

# Build day -> (g95, gasoleo) map from weekly data
# Each weekly entry applies to Mon-Sun of that ISO week
pt_by_day = {}
for week_date_str, prices in HISTORICO_PT.items():
    week_start = date.fromisoformat(week_date_str)
    week_monday = week_start - timedelta(days=week_start.weekday())
    for i in range(7):
        d = week_monday + timedelta(days=i)
        pt_by_day[d] = prices

# ── 2. Download 60 days of Brent + EUR/USD via yfinance ──────────────────────
print("\nA descarregar histórico Brent (60 dias)...")
brent_df = yf.download("BZ=F",    period="65d", auto_adjust=True, progress=False)
fx_df    = yf.download("EURUSD=X", period="65d", auto_adjust=True, progress=False)

if brent_df.empty:
    print("ERRO: yfinance não devolveu dados do Brent.")
    exit(1)

# Flatten multi-level columns if present
if isinstance(brent_df.columns, pd.MultiIndex):
    brent_df.columns = brent_df.columns.get_level_values(0)
if isinstance(fx_df.columns, pd.MultiIndex):
    fx_df.columns = fx_df.columns.get_level_values(0)

brent_close = brent_df["Close"].rename("brent_usd")
fx_close    = fx_df["Close"].rename("eurusd").reindex(brent_close.index, method="ffill")

df = pd.concat([brent_close, fx_close], axis=1).dropna(subset=["brent_usd"])
df["brent_usd"] = df["brent_usd"].round(2)
df["brent_eur"] = (df["brent_usd"] / df["eurusd"]).round(2)

print(f"  {len(df)} dias de Brent encontrados ({df.index[0].date()} a {df.index[-1].date()})")

# ── 3. Insert into Supabase ──────────────────────────────────────────────────
print("\nA inserir na base de dados...")
conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

inserted = skipped = 0

for ts, row in df.iterrows():
    d = ts.date()
    brent_usd = float(row["brent_usd"])
    brent_eur = float(row["brent_eur"]) if not pd.isna(row["brent_eur"]) else None
    g95, gas  = pt_by_day.get(d, (None, None))

    cur.execute("""
        INSERT INTO combustivel_precos (data, brent_usd, brent_eur, gasolina95, gasoleo)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (data) DO NOTHING
    """, (d, brent_usd, brent_eur, g95, gas))

    if cur.rowcount:
        inserted += 1
        pt_str = f"G95={g95} Gas={gas}" if g95 else "PT=—"
        print(f"  {d}: Brent={brent_usd:.2f}$ / {brent_eur:.2f}€  {pt_str}")
    else:
        skipped += 1

conn.commit()
conn.close()
print(f"\nBackfill concluído: {inserted} dias inseridos, {skipped} já existiam.")
print("Abre o dashboard — o gráfico do Brent já deve ter o histórico completo!")
