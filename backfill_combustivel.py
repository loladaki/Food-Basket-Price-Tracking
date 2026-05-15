"""
backfill_combustivel.py
Run ONCE (via GitHub Actions > Backfill Combustivel) to populate historical
fuel data going back to 1 week before the Iran conflict (Feb 21, 2026).

- Brent USD/EUR : daily data via yfinance (~90 days)
- Gasolina95/Gasóleo PT : weekly national averages sourced from razaoautomovel.com
  and caetano.pt, covering every week from Sem.8 (Feb 16) to Sem.20 (May 11).
  Each weekly price is applied to all 7 days of that ISO week.
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

# ── Histórico semanal de preços PT (fontes: razaoautomovel.com, caetano.pt) ──
# Formato: "YYYY-MM-DD" (qualquer dia dessa semana) -> (gasolina95, gasoleo)
# Cada entrada aplica-se a todos os dias da semana ISO correspondente.
# Registos já existentes na BD nunca são sobrescritos (ON CONFLICT DO NOTHING).
HISTORICO_PT = {
    # Sem. 8  — 1 semana antes do conflito Irão (início 28 Fev)
    "2026-02-16": (1.681, 1.589),  # estimado por interpolação (semana 2→9)

    # Sem. 9  — conflito inicia 28 Fev (dentro desta semana)
    "2026-02-23": (1.684, 1.598),  # fonte: razaoautomovel semana 9

    # Sem. 10 — primeiro impacto nos preços (+2.9 cênt. diesel, +1.6 gas)
    "2026-03-02": (1.700, 1.628),  # fonte: razaoautomovel semana 10

    # Sem. 11 — grande salto (+17.2 cênt. diesel, +6.7 gas — "aumento histórico")
    "2026-03-09": (1.772, 1.807),  # fonte: razaoautomovel semana 11

    # Sem. 12 — nova subida brusca (+10 cênt. cada)
    "2026-03-16": (1.939, 1.964),  # fonte: pplware / razaoautomovel semana 12

    # Sem. 13 — gasóleo ultrapassa 2 €
    "2026-03-23": (1.918, 2.037),  # fonte: razaoautomovel semana 13

    # Sem. 14 — continuação da subida
    "2026-03-30": (1.920, 2.075),  # fonte: razaoautomovel semana 14

    # Sem. 15 — PICO HISTÓRICO gasóleo (máximo: 2.145 €/L em 9 Abr)
    "2026-04-06": (1.948, 2.145),  # fonte: razaoautomovel semana 15

    # Sem. 16 — início da descida (-5.4 cênt. diesel, -2.8 gas)
    "2026-04-13": (1.920, 2.090),  # fonte: razaoautomovel semana 16

    # Sem. 17 — descida continua
    "2026-04-20": (1.898, 1.988),  # fonte: razaoautomovel semana 17

    # Sem. 18 — alguma estabilização (gasóleo desce, gas sobe ligeiramente)
    "2026-04-27": (1.927, 1.958),  # fonte: caetano.pt semana 18

    # Sem. 19 — nova subida brusca ("disparam") antes de nova descida
    "2026-05-04": (1.999, 2.058),  # estimado: semana 20 - delta (May 8 article: -9/-2)

    # Sem. 20 — descida confirmada pelo scraper
    "2026-05-11": (1.979, 1.968),  # fonte: scraper caetano.pt (confirmado)
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
# Cobre desde 1 semana antes do conflito Irão (21 Fev 2026) até hoje
print("\nA descarregar historico Brent (90 dias)...")
brent_df = yf.download("BZ=F",    period="95d", auto_adjust=True, progress=False)
fx_df    = yf.download("EURUSD=X", period="95d", auto_adjust=True, progress=False)

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
