import requests
from bs4 import BeautifulSoup
import re
import psycopg2
import os
from datetime import date
import time
import statistics

MAX_VARIACAO = 60          # % de desvio face a mediana recente a partir do qual descarto o valor

produtos = {
    "arroz": "https://www.auchan.pt/pt/alimentacao/mercearia/arroz-e-massa/arroz/arroz-carolino-auchan-extra-longo-1kg/56832.html",
    "massa": "https://www.auchan.pt/pt/alimentacao/mercearia/arroz-e-massa/esparguete-aletria-e-meadas/esparguete-auchan-1kg/3771753.html",
    "leite": "https://www.auchan.pt/pt/alimentacao/produtos-lacteos/leites/leite-uht/leite-auchan-uht-meio-gordo-slim-1l/3010403.html",
    "ovos": "https://www.auchan.pt/pt/alimentacao/produtos-lacteos/ovos/ovos-galinhas-criadas-no-solo/ovos-galinhas-solo-auchan-classe-m-1-duzia/3931445.html",
    "frango": "https://www.auchan.pt/pt/produtos-frescos/talho/frango-e-galinha/frango-partido-auchan-kg/3357441.html",
    "atum": "https://www.auchan.pt/pt/alimentacao/mercearia/conservas/atum/atum-posta-auchan-em-azeite-120-%2878%29g/3877258.html",
    "azeite": "https://www.auchan.pt/pt/alimentacao/mercearia/azeite-oleo-e-vinagre/azeite-virgem-e-extra-virgem/azeite-virgem-extra-auchan-750-ml/3829993.html",
    "batatas": "https://www.auchan.pt/pt/produtos-frescos/legumes/batatas-alho-e-cebola/batata-vermelha-auchan-3-kg/3483188.html",
    "tomate": "https://www.auchan.pt/pt/produtos-frescos/legumes/tomate-pepino-e-pimentos/tomate-chucha-kg/234040.html",
    "pao": "https://www.auchan.pt/pt/produtos-frescos/padaria/pao-fresco-e-broa/pao-de-rio-maior-450g/2120847.html",
    "acucar": "https://www.auchan.pt/pt/alimentacao/mercearia/acucar-e-adocante/acucar/acucar-auchan-branco-granulado-bx-1kg/4002491.html",
    "farinha": "https://www.auchan.pt/pt/alimentacao/mercearia/farinha/farinha-trigo/farinha-de-trigo-auchan-a-mesa-em-portugal-alentejo-1kg/3372552.html",
    "manteiga": "https://www.auchan.pt/pt/alimentacao/produtos-lacteos/manteiga-cremes-e-margarina/cremes-para-barrar/creme-vegetal-becel-para-barrar-original-225g/3513383.html",
    "iogurte": "https://www.auchan.pt/pt/alimentacao/produtos-lacteos/iogurtes/magros-e-naturais/iogurte-auchan-magro-aroma-morango-4x125g/726838.html",
    "queijo": "https://www.auchan.pt/pt/produtos-frescos/queijaria/queijo-fatiado-e-barra/queijo-flamengo-light-auchan-fatias-200g/1061026.html",
    "cafe": "https://www.auchan.pt/pt/alimentacao/mercearia/cafe-cha-e-infusao/cafe-saco-soluvel-e-cevadas/cafe-auchan-liofilizado-gold-intenso-100g/2955724.html",
    "cereais": "https://www.auchan.pt/pt/alimentacao/mercearia/cereais-e-barras/cereais-crianca/cereais-nestle-chocapic-375g/36138.html",
    "banana": "https://www.auchan.pt/pt/produtos-frescos/fruta/banana-e-frutos-tropicais/banana-del-monte-kg/234229.html",
    "laranja": "https://www.auchan.pt/pt/produtos-frescos/fruta/laranjas-clementinas-e-limoes/laranja-2-kg/158914.html",
    "detergente": "https://www.auchan.pt/pt/limpeza-e-cuidados-do-lar/limpeza-e-tratamento-de-roupa/detergente-maquina-roupa/detergente-liquido/detergente-roupa-maquina-liquido-auchan-caraibas-37-doses/3599527.html",
}


def get_price_info(url):
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/123.0.0.0 Safari/537.36"),
        "Accept-Language": "pt-PT,pt;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
    except:
        return None, None, None, None

    preco = pvpr = None

    sales_elem = soup.select_one(".prices .sales .value")
    if sales_elem:
        m = re.search(r"(\d+[,\.]\d+)", sales_elem.get_text(strip=True))
        if m:
            preco = float(m.group(1).replace(",", "."))

    list_elem = soup.select_one(".prices .list .value, .prices .strike-through .value")
    if list_elem:
        m = re.search(r"(\d+[,\.]\d+)", list_elem.get_text(strip=True))
        if m:
            pvpr = float(m.group(1).replace(",", "."))

    desconto_percent = desconto_euros = None
    if preco and pvpr and pvpr > preco:
        desconto_euros = round(pvpr - preco, 2)
        desconto_percent = round((desconto_euros / pvpr) * 100, 2)

    return preco, pvpr, desconto_percent, desconto_euros


def get_fallback(cursor, produto, supermercado):
    cursor.execute("""
        SELECT preco, pvpr, desconto_percent, desconto_euros
        FROM cabaz_supabase
        WHERE produto = %s AND supermercado = %s
        ORDER BY data DESC
        LIMIT 1
    """, (produto, supermercado))
    row = cursor.fetchone()
    if row:
        return row[0], row[1], row[2], row[3]
    return None, None, None, None


def mediana_recente(cursor, produto, supermercado, coluna, n=14):
    cursor.execute(f"""
        SELECT {coluna}
        FROM cabaz_supabase
        WHERE produto = %s AND supermercado = %s AND {coluna} IS NOT NULL
        ORDER BY data DESC
        LIMIT %s
    """, (produto, supermercado, n))
    vals = [float(r[0]) for r in cursor.fetchall()]
    return statistics.median(vals) if vals else None


DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
hoje = date.today()

cursor.execute("ALTER TABLE cabaz_supabase ADD COLUMN IF NOT EXISTS is_fallback boolean DEFAULT false")
conn.commit()

dados = []

for produto, url in produtos.items():
    preco, pvpr, desconto_percent, desconto_euros = get_price_info(url)
    is_fallback = False

    if preco is None:
        preco, pvpr, desconto_percent, desconto_euros = get_fallback(cursor, produto, "auchan")
        if preco is not None:
            is_fallback = True
            print(f"Fallback '{produto}': {preco:.2f} EUR")
        else:
            print(f"Sem dados para '{produto}', ignorado")
            continue
    else:
        ref_preco = mediana_recente(cursor, produto, "auchan", "preco")
        ref_pvpr = mediana_recente(cursor, produto, "auchan", "pvpr")
        fora = None
        if ref_preco and abs(preco - ref_preco) / ref_preco * 100 > MAX_VARIACAO:
            fora = f"preco {preco:.2f} (mediana {ref_preco:.2f})"
        elif pvpr and ref_pvpr and abs(pvpr - ref_pvpr) / ref_pvpr * 100 > MAX_VARIACAO:
            fora = f"pvpr {pvpr:.2f} (mediana {ref_pvpr:.2f})"
        if fora:
            fb = get_fallback(cursor, produto, "auchan")
            if fb[0] is not None:
                is_fallback = True
                print(f"'{produto}' fora do normal ({fora}), uso valor anterior")
                preco, pvpr, desconto_percent, desconto_euros = fb

    dados.append({
        "produto": produto,
        "preco": preco,
        "pvpr": pvpr,
        "desconto_percent": desconto_percent,
        "desconto_euros": desconto_euros,
        "is_fallback": is_fallback,
        "supermercado": "auchan"
    })
    time.sleep(1)


cursor.execute("DELETE FROM cabaz_supabase WHERE data = %s AND supermercado = %s",
               (hoje, "auchan"))

for item in dados:
    cursor.execute("""
        INSERT INTO cabaz_supabase
        (data, supermercado, produto, preco, pvpr, desconto_percent, desconto_euros, is_fallback)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (data, produto, supermercado) DO NOTHING
    """, (hoje, item["supermercado"], item["produto"], item["preco"],
          item["pvpr"], item["desconto_percent"], item["desconto_euros"], item["is_fallback"]))

conn.commit()
conn.close()
print(f"Auchan: {len(dados)}/20 guardados ({hoje})")
