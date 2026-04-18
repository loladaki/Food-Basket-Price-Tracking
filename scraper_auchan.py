import requests
from bs4 import BeautifulSoup
import re
import psycopg2
import os
from datetime import date
import time


# PRODUTOS

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
    "laranja": "https://www.auchan.pt/pt/produtos-frescos/fruta/fruta-da-epoca/laranja-algarve-igp-auchan-3kg/3467264.html",
    "detergente": "https://www.auchan.pt/pt/limpeza-e-cuidados-do-lar/limpeza-e-tratamento-de-roupa/detergente-maquina-roupa/detergente-liquido/detergente-roupa-maquina-liquido-auchan-caraibas-37-doses/3599527.html",
}

# EXTRAÇÃO DE PREÇOS

def get_price_info(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "pt-PT,pt;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
    except:
        return None, None, None, None

    preco = None
    pvpr = None

    # PREÇO ATUAL
    sales_elem = soup.select_one(".prices .sales .value")
    if sales_elem:
        match = re.search(r"(\d+[,\.]\d+)", sales_elem.get_text(strip=True))
        if match:
            preco = float(match.group(1).replace(",", "."))

    # PREÇO ANTES DA PROMOÇÃO (preço riscado)
    list_elem = soup.select_one(".prices .list .value, .prices .strike-through .value")
    if list_elem:
        match = re.search(r"(\d+[,\.]\d+)", list_elem.get_text(strip=True))
        if match:
            pvpr = float(match.group(1).replace(",", "."))

    # DESCONTOS
    desconto_percent = None
    desconto_euros = None

    if preco and pvpr and pvpr > preco:
        desconto_euros = round(pvpr - preco, 2)
        desconto_percent = round((desconto_euros / pvpr) * 100, 2)

    return preco, pvpr, desconto_percent, desconto_euros


# FALLBACK: buscar o ultimo preco conhecido no Supabase

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


# LIGAR AO SUPABASE antes do scraping para ter fallback disponivel

DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
hoje = date.today()


# RECOLHER OS DADOS

dados = []

for produto, url in produtos.items():

    preco, pvpr, desconto_percent, desconto_euros = get_price_info(url)

    # FALLBACK: se o scraping falhou usar o preco do dia anterior
    if preco is None:
        preco, pvpr, desconto_percent, desconto_euros = get_fallback(cursor, produto, "auchan")
        if preco is not None:
            print(f"Fallback usado para '{produto}' -- preco anterior: {preco:.2f} EUR")
        else:
            print(f"Atencao: sem preco e sem fallback para '{produto}' -- ignorado")
            continue

    dados.append({
        "produto":          produto,
        "preco":            preco,
        "pvpr":             pvpr,
        "desconto_percent": desconto_percent,
        "desconto_euros":   desconto_euros,
        "supermercado":     "auchan"
    })

    time.sleep(1)


# GUARDAR NA BASE DE DADOS

cursor.execute("""
    DELETE FROM cabaz_supabase
    WHERE data = %s AND supermercado = %s
""", (hoje, "auchan"))

for item in dados:
    cursor.execute("""
        INSERT INTO cabaz_supabase
        (data, supermercado, produto, preco, pvpr, desconto_percent, desconto_euros)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (data, produto, supermercado) DO NOTHING
    """, (
        hoje,
        item["supermercado"],
        item["produto"],
        item["preco"],
        item["pvpr"],
        item["desconto_percent"],
        item["desconto_euros"]
    ))

conn.commit()
conn.close()

print(f"Auchan: {len(dados)}/20 produtos guardados ({hoje})")
