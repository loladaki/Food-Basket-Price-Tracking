import requests
from bs4 import BeautifulSoup
import re
import psycopg2
import os
from datetime import date
import time


# PRODUTOS

produtos = {
    "arroz": "https://www.continente.pt/produto/arroz-carolino-continente-continente-4738050.html",
    "massa": "https://www.continente.pt/produto/massa-esparguete-pack-poupanca-continente-continente-5253941.html",
    "leite": "https://www.continente.pt/produto/leite-uht-meio-gordo-continente-continente-6879912.html",
    "ovos": "https://www.continente.pt/produto/ovos-de-solo-classe-m-continente-continente-7284496.html",
    "frango": "https://www.continente.pt/produto/frango-completo-aos-pedacos-continente-continente-7069752.html",
    "atum": "https://www.continente.pt/produto/atum-em-azeite-continente-continente-3697794.html",
    "azeite": "https://www.continente.pt/produto/azeite-poupanca-continente-7748019.html",
    "batatas": "https://www.continente.pt/produto/batata-branca-continente-continente-5454781.html",
    "tomate": "https://www.continente.pt/produto/tomate-chucha-continente-continente-2076838.html",
    "pao": "https://www.continente.pt/produto/pao-de-rio-maior-6913160.html",
    "acucar": "https://www.continente.pt/produto/acucar-branco-continente-continente-5038799.html",
    "farinha": "https://www.continente.pt/produto/farinha-de-trigo-t65-continente-continente-7579107.html",
    "manteiga": "https://www.continente.pt/produto/creme-vegetal-para-barrar-sabor-a-manteiga-becel-becel-7621869.html",
    "iogurte": "https://www.continente.pt/produto/iogurte-aroma-morango-continente-continente-5788569.html",
    "queijo": "https://www.continente.pt/produto/queijo-flamengo-fatiado-continente-continente-6184775.html",
    "cafe": "https://www.continente.pt/produto/cafe-soluvel-classico-continente-continente-4871954.html",
    "cereais": "https://www.continente.pt/produto/cereais-chocapic-chocapic-2004742.html",
    "banana": "https://www.continente.pt/produto/banana-continente-continente-2597619.html",
    "laranja": "https://www.continente.pt/produto/laranja-zero-desperdicio-continente-continente-7998103.html",
    "detergente": "https://www.continente.pt/produto/detergente-maquina-roupa-liquido-sabao-natural-continente-continente-7718451.html"
}


# EXTRAÇÃO DE PREÇOS

def get_price_info(url):
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
    except:
        return None, None, None, None

    preco = None
    pvpr = None

    # PREÇO ATUAL
    price_elem = soup.select_one(".pwc-tile--price-primary")
    if price_elem:
        texto = price_elem.get_text(strip=True)
        match = re.search(r"(\d+,\d+)", texto)
        if match:
            preco = float(match.group(1).replace(",", "."))

    # MÉTODO 1: PREÇO RISCADO (PROMOÇÃO)
    old_elem = soup.select_one(".strike-through .pwc-tile--price-value")
    if old_elem:
        texto_old = old_elem.get_text(strip=True)
        match = re.search(r"(\d+,\d+)", texto_old)
        if match:
            pvpr = float(match.group(1).replace(",", "."))

    # MÉTODO 2: PVPR (PROMOÇÃO)
    if pvpr is None:
        texto_total = soup.get_text()
        match = re.search(r"PVPR\s*(\d+,\d+)", texto_total)
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
        preco, pvpr, desconto_percent, desconto_euros = get_fallback(cursor, produto, "continente")
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
        "supermercado":     "continente"
    })

    time.sleep(1)


# GUARDAR NA BASE DE DADOS

cursor.execute("""
    DELETE FROM cabaz_supabase
    WHERE data = %s AND supermercado = %s
""", (hoje, "continente"))

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

print(f"Continente: {len(dados)}/20 produtos guardados ({hoje})")