import re
import time
import psycopg2
import os
from datetime import date

from playwright.sync_api import sync_playwright


# PRODUTOS

produtos = {
    "arroz": "https://www.pingodoce.pt/home/produtos/mercearia/arroz-massa-e-leguminosas/arroz/arroz-carolino-pingo-doce-918813.html?",
    "massa": "https://www.pingodoce.pt/home/produtos/as-nossas-marcas/pingo-doce/massa-esparguete-pack-poupanca-pingo-doce-895467.html?",
    "leite": "https://www.pingodoce.pt/home/produtos/leite-e-bebidas-vegetais%E2%80%8B/leite/leite-meio-gordo-e-gordo%E2%80%8B/leite-uht-meio-gordo-pingo-doce-48150.html?",
    "ovos": "https://www.pingodoce.pt/home/produtos/as-nossas-marcas/pingo-doce/ovos-de-solo-classe-m-pingo-doce-889028.html?",
    "frango": "https://www.pingodoce.pt/home/produtos/talho/aves/frango/frango-para-churrasco-embalado-pingo-doce-476488.html?",
    "atum": "https://www.pingodoce.pt/home/produtos/mercearia/conservas/atum/atum-posta-em-azeite-pingo-doce-559259.html?",
    "azeite": "https://www.pingodoce.pt/home/produtos/mercearia/azeite-oleo-e-vinagre/azeite/azeite-virgem-extra-as-nossas-planicies-pingo-doce-654603.html?",
    "batatas": "https://www.pingodoce.pt/home/produtos/frutas-e-vegetais/vegetais/batatas-cebolas-e-alhos/batata-para-cozer-e-assar-embalada-pingo-doce-454634.html?",
    "tomate": "https://www.pingodoce.pt/home/produtos/frutas-e-vegetais/vegetais/tomates-pepinos-e-pimentos/tomate-alongado--57%2F67-nossa-fruta-e-legumes-46397.html?",
    "pao": "https://www.pingodoce.pt/home/produtos/padaria-e-pastelaria/pao-da-nossa-padaria/pao-familiar/pao-de-rio-maior-nossa-padaria-254253.html?",
    "acucar": "https://www.pingodoce.pt/home/produtos/mercearia/farinha-fermento-e-acucar/acucar-e-adocante/acucar-branco-granulado-pingo-doce-643460.html?",
    "farinha": "https://www.pingodoce.pt/home/produtos/mercearia/farinha-fermento-e-acucar/farinha-fermento-e-pao-ralado/farinha-de-trigo-super-fina-com-fermento-pingo-doce-650224.html?",
    "manteiga": "https://www.pingodoce.pt/home/produtos/manteiga-margarina-e-natas-/manteiga-e-margarina/creme-vegetal-para-barrar-sabor-a-manteiga-becel-947029.html?",
    "iogurte": "https://www.pingodoce.pt/home/produtos/iogurtes-e-sobremesas/iogurtes/aromas%E2%80%8B/iogurte-aroma-tutti-frutti-pack-4-longa-vida-769091003.html?",
    "queijo": "https://www.pingodoce.pt/home/produtos/charcutaria-e-queijos/queijos/fatiado-e-bola%E2%80%8B/queijo-flamengo-fatiado-pingo-doce-900685.html?",
    "cafe": "https://www.pingodoce.pt/home/produtos/cafe-cha-e-achocolatados/cafe-soluvel-e-descafeinado/cafe-soluvel-pingo-doce-1999.html?",
    "cereais": "https://www.pingodoce.pt/home/produtos/bolachas-cereais-e-guloseimas/cereais-barras-e-bolsas-de-fruta/infantis-e-juvenis/cereais-de-chocolate-chocapic-760687.html?",
    "banana": "https://www.pingodoce.pt/home/produtos/frutas-e-vegetais/frutas/fruta-da-epoca/banana-importada-nossa-fruta-e-legumes-43218.html?",
    "laranja": "https://www.pingodoce.pt/home/produtos/frutas-e-vegetais/frutas/fruta-da-epoca/laranja-igp-algarve-embalada-nossa-fruta-e-legumes-764041.html?",
    "detergente": "https://www.pingodoce.pt/home/produtos/limpeza/roupa/detergentes/detergente-maquina-roupa-liquido-roupa-delicada-ultra-881475.html?"
}


# EXTRAÇÃO DE PREÇOS

def parse_price(text):
    if not text:
        return None
    match = re.search(r"(\d+[,\.]\d+)", text.strip())
    if match:
        return float(match.group(1).replace(",", "."))
    return None


def get_price_info(page, url):
    preco = None
    pvpr = None

    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    try:
        page.wait_for_selector(".prices", timeout=8000)
    except:
        pass

    scope = page.query_selector(".product-detail") or page

    for selector in [
        ".prices .sales .value",
        ".prices .price-container",
        ".prices .price",
        ".prices",
    ]:
        el = scope.query_selector(selector)
        if el:
            preco = parse_price(el.inner_text())
            if preco:
                break

    if not preco:
        txt = scope.inner_text() if scope else page.inner_text()
        match = re.search(r"(\d+[,\.]\d+)\s*[€E]\s*/\s*(Kg|kg|KG|L|l|Un|UN|un)", txt)
        if match:
            preco = float(match.group(1).replace(",", "."))

    for selector in [
        ".prices .list .value",
        ".prices .strike-through .value",
        ".prices del",
        ".prices s",
    ]:
        el = scope.query_selector(selector)
        if el:
            pvpr = parse_price(el.inner_text())
            if pvpr and pvpr != preco:
                break

    if pvpr and preco and pvpr <= preco:
        pvpr = None

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

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        locale="pt-PT",
    )
    page = context.new_page()

    for produto, url in produtos.items():

        preco, pvpr, desconto_percent, desconto_euros = None, None, None, None

        try:
            preco, pvpr, desconto_percent, desconto_euros = get_price_info(page, url)
        except Exception as e:
            print(f"Erro em '{produto}': {e}")

        # FALLBACK: se o scraping falhou usar o preco do dia anterior
        if not preco:
            preco, pvpr, desconto_percent, desconto_euros = get_fallback(cursor, produto, "pingodoce")
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
            "supermercado":     "pingodoce"
        })

        time.sleep(1)

    browser.close()


# GUARDAR NA BASE DE DADOS

cursor.execute("""
    DELETE FROM cabaz_supabase
    WHERE data = %s AND supermercado = %s
""", (hoje, "pingodoce"))

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

print(f"Pingo Doce: {len(dados)}/20 produtos guardados ({hoje})")
