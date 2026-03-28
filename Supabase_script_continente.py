import requests
from bs4 import BeautifulSoup
import re
import sqlite3
from datetime import datetime
import psycopg2
import os

# ==============================
# 1️⃣ PRODUTOS (mete aqui os teus links)
# ==============================

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

# ==============================
# 2️⃣ FUNÇÃO PARA OBTER PREÇOS
# ==============================

def get_price_info(url):

    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, "html.parser")
    texto = soup.get_text()

    preco = None
    pvpr = None

    # preço atual
    match = re.search(r"\n\s*(\d+,\d+)\s*€", texto)
    if match:
        preco = float(match.group(1).replace(",", "."))

    # PVPR
    match = re.search(r"PVPR\s*(\d+,\d+)", texto)
    if match:
        pvpr = float(match.group(1).replace(",", "."))

    return preco, pvpr

# ==============================
# 3️⃣ IR BUSCAR DADOS
# ==============================

dados = []
total_cabaz = 0
total_sem_promo = 0

for produto, link in produtos.items():

    preco, pvpr = get_price_info(link)

    if preco is None:
        print(f"Erro em {produto}")
        continue

    desconto_percent = None
    desconto_euros = None

    if pvpr:
        desconto_percent = round((1 - preco / pvpr) * 100, 2)
        desconto_euros = round(pvpr - preco, 2)

    dados.append({
        "produto": produto,
        "preco": preco,
        "pvpr": pvpr,
        "desconto_percent": desconto_percent,
        "desconto_euros": desconto_euros
    })

    total_cabaz += preco
    total_sem_promo += pvpr if pvpr else preco
# ==============================
# 4️⃣ MOSTRAR RESULTADOS
# ==============================

print("\n--- CABAZ ---")

for item in dados:
    print(item["produto"], "-", item["preco"], "€")

    if item["pvpr"]:
        print("PVPR:", item["pvpr"], "€")
        print("Desconto:", item["desconto_percent"], "%")

    print("------")

print("TOTAL:", round(total_cabaz, 2), "€")
print("SEM PROMO:", round(total_sem_promo, 2), "€")
print("POUPANÇA:", round(total_sem_promo - total_cabaz, 2), "€")

# ==============================
# 5️⃣ GUARDAR NA BASE DE DADOS
# ==============================

conn = psycopg2.connect(
    host="db.lfqhvioqgvksqrdxfgyy.supabase.co",
    database="postgres",
    user="postgres",
    password="XkCTO344f569",
    port=5432
)

cursor = conn.cursor()

hoje = datetime.now().strftime("%Y-%m-%d")

# evitar duplicados do mesmo dia
cursor.execute("DELETE FROM cabaz WHERE data = %s", (hoje,))

for item in dados:
    cursor.execute("""
    INSERT INTO cabaz (data, produto, preco, pvpr, desconto_percent, desconto_euros)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        hoje,
        item["produto"],
        item["preco"],
        item["pvpr"],
        item["desconto_percent"],
        item["desconto_euros"]
    ))

conn.commit()
conn.close()

print("\nDados guardados com sucesso!")