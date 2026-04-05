import requests
from bs4 import BeautifulSoup
import re
import psycopg2
import os
from datetime import date

# ==============================
# 1️⃣ PRODUTOS
# ==============================
produtos = {
    "arroz": "https://www.auchan.pt/pt/alimentacao/mercearia/arroz-e-massa/arroz/arroz-carolino-auchan-extra-longo-1kg/56832.html",
}

# ==============================
# 2️⃣ FUNÇÃO DE EXTRAÇÃO
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

    # calcular descontos
    desconto_percent = None
    desconto_euros = None
    if pvpr is not None and preco is not None and pvpr > preco:
        desconto_euros = round(pvpr - preco, 2)
        desconto_percent = round((desconto_euros / pvpr) * 100, 2)

    return preco, pvpr, desconto_percent, desconto_euros

# ==============================
# 3️⃣ RECOLHER DADOS
# ==============================
dados = []
total_cabaz = 0
total_sem_promo = 0

for produto, url in produtos.items():
    preco, pvpr, desconto_percent, desconto_euros = get_price_info(url)

    if preco is None:
        print(f"Atenção: preço não encontrado para {produto}")
        continue

    dados.append({
        "produto": produto,
        "preco": preco,
        "pvpr": pvpr,
        "desconto_percent": desconto_percent,
        "desconto_euros": desconto_euros,
        "supermercado": "continente"
    })

    total_cabaz += preco
    total_sem_promo += pvpr if pvpr is not None else preco

# ==============================
# 4️⃣ MOSTRAR RESULTADOS
# ==============================
print("\n--- CABAZ ---")
for item in dados:
    print(f"{item['produto']} - {item['preco']} €")
    if item["pvpr"] is not None:
        print(f"PVPR: {item['pvpr']} €")
        print(f"Desconto: {item['desconto_percent']} %")
    print("------")

print(f"TOTAL: {round(total_cabaz,2)} €")
print(f"SEM PROMO: {round(total_sem_promo,2)} €")
print(f"POUPANÇA: {round(total_sem_promo - total_cabaz,2)} €")