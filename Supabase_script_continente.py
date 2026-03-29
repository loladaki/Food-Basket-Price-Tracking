import requests
from bs4 import BeautifulSoup
import json
import psycopg2
import os
from datetime import date

# Pega a DATABASE_URL do ambiente
DATABASE_URL = os.getenv("DATABASE_URL")

# ==============================
# CONFIG
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

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ==============================
# FUNÇÃO SCRAPING
# ==============================

def get_product_info(url):
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    scripts = soup.find_all("script", type="application/ld+json")

    for script in scripts:
        try:
            data = json.loads(script.string)

            if "offers" in data:
                nome = data["name"]
                preco = float(data["offers"]["price"])

                pvpr = None
                if "priceSpecification" in data["offers"]:
                    ref = data["offers"]["priceSpecification"].get("referencePrice")
                    if ref:
                        pvpr = float(ref)

                desconto_percent = 0
                desconto_euros = 0

                if pvpr and preco < pvpr:
                    desconto_percent = round((pvpr - preco) / pvpr * 100, 2)
                    desconto_euros = round(pvpr - preco, 2)

                return nome, preco, pvpr, desconto_percent, desconto_euros

        except:
            continue

    return None, None, None, None, None


# ==============================
# EXECUÇÃO
# ==============================

dados = []
total = 0
total_sem_desc = 0

print("\n--- CABAZ ---")

for produto, url in produtos.items():
    nome, preco, pvpr, desconto_percent, desconto_euros = get_product_info(url)

    if preco:
        print(f"{produto} - {preco} €")

        total += preco
        if pvpr:
            total_sem_desc += pvpr
        else:
            total_sem_desc += preco

        dados.append({
            "produto": produto,
            "preco": preco,
            "pvpr": pvpr,
            "desconto_percent": desconto_percent,
            "desconto_euros": desconto_euros,
            "supermercado": "continente"
        })

print("\nTOTAL:", round(total, 2), "€")

# ==============================
# LIGAR AO SUPABASE
# ==============================

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

hoje = date.today()

# apagar dados do dia (continente)
cursor.execute("""
DELETE FROM cabaz_supabase 
WHERE data = %s AND supermercado = %s
""", (hoje, "continente"))

# inserir dados
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

print("Dados guardados com sucesso!")