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

# ==============================
# 5️⃣ GUARDAR NA SUPABASE
# ==============================
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

hoje = date.today()

# apagar dados do mesmo dia para o mesmo supermercado
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

print("\nDados guardados com sucesso!")