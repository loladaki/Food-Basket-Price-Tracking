import requests
from bs4 import BeautifulSoup
import re
import psycopg2
import os
from datetime import date
import time
import statistics


# PRODUTOS

produtos = {
    "arroz": "https://www.continente.pt/produto/arroz-carolino-continente-continente-4738050.html",
    "massa": "https://www.continente.pt/produto/massa-esparguete-pack-poupanca-continente-continente-5253941.html",
    "leite": "https://www.continente.pt/produto/leite-uht-meio-gordo-gresso-gresso-7848880.html",
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
    "iogurte": "https://www.continente.pt/produto/iogurte-aroma-coco-continente-continente-5788581.html",
    "queijo": "https://www.continente.pt/produto/queijo-flamengo-fatiado-continente-continente-6184775.html",
    "cafe": "https://www.continente.pt/produto/cafe-soluvel-classico-continente-continente-4871954.html",
    "cereais": "https://www.continente.pt/produto/cereais-chocapic-chocapic-2004742.html",
    "banana": "https://www.continente.pt/produto/banana-continente-continente-2597619.html",
    "laranja": "https://www.continente.pt/produto/laranja-zero-desperdicio-continente-continente-7998103.html",
    "detergente": "https://www.continente.pt/produto/detergente-maquina-roupa-liquido-sabao-natural-continente-continente-7718451.html"
}


# CONFIGURACAO / LOCALIZACAO DE ENTREGA
# O Continente so mostra a disponibilidade/preco de muitos produtos depois de
# definir uma zona de entrega. Sem loja definida, as paginas de produto
# redirecionam para a homepage e o scraper apanhava precos errados (produtos
# em destaque na homepage). Forcamos o codigo postal do Porto.
MAX_VARIACAO_PCT = 60   # variacao maxima (%) face a mediana historica antes de rejeitar o valor
CODIGO_POSTAL    = "4050-586"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
BASE_DW = "https://www.continente.pt/on/demandware.store/Sites-continente-Site/default"


def criar_sessao_continente():
    """
    Cria uma sessao HTTP com a zona de entrega do Porto ja definida.
    Fluxo (plataforma Salesforce Commerce Cloud):
      1. GET homepage                              -> cookies de sessao
      2. GET Stores-GetCoverageArea?postalCode=... -> storeId / areaID da zona
      3. POST Stores-SetStoreContext               -> aplica a loja a sessao
    """
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "pt-PT,pt;q=0.9"})
    try:
        s.get("https://www.continente.pt/", timeout=15)
        r = s.get(f"{BASE_DW}/Stores-GetCoverageArea",
                  params={"postalCode": CODIGO_POSTAL},
                  headers={"X-Requested-With": "XMLHttpRequest"}, timeout=15)
        data = r.json().get("data") or {}
        store_id = data.get("storeId")
        if not store_id:
            print(f"Aviso: nao obtive loja para o CP {CODIGO_POSTAL} -- a continuar sem localizacao.")
            return s
        s.post(f"{BASE_DW}/Stores-SetStoreContext",
               headers={"X-Requested-With": "XMLHttpRequest"},
               data={
                   "storeId":         store_id,
                   "physicalStoreId": data.get("physicalStoreId", store_id),
                   "storeID":         data.get("storeID", store_id),
                   "areaID":          data.get("areaID", ""),
                   "storePostalCode": CODIGO_POSTAL,
                   "storeInfo":       CODIGO_POSTAL,
               }, timeout=15)
        print(f"Zona de entrega definida: {data.get('name','?')} (loja {store_id}, CP {CODIGO_POSTAL})")
    except Exception as e:
        print(f"Aviso: falha ao definir zona de entrega ({e}) -- a continuar.")
    return s


# VALOR DE REFERENCIA: mediana dos ultimos N valores (robusta a outliers)
# 'coluna' e' um nome interno controlado ('preco' ou 'pvpr'), nunca input externo.

def get_referencia(cursor, produto, supermercado, coluna, n=14):
    cursor.execute(f"""
        SELECT {coluna}
        FROM cabaz_supabase
        WHERE produto = %s AND supermercado = %s AND {coluna} IS NOT NULL
        ORDER BY data DESC
        LIMIT %s
    """, (produto, supermercado, n))
    vals = [float(r[0]) for r in cursor.fetchall()]
    return statistics.median(vals) if vals else None


# EXTRAÇÃO DE PREÇOS

def get_price_info(sessao, url):
    try:
        response = sessao.get(url, timeout=15)
    except:
        return None, None, None, None

    # PROTECAO: se o produto estiver indisponivel na zona, o Continente
    # redireciona para a homepage. Nesse caso NAO extraimos preco -- devolvemos
    # None para acionar o fallback, evitando apanhar o preco de um produto em
    # destaque na homepage.
    if "/produto/" not in response.url:
        return None, None, None, None

    soup = BeautifulSoup(response.text, "html.parser")

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

# sessao com a zona de entrega do Porto ja definida (reutilizada em todos os produtos)
sessao = criar_sessao_continente()


# RECOLHER OS DADOS

dados = []

for produto, url in produtos.items():

    preco, pvpr, desconto_percent, desconto_euros = get_price_info(sessao, url)

    # FALLBACK: se o scraping falhou usar o preco do dia anterior
    if preco is None:
        preco, pvpr, desconto_percent, desconto_euros = get_fallback(cursor, produto, "continente")
        if preco is not None:
            print(f"Fallback usado para '{produto}' -- preco anterior: {preco:.2f} EUR")
        else:
            print(f"Atencao: sem preco e sem fallback para '{produto}' -- ignorado")
            continue
    else:
        # FAILSAFE: validar preco e pvpr contra a mediana historica de cada um.
        # Se algum se desviar demasiado, usar os valores do dia anterior.
        ref_preco = get_referencia(cursor, produto, "continente", "preco")
        ref_pvpr  = get_referencia(cursor, produto, "continente", "pvpr")
        suspeito = None
        if ref_preco:
            var = abs(preco - ref_preco) / ref_preco * 100
            if var > MAX_VARIACAO_PCT:
                suspeito = f"preco {preco:.2f} vs ref {ref_preco:.2f} ({var:.0f}%)"
        if suspeito is None and pvpr is not None and ref_pvpr:
            var = abs(pvpr - ref_pvpr) / ref_pvpr * 100
            if var > MAX_VARIACAO_PCT:
                suspeito = f"pvpr {pvpr:.2f} vs ref {ref_pvpr:.2f} ({var:.0f}%)"
        if suspeito is not None:
            fb = get_fallback(cursor, produto, "continente")
            if fb[0] is not None:
                print(f"Valor SUSPEITO '{produto}' -- {suspeito}. A usar valores anteriores.")
                preco, pvpr, desconto_percent, desconto_euros = fb

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
