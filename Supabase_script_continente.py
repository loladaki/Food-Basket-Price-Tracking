import requests
from bs4 import BeautifulSoup
import re
import psycopg2
import os
from datetime import date
import time
import statistics


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
    "iogurte": "https://www.continente.pt/produto/iogurte-aroma-coco-continente-continente-5788581.html",
    "queijo": "https://www.continente.pt/produto/queijo-flamengo-fatiado-continente-continente-6184775.html",
    "cafe": "https://www.continente.pt/produto/cafe-soluvel-classico-continente-continente-4871954.html",
    "cereais": "https://www.continente.pt/produto/cereais-chocapic-chocapic-2004742.html",
    "banana": "https://www.continente.pt/produto/banana-continente-continente-2597619.html",
    "laranja": "https://www.continente.pt/produto/laranja-zero-desperdicio-continente-continente-7998103.html",
    "detergente": "https://www.continente.pt/produto/detergente-maquina-roupa-liquido-sabao-natural-continente-continente-7718451.html"
}

MAX_VARIACAO = 60          # % de desvio face a mediana recente a partir do qual descarto o valor
CODIGO_POSTAL = "4050-586"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
BASE_DW = "https://www.continente.pt/on/demandware.store/Sites-continente-Site/default"


def criar_sessao():
    # sem uma zona de entrega escolhida o Continente atira as paginas de produto
    # para a homepage, por isso fixo o codigo postal antes de comecar
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "pt-PT,pt;q=0.9"})
    try:
        s.get("https://www.continente.pt/", timeout=15)
        r = s.get(f"{BASE_DW}/Stores-GetCoverageArea",
                  params={"postalCode": CODIGO_POSTAL},
                  headers={"X-Requested-With": "XMLHttpRequest"}, timeout=15)
        loja = r.json().get("data") or {}
        store_id = loja.get("storeId")
        if not store_id:
            print(f"Nao consegui zona para {CODIGO_POSTAL}")
            return s
        s.post(f"{BASE_DW}/Stores-SetStoreContext",
               headers={"X-Requested-With": "XMLHttpRequest"},
               data={
                   "storeId": store_id,
                   "physicalStoreId": loja.get("physicalStoreId", store_id),
                   "storeID": loja.get("storeID", store_id),
                   "areaID": loja.get("areaID", ""),
                   "storePostalCode": CODIGO_POSTAL,
                   "storeInfo": CODIGO_POSTAL,
               }, timeout=15)
        print(f"Zona: {loja.get('name','?')} (loja {store_id}, {CODIGO_POSTAL})")
    except Exception as e:
        print(f"Falha ao fixar zona: {e}")
    return s


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


def get_price_info(sessao, url):
    try:
        response = sessao.get(url, timeout=15)
    except:
        return None, None, None, None

    if "/produto/" not in response.url:   # produto fora de stock na zona
        return None, None, None, None

    soup = BeautifulSoup(response.text, "html.parser")
    preco = pvpr = None

    price_elem = soup.select_one(".pwc-tile--price-primary")
    if price_elem:
        m = re.search(r"(\d+,\d+)", price_elem.get_text(strip=True))
        if m:
            preco = float(m.group(1).replace(",", "."))

    old_elem = soup.select_one(".strike-through .pwc-tile--price-value")
    if old_elem:
        m = re.search(r"(\d+,\d+)", old_elem.get_text(strip=True))
        if m:
            pvpr = float(m.group(1).replace(",", "."))
    if pvpr is None:
        m = re.search(r"PVPR\s*(\d+,\d+)", soup.get_text())
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


DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
hoje = date.today()

sessao = criar_sessao()
dados = []

for produto, url in produtos.items():
    preco, pvpr, desconto_percent, desconto_euros = get_price_info(sessao, url)

    if preco is None:
        preco, pvpr, desconto_percent, desconto_euros = get_fallback(cursor, produto, "continente")
        if preco is not None:
            print(f"Fallback '{produto}': {preco:.2f} EUR")
        else:
            print(f"Sem dados para '{produto}', ignorado")
            continue
    else:
        ref_preco = mediana_recente(cursor, produto, "continente", "preco")
        ref_pvpr = mediana_recente(cursor, produto, "continente", "pvpr")
        fora = None
        if ref_preco and abs(preco - ref_preco) / ref_preco * 100 > MAX_VARIACAO:
            fora = f"preco {preco:.2f} (mediana {ref_preco:.2f})"
        elif pvpr and ref_pvpr and abs(pvpr - ref_pvpr) / ref_pvpr * 100 > MAX_VARIACAO:
            fora = f"pvpr {pvpr:.2f} (mediana {ref_pvpr:.2f})"
        if fora:
            fb = get_fallback(cursor, produto, "continente")
            if fb[0] is not None:
                print(f"'{produto}' fora do normal ({fora}), uso valor anterior")
                preco, pvpr, desconto_percent, desconto_euros = fb

    dados.append({
        "produto": produto,
        "preco": preco,
        "pvpr": pvpr,
        "desconto_percent": desconto_percent,
        "desconto_euros": desconto_euros,
        "supermercado": "continente"
    })
    time.sleep(1)


cursor.execute("DELETE FROM cabaz_supabase WHERE data = %s AND supermercado = %s",
               (hoje, "continente"))

for item in dados:
    cursor.execute("""
        INSERT INTO cabaz_supabase
        (data, supermercado, produto, preco, pvpr, desconto_percent, desconto_euros)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (data, produto, supermercado) DO NOTHING
    """, (hoje, item["supermercado"], item["produto"], item["preco"],
          item["pvpr"], item["desconto_percent"], item["desconto_euros"]))

conn.commit()
conn.close()
print(f"Continente: {len(dados)}/20 guardados ({hoje})")
