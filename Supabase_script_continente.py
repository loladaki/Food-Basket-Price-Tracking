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
    "leite": "https://www.continente.pt/produto/leite-uht-meio-gordo-continente-7062996.html",
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
    "detergente": "https://www.continente.pt/produto/detergente-liquido-maquina-roupa-brisa-azul-continente-8916150.html"
}

MAX_VARIACAO = 60          # % de desvio face a mediana recente a partir do qual descarto o valor

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def criar_sessao():
    # nao fixamos zona de entrega: o catalogo nacional (sem loja escolhida)
    # devolve mais produtos com preco do que uma loja concreta, que atira para a
    # homepage tudo o que nao tem no sortido - e ai o produto ficava sem preco
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "pt-PT,pt;q=0.9"})
    try:
        s.get("https://www.continente.pt/", timeout=15)   # aquece os cookies
    except Exception as e:
        print(f"Aviso: warm-up falhou: {e}")
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

cursor.execute("ALTER TABLE cabaz_supabase ADD COLUMN IF NOT EXISTS is_fallback boolean DEFAULT false")
conn.commit()

sessao = criar_sessao()
dados = []

for produto, url in produtos.items():
    preco, pvpr, desconto_percent, desconto_euros = get_price_info(sessao, url)
    is_fallback = False

    if preco is None:
        preco, pvpr, desconto_percent, desconto_euros = get_fallback(cursor, produto, "continente")
        if preco is not None:
            is_fallback = True
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
        "supermercado": "continente"
    })
    time.sleep(1)


cursor.execute("DELETE FROM cabaz_supabase WHERE data = %s AND supermercado = %s",
               (hoje, "continente"))

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
print(f"Continente: {len(dados)}/20 guardados ({hoje})")
