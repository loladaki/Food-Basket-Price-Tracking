# CabazAlimentarPT

Sigo todos os dias o preço de vinte produtos essenciais em três supermercados
portugueses — Continente, Auchan e Pingo Doce — e junto-lhes o preço do petróleo
Brent e dos combustíveis, para perceber o que mexe com o custo de vida.

O site está em `index.html` (uma página, sem dependências a instalar) e mostra:

- o custo do cabaz de hoje em cada supermercado e a variação face à semana anterior;
- comparação de preços produto a produto, num dia à escolha;
- promoções e mínimos históricos;
- evolução e uma previsão simples por regressão linear;
- onde comprar cada produto mais barato;
- combustível vs cabaz — Brent, gasolina e gasóleo lado a lado com o cabaz,
  com correlação e análise por desfasamento (lag).

## Como funciona

Todos os dias, às 6h UTC, o GitHub Actions corre os scrapers e grava os preços
numa base de dados Supabase (Postgres). O site lê diretamente do Supabase e
desenha os gráficos com Chart.js no browser.

| Ficheiro | O que faz |
|----------|-----------|
| `Supabase_script_continente.py` | Continente (catálogo nacional, sem loja fixada) |
| `scraper_auchan.py` | Auchan |
| `scraper_pingodoce_final.py` | Pingo Doce (usa Playwright) |
| `scraper_combustivel.py` | Brent (yfinance) e preços PT de gasolina/gasóleo |
| `backfill_combustivel.py` | preenche o histórico de combustível (correr uma vez) |
| `index.html` | o site |

Cada scraper tem duas salvaguardas: se o site não devolver o preço, usa o do dia
anterior; e se o valor recolhido se afastar demasiado da mediana recente,
descarta-o e mantém o anterior (evita apanhar preços errados quando o produto
está indisponível).

## Configuração

Os scrapers precisam da variável `DATABASE_URL` (ligação ao Supabase), definida
nos *secrets* do repositório. O calendário está em `.github/workflows/main.yml`.

Projeto pessoal, sem fins comerciais. Preços das lojas online (catálogo nacional).
