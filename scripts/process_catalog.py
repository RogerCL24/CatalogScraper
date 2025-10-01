import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- helpers ---
def normalize_url(url: str) -> str:
    """Asegura que la URL tenga esquema https://"""
    if not url.startswith("http"):
        return "https://" + url.lstrip("/")
    return url

def get_pvp(url: str) -> str:
    """Scrapea el PVP desde la página del producto"""
    url = normalize_url(url)
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=15)

        # si la página no existe
        if resp.status_code == 404:
            print(f"❌ 404 Not Found: {url}")
            return "NO"

        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        price_tag = soup.select_one("h2.product-detail__price")

        if price_tag:
            price = price_tag.get_text(strip=True)
            print(f"✅ Precio encontrado en {url}: {price}")
            return price
        else:
            print(f"⚠️ No se encontró precio en {url}")
            print("HTML preview:", resp.text[:200], "...")
            return "NO"

    except Exception as e:
        print(f"⚠️ Error al scrapear {url}: {e}")
        return "NO"

# --- paths ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "files", "catalog-products.xlsx")

today = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(BASE_DIR, f"productos_filtrados_{today}.xlsx")

# --- read input ---
df = pd.read_excel(INPUT_FILE)

# filtrar idioma español
df_es = df[df["lang"] == "es_ES"].copy()

# --- paralelizar scraping ---
urls = df_es["url"].tolist()
pvp_results = {}

print(f"🔎 Scrapear {len(urls)} productos en paralelo...")

with ThreadPoolExecutor(max_workers=20) as executor:  # 20 workers en paralelo
    future_to_url = {executor.submit(get_pvp, url): url for url in urls}
    for future in as_completed(future_to_url):
        url = future_to_url[future]
        try:
            price = future.result()
            pvp_results[url] = price
        except Exception as exc:
            pvp_results[url] = "NO"
            print(f"⚠️ Exception inesperada con {url}: {exc}")

# asignar PVP_WEB al dataframe
df_es["PVP_WEB"] = df_es["url"].map(pvp_results)

# --- crear dataframe final ---
df_result = pd.DataFrame({
    "SKU": df_es["SKU"],
    "MODELO": df_es["modello"],
    "CATEGORIA": df_es["categorySingular"],
    "STOCK_LA62": "N/A",  # aún no disponible
    "PVP_WEB": df_es["PVP_WEB"],
    "URL": df_es["url"].apply(normalize_url)
})

# --- save ---
df_result.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Archivo generado en: {OUTPUT_FILE}")
