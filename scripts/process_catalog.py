import pandas as pd
import os
import numpy as np
import json
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

CHROME_BIN = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser")
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")


def normalize_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    if not url.startswith("http"):
        return "https://" + url.lstrip("/")
    return url


def scrape_batch(urls_batch, batch_id):
    """Scrapea un lote de URLs con Selenium"""
    opts = Options()
    opts.binary_location = CHROME_BIN
    # headless
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)
    results = {}

    for url in urls_batch:
        try:
            if not url:
                results[url] = "NO_URL"
                continue
            driver.get(url)
            wait = WebDriverWait(driver, 6)
            el = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h2.product-detail__price"))
            )
            results[url] = el.text.strip()
            print(f"[Batch {batch_id}] ✅ {url} -> {results[url]}")
        except Exception as e:
            results[url] = "NO"
            print(f"[Batch {batch_id}] ⚠️ {url} -> {e}")

    driver.quit()
    return results


def load_stock_from_url(stock_url: str, local_path: str) -> pd.DataFrame:
    """Descarga el CSV/JSON del stock desde Drive y devuelve un DataFrame"""
    try:
        print(f"⬇️ Descargando datos de stock desde {stock_url}")
        resp = requests.get(stock_url)
        resp.raise_for_status()

        content_type = resp.headers.get("content-type", "")
        if "json" in content_type:
            data = resp.json()
            df = pd.DataFrame(data)
        else:
            # Si Power Automate lo sube como CSV
            from io import StringIO
            df = pd.read_csv(StringIO(resp.text))

        # Guardar copia local para depuración
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(resp.text)

        print(f"✅ Stock descargado: {len(df)} filas")
        return df
    except Exception as e:
        print(f"⚠️ Error descargando stock desde Drive: {e}")
        return pd.DataFrame()


# --- paths ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "files", "catalog-products.xlsx")
STOCK_FILE = os.path.join(BASE_DIR, "stock_data.json")

today = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(BASE_DIR, f"productos_filtrados_{today}.xlsx")

# --- leer catálogo ---
df_catalog = pd.read_excel(INPUT_FILE)
df_es = df_catalog[df_catalog["lang"] == "es_ES"].copy()

# --- leer stock JSON ---
STOCK_URL = os.getenv("STOCK_URL", "")

df_stock = load_stock_from_url(STOCK_URL, STOCK_FILE)

# Normalizar URLs
df_es["url"] = df_es["url"].apply(normalize_url)
urls = df_es["url"].tolist()

print(f"🔎 Scrapear {len(urls)} productos en paralelo...")

# --- scraping concurrente ---
num_batches = 4 if len(urls) >= 4 else 1
url_batches = np.array_split(urls, num_batches)
final_results = {}

with ThreadPoolExecutor(max_workers=num_batches) as executor:
    futures = {executor.submit(scrape_batch, list(batch), i): i for i, batch in enumerate(url_batches)}
    for future in as_completed(futures):
        try:
            final_results.update(future.result())
        except Exception as e:
            print("⚠️ Error en un future:", e)

df_es["PVP_WEB"] = df_es["url"].map(final_results).fillna("NO")

# --- procesar stock ---
df_es["STOCK_LA62"] = 0  # valor por defecto = 0

if not df_stock.empty:
    print("🔄 Asignando stock por SKU...")
    # Indexar df_stock por Cod Prod para búsquedas rápidas
    stock_map = df_stock.set_index("Cod Prod")["Stock Contable"].to_dict()

    # Normalizar SKU del catálogo a str sin espacios
    df_es["SKU_norm"] = df_es["SKU"].astype(str).str.strip()

    # Asignar stock comparando como strings
    df_es["STOCK_LA62"] = df_es["SKU_norm"].map(lambda s: stock_map.get(s, 0))

# --- dataframe final ---
df_result = pd.DataFrame({
    "SKU": df_es["SKU"],
    "MODELO": df_es.get("modello", ""),
    "CATEGORIA": df_es.get("categorySingular", ""),
    "STOCK_LA62": df_es["STOCK_LA62"],
    "PVP_WEB": df_es["PVP_WEB"],
    "URL": df_es["url"],
})

# Guardar
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
df_result.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Archivo generado en: {OUTPUT_FILE}")
