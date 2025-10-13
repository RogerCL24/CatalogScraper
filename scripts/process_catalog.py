import pandas as pd
import os
import numpy as np
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


CHROME_BIN = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser")


def normalize_url(url: str) -> str:
    if not url.startswith("http"):
        return "https://" + url.lstrip("/")
    return url


def scrape_batch(urls_batch, batch_id):
    """Scrapea un lote de URLs con Selenium"""
    opts = Options()
    opts.binary_location = CHROME_BIN
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=opts)
    results = {}

    for url in urls_batch:
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 6)
            el = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h2.product-detail__price"))
            )
            results[url] = el.text.strip()
            print(f"[Batch {batch_id}] ✅ {url}")
        except Exception:
            results[url] = "NO"
            print(f"[Batch {batch_id}] ⚠️ {url}")

    driver.quit()
    return results


def load_stock_json(json_path: str) -> pd.DataFrame:
    """Carga el JSON recibido desde Power Automate"""
    if not os.path.exists(json_path):
        print("⚠️ No se encontró el archivo de stock (stock_data.json)")
        return pd.DataFrame()

    with open(json_path, "r") as f:
        data = json.load(f)

    # Data puede venir anidada en "value"
    if isinstance(data, dict) and "value" in data:
        data = data["value"]

    df_stock = pd.DataFrame(data)
    print(f"📦 Stock importado: {len(df_stock)} filas")
    return df_stock


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
df_stock = load_stock_json(STOCK_FILE)

# Normalizar URLs
df_es["url"] = df_es["url"].apply(normalize_url)
urls = df_es["url"].tolist()

print(f"🔎 Scrapear {len(urls)} productos en paralelo...")

# --- scraping concurrente ---
num_batches = 4
url_batches = np.array_split(urls, num_batches)
final_results = {}

with ThreadPoolExecutor(max_workers=num_batches) as executor:
    futures = {executor.submit(scrape_batch, batch, i): i for i, batch in enumerate(url_batches)}
    for future in as_completed(futures):
        final_results.update(future.result())

df_es["PVP_WEB"] = df_es["url"].map(final_results)

# --- procesar stock ---
df_es["STOCK_LA62"] = "N/A"  # valor por defecto

if not df_stock.empty:
    print("🔄 Asignando stock por SKU...")
    for idx, row in df_stock.iterrows():
        try:
            sku = str(row.iloc[0]).strip()  # primera columna
            stock_value = row.iloc[1] if len(row) > 1 else None  # segunda columna

            # Normalizar stock vacío → 0
            if pd.isna(stock_value) or stock_value == "":
                stock_value = 0

            # Buscar coincidencias en el catálogo
            mask = df_es["SKU"].astype(str).str.strip() == sku
            if mask.any():
                df_es.loc[mask, "STOCK_LA62"] = stock_value
        except Exception as e:
            print(f"⚠️ Error procesando fila {idx}: {e}")

# --- dataframe final ---
df_result = pd.DataFrame({
    "SKU": df_es["SKU"],
    "MODELO": df_es["modello"],
    "CATEGORIA": df_es["categorySingular"],
    "STOCK_LA62": df_es["STOCK_LA62"],
    "PVP_WEB": df_es["PVP_WEB"],
    "URL": df_es["url"],
})

df_result.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Archivo generado en: {OUTPUT_FILE}")
