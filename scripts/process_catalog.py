import pandas as pd
import os
import numpy as np
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
    """Scrapea un lote de URLs con un driver independiente"""
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
        except Exception as e:
            results[url] = "NO"
            print(f"[Batch {batch_id}] ⚠️ {url}")

    driver.quit()
    return results


# --- paths ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "files", "catalog-products.xlsx")

today = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(BASE_DIR, f"productos_filtrados_{today}.xlsx")

# --- read input ---
df = pd.read_excel(INPUT_FILE)
df_es = df[df["lang"] == "es_ES"].copy()

urls = df_es["url"].apply(normalize_url).tolist()
print(f"🔎 Scrapear {len(urls)} productos en paralelo...")

# --- dividir URLs en lotes ---
num_batches = 4  # puedes ajustar a 3 o 4 según el tiempo
url_batches = np.array_split(urls, num_batches)

final_results = {}
with ThreadPoolExecutor(max_workers=num_batches) as executor:
    futures = {executor.submit(scrape_batch, batch, i): i for i, batch in enumerate(url_batches)}
    for future in as_completed(futures):
        final_results.update(future.result())

# --- asignar resultados ---
df_es["PVP_WEB"] = df_es["url"].apply(normalize_url).map(final_results)

# --- dataframe final ---
df_result = pd.DataFrame({
    "SKU": df_es["SKU"],
    "MODELO": df_es["modello"],
    "CATEGORIA": df_es["categorySingular"],
    "STOCK_LA62": "N/A",
    "PVP_WEB": df_es["PVP_WEB"],
    "URL": df_es["url"].apply(normalize_url),
})

df_result.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Archivo generado en: {OUTPUT_FILE}")
