import pandas as pd
import os
from datetime import datetime
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


def get_pvp(driver, url: str) -> str:
    """Usa un solo driver para obtener el PVP de la URL"""
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 8)

        price_el = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h2.product-detail__price"))
        )
        price = price_el.text.strip()
        print(f"✅ {url} -> {price}")
        return price

    except Exception as e:
        print(f"⚠️ {url} -> {str(e).splitlines()[0]}")
        return "NO"


# --- paths ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "files", "catalog-products.xlsx")

today = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(BASE_DIR, f"productos_filtrados_{today}.xlsx")

# --- read input ---
df = pd.read_excel(INPUT_FILE)
df_es = df[df["lang"] == "es_ES"].copy()

urls = df_es["url"].apply(normalize_url).tolist()
pvp_results = {}

print(f"🔎 Scrapear {len(urls)} productos con un solo navegador Selenium...")

# --- init single driver ---
opts = Options()
opts.binary_location = CHROME_BIN
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=opts)

# recorrer todas las urls
for url in urls:
    pvp_results[url] = get_pvp(driver, url)

driver.quit()

# --- asignar resultados ---
df_es["PVP_WEB"] = df_es["url"].apply(normalize_url).map(pvp_results)

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
