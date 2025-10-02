import pandas as pd
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# --- helpers ---
def normalize_url(url: str) -> str:
    """Asegura que la URL tenga esquema https://"""
    if not url.startswith("http"):
        return "https://" + url.lstrip("/")
    return url


def get_pvp(url: str) -> str:
    """Abre Selenium en headless, va a la URL y obtiene el precio"""
    url = normalize_url(url)
    try:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(options=opts)
        driver.get(url)

        wait = WebDriverWait(driver, 10)
        price_el = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h2.product-detail__price"))
        )
        price = price_el.text.strip()
        driver.quit()

        print(f"✅ Precio encontrado en {url}: {price}")
        return price
    except Exception as e:
        print(f"⚠️ Error en {url}: {e}")
        try:
            driver.quit()
        except:
            pass
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

print(f"🔎 Scrapear {len(urls)} productos en paralelo con Selenium...")

with ThreadPoolExecutor(max_workers=5) as executor:  # Selenium es pesado → mejor 5 en paralelo
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
