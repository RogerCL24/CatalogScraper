import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Ruta relativa al proyecto
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # carpeta Project/
INPUT_FILE = os.path.join(BASE_DIR, "files", "catalog-products-csv-_3_.xlsx")

today = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(BASE_DIR, f"productos_filtrados_{today}.xlsx")

def normalize_url(url):
    if not url.startswith("http"):
        return "https://" + url.lstrip("/")
    return url

def get_pvp(url):
    """Scrapea el PVP desde la página del producto"""
    try:
        url = normalize_url(url)
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        price_tag = soup.select_one("h2.product-detail__price")
        if price_tag:
            return price_tag.get_text(strip=True)
        else:
            return "NO"
    except Exception as e:
        print(f"⚠️ Error al scrapear {url}: {e}")
        return "NO"


# Leer Excel completo
df = pd.read_excel(INPUT_FILE)

# Filtrar por idioma español
df_es = df[df["lang"] == "es_ES"].copy()

# Scraping del PVP para cada producto
df_es["PVP_WEB"] = df_es["url"].apply(get_pvp)

# Crear dataframe con columnas necesarias
df_result = pd.DataFrame({
    "SKU": df_es["SKU"],
    "MODELO": df_es["modello"],
    "CATEGORIA": df_es["categorySingular"],
    "STOCK_LA62": "N/A",   # aún no disponible
    "PVP_WEB": df_es["PVP_WEB"],      
    "URL": df_es["url"]
})

# Guardar directamente en la raíz
df_result.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Archivo generado en: {OUTPUT_FILE}")
