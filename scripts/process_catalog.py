import pandas as pd
import os
from datetime import datetime

# Ruta relativa al proyecto
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # carpeta Project/
INPUT_FILE = os.path.join(BASE_DIR, "files", "catalog-products-csv-_3_.xlsx")

today = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(BASE_DIR, f"productos_filtrados_{today}.xlsx")

# Leer Excel completo
df = pd.read_excel(INPUT_FILE)

# Filtrar por idioma español
df_es = df[df["lang"] == "es_ES"].copy()

# Crear dataframe con columnas necesarias
df_result = pd.DataFrame({
    "SKU": df_es["SKU"],
    "MODELO": df_es["modello"],
    "CATEGORIA": df_es["categorySingular"],
    "STOCK_LA62": "N/A",   # aún no disponible
    "PVP_WEB": "N/A",      # aún no disponible
    "URL": df_es["url"]
})

# Guardar directamente en la raíz
df_result.to_excel(OUTPUT_FILE, index=False)

print(f"✅ Archivo generado en: {OUTPUT_FILE}")
