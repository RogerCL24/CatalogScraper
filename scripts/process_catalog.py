import pandas as pd
import os

# Ruta relativa al proyecto
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # carpeta Project/
INPUT_FILE = os.path.join(BASE_DIR, "files", "catalog-products.xlsx")
OUTPUT_FILE = os.path.join(BASE_DIR, "output", "productos_filtrados.xlsx")

# Leer Excel completo
df = pd.read_excel(INPUT_FILE)

# Filtrar por idioma español (columna lang == "es_ES")
df_es = df[df["lang"] == "es_ES"].copy()

# Seleccionar columnas que nos interesan
df_result = pd.DataFrame({
    "SKU": df_es["SKU"],
    "MODELO": df_es["modello"],
    "CATEGORIA": df_es["category"],
    "STOCK_LA62": "N/A",   # todavía no disponible
    "PVP_WEB": "N/A",      # todavía no disponible
    "URL": df_es["url"]
})

# Crear carpeta output si no existe
os.makedirs(os.path.join(BASE_DIR, "output"), exist_ok=True)

# Guardar como Excel
df_result.to_excel(OUTPUT_FILE, index=False)

print(f"✅ Archivo generado en: {OUTPUT_FILE}")
