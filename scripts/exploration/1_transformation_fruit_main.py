import pandas as pd
import re
import os

fruit_main_path = "data/raw/fruits/fruit-main"
processed_path = "datasets"
os.makedirs(processed_path, exist_ok=True)

# Função que processa cada planilha dentro da pasta fruits
def processed_fruit_excel(file_path, fruit_name):
    xls_file = pd.ExcelFile(file_path)

    sheet_name = fruit_name.capitalize()
    if sheet_name not in xls_file.sheet_names:
        print(f"Aba {sheet_name} não encontrada em {file_path}")
        return None
    
    df = pd.read_excel(xls_file, sheet_name=sheet_name, skiprows=1)

    # Identificando subtítulos e dividindo em duas partes
    subtitle_row = df[df.iloc[:, 0].str.match(r"^[A-Za-z\s]+$", na=False)].index

    if not subtitle_row.empty:
        df_products = df.iloc[:subtitle_row[0], :].copy()
        df_subtitle = df.iloc[subtitle_row[0]+1:, :].copy()