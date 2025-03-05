import pandas as pd
import re

fruit_main_path = "data/raw/fruits/fruit-main"
processed_path = "datasets"

def clean_name(name):   
    return re.sub(r'\d+$', '', str(name)).strip()

# Função que processa cada planilha dentro da pasta fruits
def processed_fruit_excel(file_path, fruit_name):
    xls_file = pd.ExcelFile(file_path)

    sheet_name = fruit_name.capitalize()
    if sheet_name not in xls_file.sheet_names:
        print(f"Aba {sheet_name} não encontrada em {file_path}")
        return None
    
    df = pd.read_excel(xls_file, sheet_name=sheet_name, skiprows=1)
    df = df[~df.iloc[:, 0].astype(str).str.contains("USDA|Excludes|Includes|Source", na=False)]

    # Identificando subtítulos e dividindo em duas partes
    subtitle_row = df[df.iloc[:, 0].astype(str).str.match(r"^[A-Za-z\s]+$", na=False)].index
    if not subtitle_row.empty:
        df_products = df.iloc[:subtitle_row[0]].reset_index(drop=True)
        df_products.iloc[:, 0] = df_products.iloc[:, 0].apply(clean_name)
        # Renomeando colunas
        df_products.columns = [
            "Category", "Avg_Retail_price", "Unit", "Prep_Yield_Factor",
            "Cup_Size", "Cup_Unit", "Avg_Price_Cup"
        ]

        df_subtitle = df.iloc[subtitle_row[0] + 1:].reset_index(drop=True)
        df_subtitle.iloc[:, 0] = df_subtitle.iloc[:, 0].apply(clean_name)
        # Renomeando colunas
        df_subtitle.columns = [
            "Category", "Avg_Retail_price", "Unit", "Prep_Yield_Factor",
            "Cup_Size", "Cup_Unit", "Avg_Price_Cup"
        ]
    else:
        df_products = df.copy()
        df_products.iloc[:, 0] = df_products.iloc[:, 0].apply(clean_name)
        df_products.columns = [
            "Category", "Avg_Retail_price", "Unit", "Prep_Yield_Factor",
            "Cup_Size", "Cup_Unit", "Avg_Price_Cup"
        ]
        df_subtitle = None

    df_products["Category"] = "Product"
    if df_subtitle is not None:
        df_subtitle["Category"] = "Subcategory"
        df_final = pd.concat([df_products, df_subtitle], ignore_index=True)
    else:
        df_final = df_products

    df_final["Fruit"] = fruit_name

    return df_final

