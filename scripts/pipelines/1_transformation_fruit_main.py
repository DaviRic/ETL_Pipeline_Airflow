import pandas as pd
import re
import os

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
    # Remove linhas que contêm palavras-chave em qualquer coluna
    df = df[~df.astype(str).apply(lambda row: row.str.contains("USDA|Excludes|Includes|Source|Consumers|The", na=False, case=False)).any(axis=1)]

    # Identificando subtítulos e dividindo em duas partes
    subtitle_row = df[df.iloc[:, 0].astype(str).str.match(r"^[A-Za-z\s]+$", na=False)].index
    
    if not subtitle_row.empty and subtitle_row[0] < len(df) - 1:
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
    if df_subtitle is not None and not df_subtitle.empty:
        df_subtitle["Category"] = "Subcategory"
        df_final = pd.concat([df_products, df_subtitle], ignore_index=True)
    else:
        df_final = df_products

    df_final["Fruit"] = fruit_name

    return df_final

raw_fruit_path = "data/raw/fruits"
processed_fruit_path = "datasets/processed_fruits"
os.makedirs(processed_fruit_path, exist_ok=True)

# Função para processar todas as planilhas de todas as subpastas
def process_all_fruit_excels():
    for subfolder in os.listdir(raw_fruit_path):
        # Está criando um caminho para acessar a subpasta dentro da pasta principal
        # O intúito dessa linha é acessar as subpastas "fruit-main", "fruit-2020", etc
        subfolder_path = os.path.join(raw_fruit_path, subfolder)

        # Verifica se é uma pasta
        if os.path.isdir(subfolder_path):
            # A variável "save_path" serve para salvar o caminho onde os dados processados serão salvos
            '''
            Pego o caminho para a pasta onde os dados processados ficarão salvos e coloco dentro da pasta
            com o nome que está em armazenado em "subfolder", que no caso é "fruit-main" ou "fruit-2020"
            ou "fruit-2013", etc
            '''
            save_path = os.path.join(processed_fruit_path, subfolder)
            # Se não existir a pasta, então cria para salvar os dados nela
            os.makedirs(save_path, exist_ok=True)

            # Vou iterar sobre cada pasta que contem as planilhas excel para transformá-las
            for file in os.listdir(subfolder_path):
                # Verifica se o arquivo termina com ".xlsx"
                if file.endswith(".xlsx"):
                    # Normaliza o nome do arquivo substituindo espaços e underscores por hífens
                    normalized_file = file.replace("_", "-").replace(" ", "-").lower()
                    # Extrai o nome da fruta pelo nome do arquivo e coloca todas as letras em minúscula
                    fruit_name = normalized_file.split("-")[0].lower()
                    
                    # Crio uma string com o nome do caminho do arquivo ".xlsx" para passar ele por parâmetro na função
                    # "processed_fruit_excel"
                    file_path = os.path.join(subfolder_path, file)

                    print(f"Processando {file} de {subfolder}...")

                    # Essa é a linha que transforma os dados. Remodelando a estrutura das colunas para melhorar o
                    # entendimento dos dados
                    df_fruit = processed_fruit_excel(file_path, fruit_name)

                    # Verifica se gerou um dataframe ao rodar "processed_fruit_excel"
                    if df_fruit is not None:
                        # Cria a string com o nome do caminho onde serão salvos os dados transformados
                        output_file = os.path.join(save_path, file)
                        # Transforma o dataframe em uma rquivo excel e salva no diretório cujo caminho está em "output_file"
                        df_fruit.to_excel(output_file, index=False)
                        print(f"Salvo: {output_file}")

    print("Todas as planilhas foram processadas e salvas!")

# Executa o processamento
process_all_fruit_excels()