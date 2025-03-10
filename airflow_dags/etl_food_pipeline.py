from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os
import re

# Função auxiliar que retira o número do fim nomes do elementos das colunas
def clean_name(name):   
    return re.sub(r'\d+$', '', str(name)).strip()

# Função que processa cada planilha dentro da pasta fruits que contém os dados brutos. Essa função é a parte de
# Transform para todas as planilhas da pasta fruits
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

# Diretórios de acesso das pastas as quais serão transformadas
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


# Função que processa cada planilha dentro da pasta vegetables que contém os dados brutos. Essa função é a parte de
# Transform para todas as planilhas da pasta vegetables
def processed_vegetables_excel(file_path, vegetable_name):
    xls_file = pd.ExcelFile(file_path)

    sheet_name = vegetable_name.capitalize()
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

    df_final["Vegetable"] = vegetable_name

    return df_final

raw_vegetable_path = "data/raw/vegetables"
processed_vegetable_path = "datasets/processed_vegetables"
os.makedirs(processed_vegetable_path, exist_ok=True)

# Função para processar todas as planilhas de todas as subpastas
def process_all_vegetable_excels():
    for subfolder in os.listdir(raw_vegetable_path):
        # Está criando um caminho para acessar a subpasta dentro da pasta principal
        # O intúito dessa linha é acessar as subpastas "vegetable-main", "vegetable-2020", etc
        subfolder_path = os.path.join(raw_vegetable_path, subfolder)

        # Verifica se é uma pasta
        if os.path.isdir(subfolder_path):
            # A variável "save_path" serve para salvar o caminho onde os dados processados serão salvos
            '''
            Pego o caminho para a pasta onde os dados processados ficarão salvos e coloco dentro da pasta
            com o nome que está em armazenado em "subfolder", que no caso é "vegetable-main" ou "vegetable-2020"
            ou "vegetable-2013", etc
            '''
            save_path = os.path.join(processed_vegetable_path, subfolder)
            # Se não existir a pasta, então cria para salvar os dados nela
            os.makedirs(save_path, exist_ok=True)

            # Vou iterar sobre cada pasta que contem as planilhas excel para transformá-las
            for file in os.listdir(subfolder_path):
                # Verifica se o arquivo termina com ".xlsx"
                if file.endswith(".xlsx"):
                    # Normaliza o nome do arquivo substituindo espaços e underscores por hífens
                    normalized_file = file.replace("_", "-").replace(" ", "-").lower()
                    # Extrai o nome da fruta pelo nome do arquivo e coloca todas as letras em minúscula
                    vegetable_name = normalized_file.split("-")[0].lower()
                    
                    # Crio uma string com o nome do caminho do arquivo ".xlsx" para passar ele por parâmetro na função
                    # "processed_vegetables_excel"
                    file_path = os.path.join(subfolder_path, file)

                    print(f"Processando {file} de {subfolder}...")

                    # Essa é a linha que transforma os dados. Remodelando a estrutura das colunas para melhorar o
                    # entendimento dos dados
                    df_vegetable = processed_vegetables_excel(file_path, vegetable_name)

                    # Verifica se gerou um dataframe ao rodar "processed_vegetables_excel"
                    if df_vegetable is not None:
                        # Cria a string com o nome do caminho onde serão salvos os dados transformados
                        output_file = os.path.join(save_path, file)
                        # Transforma o dataframe em uma rquivo excel e salva no diretório cujo caminho está em "output_file"
                        df_vegetable.to_excel(output_file, index=False)
                        print(f"Salvo: {output_file}")

    print("Todas as planilhas foram processadas e salvas!")


# Consolidação dos dados de fruits e de vegetables (Apenas os dados princiais de fruit e vegetables, no caso, os dados
# atualizados mais recentemente) 

# Diretórios onde os arquivos de frutas e vegetais transformados estão salvos
processed_fruit_path = "datasets/processed_fruits/fruit-main"
processed_vegetable_path = "datasets/processed_vegetables/vegetable-main"


# Função para processar as planilhas de frutas e vegetais
def process_files_in_directory(directory, food_type):
    df_list = []

    for file in os.listdir(directory):
        if file.endswith(".xlsx"):
            file_path = os.path.join(directory, file)
            
            # Lê a planilha
            df = pd.read_excel(file_path)
            
            # Se a coluna "Fruit" ou "Vegetable" existir, a removemos
            if "Fruit" in df.columns:
                df = df.drop(columns=["Fruit"])
            if "Vegetable" in df.columns:
                df = df.drop(columns=["Vegetable"])

            # Adiciona a coluna food_type (fruit ou vegetable)
            df["food_type"] = food_type
            
            # Adiciona na lista
            df_list.append(df)
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

# Processa todos os arquivos em processed_fruit_path (fruits) e processed_vegetable_path (vegetables)
def consolidate_all_data():
    df_fruits = process_files_in_directory(processed_fruit_path, "fruit")
    df_vegetables = process_files_in_directory(processed_vegetable_path, "vegetable")

    # Junta os DataFrames, mas agora sem precisar da variável global
    df_consolidado = pd.concat([df_fruits, df_vegetables], ignore_index=True)

    # Remove linhas com valores NaN em qualquer coluna
    df_consolidado = df_consolidado.dropna(how='any')

    # Salva o DataFrame consolidado em um único arquivo Excel
    output_file = "datasets/consolidated_files/consolidated_food_main.xlsx"
    df_consolidado.to_excel(output_file, index=False)

# Função para carregar os dados do excel e colocar no postgresql
def load_data_to_postgres():
    # Conectar ao banco
    conn = psycopg2.connect(
        dbname = 'food_db',
        user = 'postgres',
        password = '123',
        host = 'localhost',
        port = '5432'
    )
    cursor = conn.cursor()
    
    file_path = 'datasets/consolidated_files/consolidated_food_main.xlsx'
    df = pd.read_excel(file_path)

    # Inserir dados no banco
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO food_info (category, avg_retail_price, unit, prep_yield_factor, cup_size, cup_unit, avg_price_cup, food_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (

            row["Category"], row["Avg_Retail_price"], row["Unit"], row["Prep_Yield_Factor"],
            row["Cup_Size"], row["Cup_Unit"], row["Avg_Price_Cup"], row["food_type"] 
        ))

    conn.commit()
    cursor.close()
    conn.close()
    
# Definição do DAG
default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2025, 3, 8),
    'retries' : 1
}

dag_etl = DAG (
    'etl_food_pipeline',
    default_args=default_args,
    description='Pipeline que para carregar dados de alimentos no PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

process_fruit = PythonOperator(
    task_id='process_all_fruit_excels',
    python_callable=process_all_fruit_excels,
    dag=dag_etl
)

process_vegetables = PythonOperator(
    task_id='process_all_vegetable_excels',
    python_callable=process_all_vegetable_excels,
    dag=dag_etl
)

consolidate_files_data = PythonOperator(
    task_id='consolidate_all_data',
    python_callable=consolidate_all_data,
    dag=dag_etl
)

load_task = PythonOperator(
    task_id = 'load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag_etl
)

[process_fruit, process_vegetables] >> consolidate_files_data >> load_task
