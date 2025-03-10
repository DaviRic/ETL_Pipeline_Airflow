from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os
import re

fruit_main_path = "data/raw/fruits/fruit-main"
processed_path = "datasets"

def clean_name(name):   
    return re.sub(r'\d+$', '', str(name)).strip()

def transform_data(file_path, fruit_name):
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

load_task = PythonOperator(
    task_id = 'load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag_etl
)

load_task
