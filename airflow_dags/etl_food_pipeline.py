from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

# Função para carregar os dados do excel e colocar no postgresql
def load_data_to_postges():
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
            """,

            row["category"], row["avg_retail_price"], row["unit"], row["prep_yield_factor"],
            row["cup_size"], row["cup_unit"], row["avg_price_cup"], row["food_type"] 
        )

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
    'elt_food_pipeline',
    default_args=default_args,
    description='Pipeline que para carregar dados de alimentos no PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

load_task = PythonOperator(
    task_id = 'load_data_to_postges',
    python_callable=load_data_to_postges,
    dag=dag_etl
)

load_task
