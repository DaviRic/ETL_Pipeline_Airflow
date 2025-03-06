import pandas as pd
import psycopg2
from psycopg2 import sql

# Conexão com o banco de dados
db_name = "food_db"
db_user = "postgres"
db_password = "123"
db_host = "localhost"
db_port = "5432"

conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)

# Criar um cursor
cursor = conn.cursor()

file_path = "datasets/consolidated_files/consolidated_food_main.xlsx"
df_consolidado = pd.read_excel(file_path)

# Função que insere os dados na tabela "food_info" do banco de dados food_db
def insert_data_into_db(df):
    for index, row in df.iterrows():
        # Montando a query de inserção e aramazenando em insert_query
        insert_query = sql.SQL("""
            INSERT INTO food_info (
                Category, Avg_Retail_price, Unit, Prep_Yield_Factor, Cup_Size, Cup_Unit,
                Avg_Price_Cup, food_type
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """)

        # Dados da linha a ser inserida
        data = (
            row["Category"],
            row["Avg_Retail_price"],
            row["Unit"],
            row["Prep_Yield_Factor"],
            row["Cup_Size"],
            row["Cup_Unit"],
            row["Avg_Price_Cup"],
            row["food_type"]
        )

        # Executando a query para inserir os dado no banco de dados
        cursor.execute(insert_query, data)
    
    # Commit para salvar as mudanças no banco de dados
    conn.commit()

# Inserir os dados do dataframe na tabela
insert_data_into_db(df_consolidado)

cursor.close()
conn.close()