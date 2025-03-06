import pandas as pd
import os

# Diretórios onde os arquivos de frutas e vegetais transformados estão salvos
processed_fruit_path = "datasets/processed_fruits/fruit-main"
processed_vegetable_path = "datasets/processed_vegetables/vegetable-main"

# Lista para armazenar os DataFrames
df_list = []

# Função para processar as planilhas de frutas e vegetais
def process_files_in_directory(directory, food_type):
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

# Processa todos os arquivos em processed_fruit_path (fruits) e processed_vegetable_path (vegetables)
process_files_in_directory(processed_fruit_path, "fruit")
process_files_in_directory(processed_vegetable_path, "vegetable")

# Junta todos os DataFrames em um único DataFrame
df_consolidado = pd.concat(df_list, ignore_index=True)

# Remove linhas com valores NaN em qualquer coluna
df_consolidado = df_consolidado.dropna(how='any')

# Salva o DataFrame consolidado em um único arquivo Excel
output_file = "datasets/consolidated_files/consolidated_food_main.xlsx"
df_consolidado.to_excel(output_file, index=False)

print(f"Arquivo consolidado (sem NaN) salvo em: {output_file}")