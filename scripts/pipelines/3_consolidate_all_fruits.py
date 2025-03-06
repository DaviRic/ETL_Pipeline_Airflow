import pandas as pd
import os

# Diretório onde os arquivos transformados foram salvos
processed_fruit_path = "datasets/processed_fruits"

# Lista para armazenar os DataFrames
df_list = []

# Percorre todas as subpastas dentro de processed_fruits
for subfolder in os.listdir(processed_fruit_path):
    subfolder_path = os.path.join(processed_fruit_path, subfolder)

    if os.path.isdir(subfolder_path):
        # Percorre todos os arquivos Excel dentro das subpastas
        for file in os.listdir(subfolder_path):
            if file.endswith(".xlsx"):
                file_path = os.path.join(subfolder_path, file)
                
                # Lê a planilha
                df = pd.read_excel(file_path)
                
                # Adiciona na lista
                df_list.append(df)

# Junta todos os DataFrames em um único DataFrame
df_consolidado = pd.concat(df_list, ignore_index=True)

# Remove linhas onde a "Category" está vazia
df_consolidado = df_consolidado.dropna(subset=["Avg_Retail_price"])

# Salva o DataFrame consolidado em um único arquivo Excel
output_file = "datasets/consolidated_files/consolidated_fruits.xlsx"
df_consolidado.to_excel(output_file, index=False)

print(f"Arquivo consolidado salvo em: {output_file}")