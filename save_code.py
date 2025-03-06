import pandas as pd
import os
import re
# Pipeline ETL Completo com Python e Apache Airflow

# Caminho da pasta contendo as planilhas de frutas
fruit_main_path = "data/raw/fruits/fruit_main"
processed_path = "datasets"
os.makedirs(processed_path, exist_ok=True)

# Função para processar cada planilha
def processed_fruit_excel(file_path, fruit_name):
    xls = pd.ExcelFile(file_path)
    
    # Supondo que a principal aba tenha o nome da fruta
    sheet_name = fruit_name.capitalize()
    if sheet_name not in xls.sheet_names:
        print(f"Aba {sheet_name} não encontrada em {file_path}")
        return None
    
    df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=1)
    
    # Identificar subtítulos e dividir em duas partes
    subtitle_row = df[df.iloc[:, 0].str.match(r"^[A-Za-z\s]+$", na=False)].index
    
    if not subtitle_row.empty:
        df_products = df.iloc[:subtitle_row[0], :].copy()
        df_juices = df.iloc[subtitle_row[0] + 1 :, :].copy()
    else:
        df_products = df.copy()
        df_juices = None
    
    # Remover números no final dos produtos
    df_products.iloc[:, 0] = df_products.iloc[:, 0].apply(lambda x: re.sub(r"\d+$", "", str(x)))
    
    # Adicionar coluna indicando categoria
    df_products["Category"] = "Product"
    if df_juices is not None:
        df_juices["Category"] = "Subcategory"
        df_final = pd.concat([df_products, df_juices], ignore_index=True)
    else:
        df_final = df_products
    
    # Adicionar coluna com o nome da fruta
    df_final["Fruit"] = fruit_name
    
    return df_final

# Processar todas as planilhas
all_fruits_data = []

for file in os.listdir(fruit_main_path):
    if file.endswith(".xlsx"):
        fruit_name = file.split("-")[0].lower()
        file_path = os.path.join(fruit_main_path, file)
        print(f"Processando {file}...")
        df_fruit = processed_fruit_excel(file_path, fruit_name)
        if df_fruit is not None:
            all_fruits_data.append(df_fruit)

# Concatenar tudo em um único DataFrame
if all_fruits_data:
    df_all_fruits = pd.concat(all_fruits_data, ignore_index=True)
    df_all_fruits.to_csv(os.path.join(processed_path, "fruit_data_processed.csv"), index=False)
    print("Processamento concluído! Dados salvos em datasets/fruit_data_processed.csv")
else:
    print("Nenhum dado foi processado.")


# ===================================================================================================================


# Diretórios de origem e destino
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
                    # Extrai o nome da fruta pelo nome do arquivo e coloca todas as letras em minúscula
                    fruit_name = file.split("-")[0].lower()
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


# Tem como eu fazer condições na hora de definir o separador? Em fruit-2020, fruit-2013 e fruit-2016 os nomes das planilhas estão peradas por espaços ou undescores (_), por exemplo: apples_2013, apricot 2016 ou Figs 2020. Então eu queria tratar esses casos, porque eu não quero renomear tudo não