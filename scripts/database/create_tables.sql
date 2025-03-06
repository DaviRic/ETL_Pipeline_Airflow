-- Cria o banco de dados
CREATE DATABASE food_db;

-- usar o banco de dados food_db
\c food_db;

-- Criar a tabela products
CREATE TABLE IF NOT EXISTS food_info (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    avg_retail_price NUMERIC(10,2),
    unit VARCHAR(20),
    prep_yield_factor NUMERIC(10,2),
    cup_size NUMERIC(10,2),
    cup_unit VARCHAR(20),
    avg_price_cup NUMERIC(10,2),
    food_type TEXT CHECK(food_type IN ('fruit', 'vegetable')) NOT NULL
);