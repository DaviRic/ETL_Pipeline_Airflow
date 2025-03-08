-- Qual o preço médio das frutas e dos vegetais
SELECT food_type, AVG(avg_retail_price)
FROM food_info
GROUP BY food_type;

-- Top 5 alimentos mais caros
SELECT category, food_type, avg_retail_price
FROM food_info
ORDER BY avg_retail_price DESC
LIMIT 5;

-- Top 5 alimentos mais baratos
SELECT category, food_type, avg_retail_price
FROM food_info
ORDER BY avg_retail_price
LIMIT 5;

-- Média de rendimento por tipo de alimento
SELECT SUM(avg_retail_price)
FROM food_info
GROUP BY food_type;