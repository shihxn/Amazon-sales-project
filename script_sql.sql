LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.2/Uploads/Amazon-Products.csv'
INTO TABLE dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; -- Skip the header row

ALTER TABLE dataset
MODIFY COLUMN actual_price varchar(1000);

SELECT *
FROM dataset;

SELECT *
FROM dataset
WHERE id IS NULL 
   OR name IS NULL 
   OR main_category IS NULL 
   OR sub_category IS NULL 
   OR ratings IS NULL 
   OR no_of_ratings IS NULL 
   OR discount_price IS NULL 
   OR actual_price IS NULL;
   
   DELETE FROM dataset
WHERE id IN (
    SELECT id
    FROM (
        SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_num
        FROM dataset
    ) AS temp
    WHERE row_num > 1
);

UPDATE dataset
SET ratings = 0
WHERE ratings IS NULL;

UPDATE dataset
SET no_of_ratings = 0
WHERE no_of_ratings IS NULL;

UPDATE dataset
SET discount_price = actual_price
WHERE discount_price IS NULL;

SELECT main_category, COUNT(id) AS total_products
FROM dataset
GROUP BY main_category
ORDER BY total_products DESC;

SELECT sub_category, AVG(ratings) AS avg_rating
FROM dataset
GROUP BY sub_category
ORDER BY avg_rating DESC;

SELECT name, no_of_ratings
FROM dataset
ORDER BY no_of_ratings DESC
LIMIT 10;

SELECT name, actual_price, discount_price,
       ROUND(((actual_price - discount_price) / actual_price) * 100, 2) AS discount_percentage
FROM dataset;

SELECT name, discount_price, actual_price, ratings,
       ROUND(((actual_price - discount_price) / actual_price) * 100, 2) AS discount_percentage
FROM dataset
WHERE ((actual_price - discount_price) / actual_price) * 100 > 50
  AND ratings < 3;
  
  SELECT main_category, SUM(discount_price * no_of_ratings) AS total_revenue
FROM dataset
GROUP BY main_category
ORDER BY total_revenue DESC;

SELECT sub_category, SUM(no_of_ratings) AS total_ratings
FROM dataset
GROUP BY sub_category
ORDER BY total_ratings DESC;

SELECT 
    CASE 
        WHEN ((actual_price - discount_price) / actual_price) * 100 > 50 THEN 'High Discount'
        WHEN ((actual_price - discount_price) / actual_price) * 100 BETWEEN 20 AND 50 THEN 'Medium Discount'
        ELSE 'Low Discount'
    END AS discount_category,
    AVG(ratings) AS avg_rating,
    SUM(no_of_ratings) AS total_ratings
FROM dataset
GROUP BY discount_category
ORDER BY avg_rating DESC;

SELECT `name`, main_category, sub_category, ratings, no_of_ratings, discount_price, actual_price,
       ROUND(((actual_price - discount_price) / actual_price) * 100, 2) AS discount_percentage
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 9.2/Uploads/Amazon-Products2.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM dataset;
