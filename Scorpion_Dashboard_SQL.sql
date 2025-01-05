-- 1. SQL Dotaz pre počet hodnotení podľa žánru (TOP 10)
SELECT dg.name AS genre, COUNT(fr.id) AS rating_count
FROM fact_ratings AS fr
JOIN dim_genres AS dg ON fr.dim_genres_id = dg.id
GROUP BY dg.name
ORDER BY rating_count DESC
LIMIT 10;

-- 2. SQL Dotaz pre najobľúbenejšie filmy (TOP 10)
SELECT m.title, ROUND(AVG(fr.rating), 2) AS avg_rating, COUNT(fr.id) AS rating_count
FROM fact_ratings AS fr
JOIN dim_movies AS m ON fr.dim_movies_id = m.id
GROUP BY  m.title
HAVING COUNT(fr.id) >= 30
ORDER BY title
LIMIT 10;

-- 3. SQL Dotaz pre najaktívnejších používateľov (TOP 10)
SELECT fr.dim_users_id AS user_id, COUNT(fr.id) AS rating_count
FROM fact_ratings AS fr
GROUP BY fr.dim_users_id
ORDER BY rating_count DESC
LIMIT 10;

-- 4. SQL Dotaz pre aktivitu počas dni v týždni (7 dní)
SELECT d.day_of_week AS day, COUNT(r.id) AS total_ratings
FROM fact_ratings AS r
JOIN dim_date AS d ON r.dim_date_id = d.id
GROUP BY d.day_of_week
ORDER BY day ASC;

-- 5. SQL Dotaz pre počet hodnotení podľa povolaní (TOP 10)
SELECT u.occupations_name AS occupation, COUNT(r.id) AS total_ratings
FROM fact_ratings AS r
JOIN dim_users AS u ON r.dim_users_id = u.id
GROUP BY  u.occupations_name
ORDER BY total_ratings DESC
LIMIT 10;
