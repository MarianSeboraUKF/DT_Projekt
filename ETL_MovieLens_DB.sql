-- Vytvoríme novú databázu s názvom SCORPION_MovieLens.
CREATE OR REPLACE DATABASE SCORPION_MovieLens;

-- Nastavíme databázu SCORPION_MovieLens ako aktívnu pre aktuálnu reláciu.
USE SCORPION_MovieLens;

-- Vytvorenie schémy pre staging tabuľky
CREATE OR REPLACE SCHEMA SCORPION_MovieLens.staging;

USE SCHEMA SCORPION_MovieLens.staging;

CREATE OR REPLACE STAGE scorpion_stage;


-- Staging tabuľky pre načítanie dát
-- Vytvorenie stage tabuľky pre age_group
CREATE OR REPLACE TABLE age_group_staging (
    id INT PRIMARY KEY,
    name VARCHAR(45)
);

-- Vytvorenie stage tabuľky pre occupations
CREATE OR REPLACE TABLE occupations_staging (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- Vytvorenie stage tabuľky pre users
CREATE OR REPLACE TABLE users_staging (
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(25),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id)
);

-- Vytvorenie stage tabuľky pre movies
CREATE OR REPLACE TABLE movies_staging (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

-- Vytvorenie stage tabuľky pre genres
CREATE OR REPLACE TABLE genres_staging (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- Vytvorenie stage tabuľky pre genres_movies
CREATE OR REPLACE TABLE genres_movies_staging (
    id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(id)
);

-- Vytvorenie stage tabuľky pre tags
CREATE OR REPLACE TABLE tags_staging (
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    tags VARCHAR(4000),
    created_at DATETIME
);

-- Vytvorenie stage tabuľky pre ratings
CREATE OR REPLACE TABLE ratings_staging (
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);

-- Načítanie údajov a dát z CSV súborov
COPY INTO age_group_staging
FROM @scorpion_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO occupations_staging
FROM @scorpion_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO users_staging
FROM @scorpion_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO movies_staging
FROM @scorpion_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_staging
FROM @scorpion_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_movies_staging
FROM @scorpion_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO tags_staging
FROM @scorpion_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO ratings_staging
FROM @scorpion_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Vytvorenie dimenzie pre tags
CREATE OR REPLACE TABLE dim_tags AS
SELECT
    ROW_NUMBER() OVER (ORDER BY tags) AS ID,
    tags
FROM tags_staging
GROUP BY tags;

-- Vystvorenie dimenzie pre time
CREATE OR REPLACE TABLE dim_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY EXTRACT(HOUR FROM rated_at), EXTRACT(MINUTE FROM rated_at)) AS ID,
    EXTRACT(HOUR FROM rated_at) AS hour,
    EXTRACT(MINUTE FROM rated_at) AS minute,
    EXTRACT(SECOND FROM rated_at) AS second
FROM ratings_staging
GROUP BY EXTRACT(HOUR FROM rated_at), EXTRACT(MINUTE FROM rated_at), EXTRACT(SECOND FROM rated_at)
ORDER BY hour, minute, second;

-- Vytvorenie dimenzie pre date
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS ID,
    CAST(rated_at AS DATE) AS date,
    EXTRACT(DAY FROM rated_at) AS day,
    MOD(EXTRACT(DOW FROM rated_at) + 1, 7) + 1 AS day_of_week,
    CASE MOD(EXTRACT(DOW FROM rated_at) + 1, 7) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS day_of_week_as_string,
    EXTRACT(MONTH FROM rated_at) AS month,
    EXTRACT(YEAR FROM rated_at) AS year,
    EXTRACT(QUARTER FROM rated_at) AS quarter
FROM (
    SELECT DISTINCT
        CAST(rated_at AS DATE) AS rated_at,
        EXTRACT(DAY FROM rated_at) AS day,
        EXTRACT(DOW FROM rated_at) AS dow,
        EXTRACT(MONTH FROM rated_at) AS month,
        EXTRACT(YEAR FROM rated_at) AS year,
        EXTRACT(QUARTER FROM rated_at) AS quarter
    FROM ratings_staging
) unique_dates;

-- Vytvorenie dimenzie pre movies
CREATE OR REPLACE TABLE dim_movies AS
SELECT
    ROW_NUMBER() OVER (ORDER BY title) AS ID,
    title
FROM movies_staging
GROUP BY title;

-- Vytvorenie dimenzie pre genres
CREATE OR REPLACE TABLE dim_genres AS
SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS ID,
    name
FROM genres_staging
GROUP BY name;

-- Vytvorenie dimenzie pre users
CREATE OR REPLACE TABLE dim_users AS
SELECT
    u.id AS ID,
    CASE 
        WHEN u.age < 18 THEN 'Under 18'
        WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN u.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN u.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN u.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group_name,
    u.gender,
    o.name AS occupations_name,
FROM USERS_STAGING u
JOIN occupations_staging o ON u.occupation_id = o.id;


-- Vytvorenie fact_ratings tabuľky
CREATE OR REPLACE TABLE fact_ratings AS
SELECT 
    r.id AS ID,
    r.rating,
    du.ID AS dim_users_ID,
    dt.ID AS dim_tags_ID,
    dm.ID AS dim_movies_ID,
    dg.ID AS dim_genres_ID,
    dtime.ID AS dim_time_ID,
    ddate.ID AS dim_date_ID
FROM ratings_staging r
JOIN dim_users du ON r.user_id = du.ID
JOIN dim_movies dm ON r.movie_id = dm.ID
LEFT JOIN tags_staging ts ON r.user_id = ts.user_id AND r.movie_id = ts.movie_id
LEFT JOIN dim_tags dt ON ts.tags = dt.tags
JOIN genres_movies_staging gm ON r.movie_id = gm.movie_id
JOIN dim_genres dg ON gm.genre_id = dg.ID
JOIN dim_time dtime 
    ON EXTRACT(HOUR FROM r.rated_at) = dtime.hour
   AND EXTRACT(MINUTE FROM r.rated_at) = dtime.minute
   AND EXTRACT(SECOND FROM r.rated_at) = dtime.second
JOIN dim_date ddate ON CAST(r.rated_at AS DATE) = ddate.date;

-- Dropnutie dočasných stage tabuliek
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS ratings_staging;
