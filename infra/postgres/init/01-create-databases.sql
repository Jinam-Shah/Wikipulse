-- Runs only the first time the postgres volume is created.
-- The default database (POSTGRES_DB, "wikipulse") is created by the image itself.
-- Airflow keeps its metadata in a separate database so it never mixes with app data.
CREATE DATABASE airflow;
