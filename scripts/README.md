# Movie Data Engineering Pipeline

## Team
- 2320030493 — A. Vinod
- 2320030224 — R. Akshaya
- 2320030231 — Harshitha

## Course
Fundamentals of Data Engineering

## Project Overview
This project demonstrates an end-to-end ETL pipeline that processes movie data from TMDB,
cleans it, loads it into a relational database (SQLite), and performs SQL-based analysis.

### Workflow
1. Ingest raw CSV files (`tmdb_5000_movies.csv`, `tmdb_5000_credits.csv`) into Python.
2. Clean and transform the data (parse JSON fields, handle missing values).
3. Load transformed data into `movies.db` (SQLite) using SQLAlchemy.
4. Run SQL queries and visualize results (e.g., top genres, budget vs revenue).

## Files
- `scripts/` — Python scripts:
  - `movie_ingestion.py`
  - `movie_cleaning.py`
  - `movie_load.py`
  - `movie_analysis.py`
- `data/` — raw CSVs and optional `movies.db` (not recommended for repo if large)
- `README.md`
- `requirements.txt`

## How to run
1. Install Python 3 and dependencies:


## Notes
- We used SQLite for easy local prototyping and SQLAlchemy to interface Python with the DB.
- The full TMDB CSVs are large; optionally include only sample files in `data/` or provide download instructions.

## Future work
- Add dashboard (Streamlit / Power BI)
- Automate with Airflow
- Move DB to cloud (Postgres / RDS)
