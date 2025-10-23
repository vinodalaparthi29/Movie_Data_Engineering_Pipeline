# check_database.py

import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect("../data/movies.db")

# Step 1: List all tables
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
print("Tables in the database:")
print(tables)

# Step 2: See first few rows from the main 'movies' table
print("\n Sample records from 'movies' table:")
movies_sample = pd.read_sql_query("SELECT * FROM movies LIMIT 5;", conn)
print(movies_sample)

# Step 3: Check number of rows
row_count = pd.read_sql_query("SELECT COUNT(*) AS total_movies FROM movies;", conn)
print("\n Total rows in 'movies' table:", row_count.iloc[0]['total_movies'])

# Step 4 (optional): Explore other tables if you created normalized ones
# e.g. genres, directors, cast tables
# directors_sample = pd.read_sql_query("SELECT * FROM directors LIMIT 5;", conn)
# print("\n Directors sample:")
# print(directors_sample)

conn.close()

