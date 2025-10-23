import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

# Connect to your SQLite database
engine = create_engine("sqlite:///../data/movies.db")
# Check columns available in 'movies' table
# Check what columns are available in 'movies' table
with engine.connect() as conn:
    df_columns = pd.read_sql(text("PRAGMA table_info(movies);"), conn)
    print("\nColumns in 'movies' table:\n")
    print(df_columns[['name', 'type']])

# Helper function to run SQL and return DataFrame
def run_query(sql):
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    return df


# Top 10 genres by movie count
query_genres = """
SELECT g.name AS genre, COUNT(mg.movie_id) AS movie_count
FROM genres g
JOIN movie_genres mg ON g.id = mg.genre_id
GROUP BY g.name
ORDER BY movie_count DESC
LIMIT 10;
"""
df_genres = run_query(query_genres)
print("Top 10 genres by movie count:\n", df_genres)

# Plot
plt.figure(figsize=(10,5))
plt.barh(df_genres['genre'], df_genres['movie_count'], color='skyblue')
plt.gca().invert_yaxis()
plt.title("Top 10 Movie Genres")
plt.xlabel("Number of Movies")
plt.ylabel("Genre")
plt.tight_layout()
plt.show()


# Average popularity by genre
query_rating = """
SELECT g.name AS genre, ROUND(AVG(m.vote_average), 2) AS avg_rating
FROM movies m
JOIN movie_genres mg ON m.id = mg.movie_id
JOIN genres g ON g.id = mg.genre_id
GROUP BY g.name
ORDER BY avg_rating DESC
LIMIT 10;
"""
df_rating = run_query(query_rating)
print("\nTop Genres by Average Rating:")
print(df_rating)

plt.figure(figsize=(10,5))
plt.barh(df_pop['genre'], df_pop['avg_popularity'], color='orange')
plt.gca().invert_yaxis()
plt.title("Top 10 Genres by Average Popularity")
plt.xlabel("Average Popularity")
plt.ylabel("Genre")
plt.tight_layout()
plt.show()
