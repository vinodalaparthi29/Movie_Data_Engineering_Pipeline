import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

# Step 1: Read cleaned CSV
cleaned_file = "cleaned_movies.csv"
df = pd.read_csv(cleaned_file)

# Step 2: Connect to SQLite
engine = create_engine("sqlite:///movie_data.db")

# Step 3: Load DataFrame into database
df.to_sql("movies", con=engine, if_exists="replace", index=False)

# Step 4: Verify
with engine.connect() as connection:
    result = connection.execute(text("SELECT COUNT(*) FROM movies"))
    total = result.scalar_one()   # scalar_one() returns the single value (COUNT)
    print("Total rows in movies table:", total)
