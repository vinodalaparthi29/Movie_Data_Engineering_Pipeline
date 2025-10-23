import os
import pandas as pd

# Automatically find base project directory (one level above current file)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# File paths
MOVIES_FILE = os.path.join(DATA_DIR, "tmdb_5000_movies.csv")
CREDITS_FILE = os.path.join(DATA_DIR, "tmdb_5000_credits.csv")

def load_data():
    """Reads the movie and credits CSV files into pandas DataFrames."""
    
    print("Current working directory:", os.getcwd())
    print("Looking for files in:", DATA_DIR)

    if not os.path.exists(MOVIES_FILE) or not os.path.exists(CREDITS_FILE):
        raise FileNotFoundError("One or both CSV files are missing in the data folder.")
    
    print("Loading datasets...")
    movies_df = pd.read_csv(MOVIES_FILE)
    credits_df = pd.read_csv(CREDITS_FILE)
    
    print("\nMovies dataset shape:", movies_df.shape)
    print("Credits dataset shape:", credits_df.shape)
    
    print("\n Sample from movies.csv:")
    print(movies_df.head(2))
    
    print("\nSample from credits.csv:")
    print(credits_df.head(2))
    
    return movies_df, credits_df


if __name__ == "__main__":
    movies_df, credits_df = load_data()
