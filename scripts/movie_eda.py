# movie_eda.py
"""
Phase 4: Data Exploration & Transformation
Author: Alaparthi Vinod

This script performs:
1. Loading cleaned movie dataset
2. Basic exploratory data analysis (EDA)
3. Data transformation and feature engineering
4. Saving transformed dataset
"""

import pandas as pd
import matplotlib.pyplot as plt
import ast

# ----------------------------
# Step 1: Load cleaned dataset
# ----------------------------
df = pd.read_csv("../data/movies_cleaned.csv")

print("Dataset loaded successfully")
print("Number of rows:", df.shape[0])
print("Number of columns:", df.shape[1])
print(df.info())
print(df.head())

# ----------------------------
# Step 2: Basic Exploration
# ----------------------------
# Missing values
print("\nMissing values per column:")
print(df.isnull().sum())

# Quick statistics
print("\nSummary statistics for numeric columns:")
print(df.describe())

# Example: plot budget distribution
plt.figure(figsize=(8, 5))
plt.hist(df['budget'], bins=50, color='skyblue', edgecolor='black')
plt.title("Budget Distribution")
plt.xlabel("Budget")
plt.ylabel("Number of Movies")
plt.show()

# Revenue distribution
plt.figure(figsize=(8, 5))
plt.hist(df['revenue'], bins=50, color='lightgreen', edgecolor='black')
plt.title("Revenue Distribution")
plt.xlabel("Revenue")
plt.ylabel("Number of Movies")
plt.show()

# ----------------------------
# Step 3: Transform nested columns
# ----------------------------
def parse_list_column(column):
    """
    Convert string representation of list to actual list using ast.literal_eval
    """
    return column.apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])

# Parse genres and cast columns
df['genres_list'] = parse_list_column(df['genres'])
df['cast_list'] = parse_list_column(df['cast'])

# Count number of genres per movie
df['num_genres'] = df['genres_list'].apply(len)

# Extract main genre (first genre)
df['main_genre'] = df['genres_list'].apply(lambda x: x[0] if len(x) > 0 else None)

# Extract first 3 actors as main cast
df['main_cast'] = df['cast_list'].apply(lambda x: x[:3] if len(x) > 0 else [])

# Count number of cast members
df['num_cast'] = df['cast_list'].apply(len)

# ----------------------------
# Step 4: Feature Engineering
# ----------------------------
# Profit = revenue - budget
df['profit'] = df['revenue'] - df['budget']

# Convert release_date to datetime and extract year
df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
df['release_year'] = df['release_date'].dt.year

# Optional: Month of release
df['release_month'] = df['release_date'].dt.month

# ----------------------------
# Step 5: Optional cleanup
# ----------------------------
# Drop redundant columns
columns_to_drop = ['genres', 'cast', 'cast_list', 'genres_list']
for col in columns_to_drop:
    if col in df.columns:
        df.drop(col, axis=1, inplace=True)

# ----------------------------
# Step 6: Save transformed dataset
# ----------------------------
df.to_csv("../data/movies_transformed.csv", index=False)
print("\nTransformed dataset saved as movies_transformed.csv")
