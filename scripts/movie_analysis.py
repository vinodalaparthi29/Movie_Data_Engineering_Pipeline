import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the database
conn = sqlite3.connect("../data/movies.db")

# Read movies data into a DataFrame
query = "SELECT title, release_year, budget, revenue, profit, runtime, director_text FROM movies"
df = pd.read_sql_query(query, conn)

# Close connection
conn.close()

# Basic data overview
print("Data Loaded Successfully")
print(df.head())

# Convert datatypes if necessary
df["budget"] = pd.to_numeric(df["budget"], errors="coerce")
df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
df["profit"] = pd.to_numeric(df["profit"], errors="coerce")
df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

# Drop missing data for safety
df = df.dropna(subset=["budget", "revenue", "profit", "release_year"])

# 🎯 1. Budget vs Revenue trend
plt.figure(figsize=(8, 5))
plt.scatter(df["budget"], df["revenue"], alpha=0.5, color="blue")
plt.title("Movie Budget vs Revenue")
plt.xlabel("Budget")
plt.ylabel("Revenue")
plt.grid(True)
plt.show()

# 🎯 2. Profit trend over years
profit_by_year = df.groupby("release_year")["profit"].mean().reset_index()
plt.figure(figsize=(8, 5))
plt.plot(profit_by_year["release_year"], profit_by_year["profit"], color="green", marker="o")
plt.title("Average Movie Profit Over Years")
plt.xlabel("Year")
plt.ylabel("Average Profit")
plt.grid(True)
plt.show()

# 🎯 3. Top 10 directors by average profit
director_profit = df.groupby("director_text")["profit"].mean().sort_values(ascending=False).head(10)
plt.figure(figsize=(9, 5))
director_profit.plot(kind="bar", color="orange")
plt.title("Top 10 Directors by Average Profit")
plt.ylabel("Average Profit")
plt.xlabel("Director")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()
