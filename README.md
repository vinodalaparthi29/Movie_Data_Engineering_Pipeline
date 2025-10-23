# Movie Data Engineering Pipeline

This project is part of our **Fundamentals of Data Engineering** course.  
We built a complete data pipeline that extracts, cleans, loads, and analyzes movie data using Python and SQL.

---

## Project Overview

Our goal is to analyze movie performance metrics such as **budget, revenue, and profit trends** using real-world TMDB datasets.  
We aim to understand what factors contribute to a movie's success.

---

## Tech Stack

- **Python** – Data processing and analysis  
- **Pandas** – Cleaning and transforming data  
- **SQLAlchemy + SQLite** – Database storage and querying  
- **Matplotlib / Seaborn** – Data visualization  
- **Git + GitHub** – Version control and collaboration  

---

## Pipeline Stages

1. **Data Ingestion** → Reading raw TMDB CSVs  
2. **Data Cleaning** → Removing duplicates, fixing missing values, formatting dates  
3. **Data Loading** → Storing structured data into SQLite database  
4. **Data Analysis** → Generating insights and visualizations from the cleaned data  

---

## Current Insights

Here are a few early insights we generated:
- Most profitable movies tend to have **mid-range budgets (50–150M USD)**.  
- Profitability doesn’t always correlate directly with revenue — **low-budget hits** are common.  
- The number of successful movies peaked between **2014–2018** in our dataset.

---

## Example Visuals

*(Screenshots or plots can be added later once you generate them — for now, you can mention this section)*

---

##  Team Members

- Vinod Alaparthi  
- R. Akshaya
- Ch, Harshitha

---

## Folder Structure

Movie_Data_Engineering_Pipeline/
│
├── scripts/
│ ├── movie_cleaning.py
│ ├── movie_load.py
│ ├── movie_analysis.py
│
├── data/
│ ├── tmdb_5000_movies.csv
│ ├── tmdb_5000_credits.csv
│
├── requirements.txt
├── .gitignore
├── README.md
└── Movie_Data_Engineering_Pipeline_Project.pptx


## How to Run

```bash
# 1. Create virtual environment (optional but recommended)
python -m venv venv
venv\Scripts\activate   # on Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline
python scripts/movie_cleaning.py
python scripts/movie_load.py
python scripts/movie_analysis.py


### Step 6.2 — Commit and push updated README

Run these commands:

```bash
git add README.md
git commit -m "Updated README with detailed project description"
git push
