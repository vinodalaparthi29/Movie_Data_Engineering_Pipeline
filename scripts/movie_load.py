# movie_load.py
"""
Phase 5 — Data Loading into SQL (normalized schema)
Creates an SQLite database ../data/movies.db and loads data from ../data/movies_transformed.csv
Normalized tables: movies, genres, movie_genres, actors, movie_cast, directors, movie_directors
"""

import pandas as pd
import ast
from sqlalchemy import text
from sqlalchemy import (create_engine, MetaData, Table, Column, Integer, String,
                        Float, Date, DateTime, ForeignKey, UniqueConstraint, Index)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select
from sqlalchemy.engine import Engine
from datetime import datetime
import os

# -----------------------------
# Config
# -----------------------------
TRANSFORMED_CSV = os.path.join("..", "data", "movies_transformed.csv")
DB_PATH = os.path.join("..", "data", "movies.db")
DB_URL = f"sqlite:///{DB_PATH}"   # swap this to postgresql://user:pass@host/dbname for production

BATCH_SIZE = 500  # commit after this many movies (tune based on dataset size)

# -----------------------------
# Helper parsing functions
# -----------------------------
def try_parse_list(val):
    """Safely parse a Python list-like string; return list or []"""
    if pd.isna(val):
        return []
    if isinstance(val, list):
        return val
    try:
        return ast.literal_eval(val)
    except Exception:
        # Attempt simple split fallback if it's a comma-separated string
        if isinstance(val, str):
            return [i.strip() for i in val.split(',') if i.strip()]
        return []

def parse_cast_to_names(cast_field):
    """
    Accepts different formats:
     - already a list of strings
     - a list of dicts [{'name': 'Actor', ...}, ...]
     - a string repr of list/dicts
    Returns list of actor names (strings) in original order.
    """
    parsed = try_parse_list(cast_field)
    names = []
    for item in parsed:
        if isinstance(item, dict):
            # common TMDB format
            name = item.get('name') or item.get('actor') or item.get('original_name')
            if name:
                names.append(name)
        elif isinstance(item, str):
            names.append(item)
        else:
            # ignore unexpected types
            continue
    return names

# -----------------------------
# Prepare DB schema with SQLAlchemy
# -----------------------------
engine = create_engine(DB_URL, future=True)
metadata = MetaData()

movies = Table(
    "movies", metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String, nullable=False),
    Column("original_title", String),
    Column("overview", String),
    Column("release_date", Date),
    Column("release_year", Integer, index=True),
    Column("release_month", Integer),
    Column("budget", Integer),
    Column("revenue", Integer),
    Column("profit", Integer),
    Column("runtime", Integer),
    Column("director_text", String),  # keep denormalized director for quick queries
    # Add more columns if present in CSV...
    sqlite_autoincrement=True
)

genres = Table(
    "genres", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True)
)

movie_genres = Table(
    "movie_genres", metadata,
    Column("movie_id", Integer, ForeignKey("movies.id", ondelete="CASCADE"), nullable=False),
    Column("genre_id", Integer, ForeignKey("genres.id", ondelete="CASCADE"), nullable=False),
    UniqueConstraint('movie_id', 'genre_id', name='uq_movie_genre')
)

actors = Table(
    "actors", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True)
)

movie_cast = Table(
    "movie_cast", metadata,
    Column("movie_id", Integer, ForeignKey("movies.id", ondelete="CASCADE"), nullable=False),
    Column("actor_id", Integer, ForeignKey("actors.id", ondelete="CASCADE"), nullable=False),
    Column("cast_order", Integer, nullable=True),
    UniqueConstraint('movie_id', 'actor_id', name='uq_movie_actor')
)

directors = Table(
    "directors", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True)
)

movie_directors = Table(
    "movie_directors", metadata,
    Column("movie_id", Integer, ForeignKey("movies.id", ondelete="CASCADE"), nullable=False),
    Column("director_id", Integer, ForeignKey("directors.id", ondelete='CASCADE'), nullable=False),
    UniqueConstraint('movie_id', "director_id", name='uq_movie_director')
)

# Indexes for common queries
Index("ix_movies_release_year", movies.c.release_year)
Index("ix_movie_genre_movie", movie_genres.c.movie_id)
Index("ix_movie_cast_movie", movie_cast.c.movie_id)

# Create tables
metadata.create_all(engine)

# -----------------------------
# Load transformed CSV
# -----------------------------
df = pd.read_csv(TRANSFORMED_CSV)
print("Loaded CSV:", TRANSFORMED_CSV, "shape:", df.shape)

# Ensure id column exists
if 'id' not in df.columns:
    raise SystemExit("CSV must contain 'id' column (TMDB movie id)")

# Convert release_date to proper date objects where possible
def to_date_safe(x):
    try:
        if pd.isna(x):
            return None
        return pd.to_datetime(x).date()
    except Exception:
        return None

df['release_date_parsed'] = df.get('release_date', pd.Series([None]*len(df))).apply(to_date_safe)

# We'll attempt to read these fields (if exist) - otherwise fallback to None / empty
possible_cast_cols = ['main_cast', 'cast_list', 'cast', 'cast_parsed']
possible_genre_cols = ['main_genre', 'genres_list', 'genres', 'genres_parsed']
possible_director_cols = ['director', 'director_text', 'directors']

# -----------------------------
# Insert helpers (caching to avoid repeated DB lookups)
# -----------------------------
from sqlalchemy import insert, select

def get_or_create_scalar(conn, table, col_name, value, cache):
    """
    Get id for unique scalar (like genre name or actor name) or create it.
    cache: dict name->id to avoid DB roundtrips.
    """
    if not value:
        return None
    if value in cache:
        return cache[value]
    s = select(table.c.id).where(table.c.name == value)
    res = conn.execute(s).scalar_one_or_none()
    if res:
        cache[value] = res
        return res
    # insert
    r = conn.execute(insert(table).values(name=value))
    inserted_id = r.inserted_primary_key[0]
    cache[value] = inserted_id
    return inserted_id

# -----------------------------
# Main loading loop (batched with transaction)
# -----------------------------
genre_cache = {}   # name -> id
actor_cache = {}   # name -> id
director_cache = {}  # name -> id

conn = engine.connect()
trans = None
count = 0

try:
    trans = conn.begin()
    for idx, row in df.iterrows():
        movie_id = int(row['id'])
        title = row.get('title') or row.get('original_title') or "Unknown"
        original_title = row.get('original_title')
        overview = row.get('overview')
        release_date = row['release_date_parsed']
        release_year = int(row['release_year']) if not pd.isna(row.get('release_year')) else None
        release_month = int(row['release_month']) if not pd.isna(row.get('release_month')) else None
        budget = int(row['budget']) if not pd.isna(row.get('budget')) else 0
        revenue = int(row['revenue']) if not pd.isna(row.get('revenue')) else 0
        profit = int(row['profit']) if not pd.isna(row.get('profit')) else (revenue - budget)
        runtime = int(row['runtime']) if not pd.isna(row.get('runtime')) else None

        director_text = None
        # read director from possible columns
        for c in possible_director_cols:
            if c in row and pd.notna(row[c]) and row[c]:
                director_text = row[c]
                break

        # Insert or replace movie row
        # Use insert... on conflict replace is DB-specific; here we simply try to delete existing first (idempotent)
        conn.execute(movies.delete().where(movies.c.id == movie_id))  # remove old if any
        conn.execute(
            insert(movies).values(
                id=movie_id,
                title=title,
                original_title=original_title,
                overview=overview,
                release_date=release_date,
                release_year=release_year,
                release_month=release_month,
                budget=budget,
                revenue=revenue,
                profit=profit,
                runtime=runtime,
                director_text=director_text
            )
        )

        # Genres: handle either main_genre (single) or genres_list (list)
        genre_names = []
        for c in possible_genre_cols:
            if c in row and pd.notna(row[c]) and row[c]:
                val = row[c]
                # if it's a list-like string, parse to list
                parsed = try_parse_list(val)
                if isinstance(parsed, list):
                    # list of names or dicts
                    for item in parsed:
                        if isinstance(item, dict):
                            nm = item.get('name')
                            if nm: genre_names.append(nm)
                        elif isinstance(item, str):
                            genre_names.append(item)
                elif isinstance(parsed, str):
                    genre_names.append(parsed)
                break

        for gname in genre_names:
            gid = get_or_create_scalar(conn, genres, 'name', gname, genre_cache)
            if gid:
                # remove previous link if any (idempotency)
                conn.execute(movie_genres.delete().where(movie_genres.c.movie_id == movie_id).where(movie_genres.c.genre_id == gid))
                conn.execute(insert(movie_genres).values(movie_id=movie_id, genre_id=gid))

        # Cast: try to parse main_cast as list of names
        actor_names = []
        for c in possible_cast_cols:
            if c in row and pd.notna(row[c]) and row[c]:
                actor_names = parse_cast_to_names(row[c])
                break

        for pos, aname in enumerate(actor_names):
            aid = get_or_create_scalar(conn, actors, 'name', aname, actor_cache)
            if aid:
                conn.execute(movie_cast.delete().where(movie_cast.c.movie_id == movie_id).where(movie_cast.c.actor_id == aid))
                conn.execute(insert(movie_cast).values(movie_id=movie_id, actor_id=aid, cast_order=pos))

        # Directors: director_text might be single or list
        director_names = []
        if director_text:
            # parse director_text (maybe "Name" or "['Name']", or dict/list repr)
            parsed = try_parse_list(director_text)
            if isinstance(parsed, list):
                # if it's a list of strings or dicts
                for item in parsed:
                    if isinstance(item, dict):
                        nm = item.get('name')
                        if nm: director_names.append(nm)
                    elif isinstance(item, str):
                        director_names.append(item)
            elif isinstance(parsed, str):
                director_names = [parsed]
        # As fallback, if there is a 'director' field that is a string
        if not director_names and 'director' in row and pd.notna(row['director']) and row['director']:
            director_names = [row['director']]

        for dname in director_names:
            did = get_or_create_scalar(conn, directors, 'name', dname, director_cache)
            if did:
                conn.execute(movie_directors.delete().where(movie_directors.c.movie_id == movie_id).where(movie_directors.c.director_id == did))
                conn.execute(insert(movie_directors).values(movie_id=movie_id, director_id=did))

        count += 1
        if count % BATCH_SIZE == 0:
            trans.commit()
            trans = conn.begin()
            print(f"{count} movies processed and committed.")

    # Final commit
    trans.commit()
    print(f"All done — total movies processed: {count}")

except Exception as e:
    if trans is not None:
        trans.rollback()
    print("Error during import:", e)
    raise
finally:
    conn.close()

# -----------------------------
# Quick verification: open DB and run example queries
# -----------------------------
engine = create_engine(DB_URL)
from sqlalchemy import func

# Verify total rows
with engine.connect() as c:
    res = c.execute(text("""
        SELECT g.name, COUNT(mg.movie_id) as movie_count
        FROM genres g
        JOIN movie_genres mg ON g.id = mg.genre_id
        GROUP BY g.name
        ORDER BY movie_count DESC
        LIMIT 10;
    """))

    rows = res.fetchall()
    print("\nTop 10 Genres by Movie Count:")
    for r in rows:
        print(f"{r[0]} : {r[1]}")
