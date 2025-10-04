#manual queries

import duckdb
import pandas as pd

con = duckdb.connect('my_f1_database.duckdb')

# Option 1: Directly create table from CSV
con.execute("""
    CREATE TABLE IF NOT EXISTS driver_stats AS
    SELECT * FROM read_csv_auto('processed/ai_transformed_result.csv')
""")

# Option 2: If you want to insert from pandas DataFrame
df = pd.read_csv("processed/ai_transformed_result.csv")
con.register('df_view', df)
con.execute("INSERT INTO driver_stats SELECT * FROM df_view")

# Verify data
result = con.execute("SELECT year, surname, total_points FROM driver_stats ORDER BY total_points DESC LIMIT 10").fetchdf()
print(result)
