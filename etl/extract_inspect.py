import pandas as pd
import numpy as np
import os
import json

# -----------------------------
# Helper function to standardize column names
# -----------------------------
def clean_columns(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

# -----------------------------
# Paths
# -----------------------------
base_data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
processed_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "processed"))
os.makedirs(processed_path, exist_ok=True)

# -----------------------------
# Load raw datasets
# -----------------------------
datasets = {
    "drivers": "drivers.csv",
    "races": "races.csv",
    "constructors": "constructors.csv",
    "results": "results.csv"
}

dfs = {}
for name, file in datasets.items():
    path = os.path.join(base_data_path, file)
    dfs[name] = pd.read_csv(path, na_values="\\N")
    dfs[name] = clean_columns(dfs[name])

# -----------------------------
# Data cleaning
# -----------------------------
# Drivers
dfs["drivers"]["driverid"] = pd.to_numeric(dfs["drivers"]["driverid"], errors="coerce")
dfs["drivers"]["number"] = pd.to_numeric(dfs["drivers"]["number"], errors="coerce")
dfs["drivers"]["dob"] = pd.to_datetime(dfs["drivers"]["dob"], errors="coerce")

# Races
for col in ["raceid", "year", "round", "circuitid"]:
    dfs["races"][col] = pd.to_numeric(dfs["races"][col], errors="coerce")
dfs["races"]["date"] = pd.to_datetime(dfs["races"]["date"], errors="coerce")

# Constructors
dfs["constructors"]["constructorid"] = pd.to_numeric(dfs["constructors"]["constructorid"], errors="coerce")

# Results
numeric_cols = ["resultid","raceid","driverid","constructorid","grid","position","laps","points"]
for col in numeric_cols:
    dfs["results"][col] = pd.to_numeric(dfs["results"][col], errors="coerce")

# -----------------------------
# Save cleaned CSVs
# -----------------------------
for name, df in dfs.items():
    df.to_csv(os.path.join(processed_path, f"{name}_clean.csv"), index=False)
    print(f"{name}_clean.csv saved.")

# -----------------------------
# Generate metadata JSON
# -----------------------------
metadata = {}
for name, df in dfs.items():
    metadata[name] = {
        "columns": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
        "primary_key": None
    }

# Define primary keys manually
metadata["drivers"]["primary_key"] = "driverid"
metadata["races"]["primary_key"] = "raceid"
metadata["constructors"]["primary_key"] = "constructorid"
metadata["results"]["primary_key"] = "resultid"

# Save metadata
metadata_file = os.path.join(processed_path, "metadata.json")
with open(metadata_file, "w") as f:
    json.dump(metadata, f, indent=4)
print(f"Metadata saved at {metadata_file}")
