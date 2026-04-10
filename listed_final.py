import pandas as pd
import glob as gl

files = sorted(gl.glob("/Users/colin/Downloads/IDX Exchange/raw/CRMLSListing*.csv"))
print(f"Loading {len(files)} listed files...")

df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True)
print(f"After concatenation: {df.shape[0]} rows")

# SAFETY CLEANING

date_cols = ["ListingContractDate", "ContractStatusChangeDate"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

numeric_cols = ["ListPrice", "LivingArea", "LotSizeSquareFeet"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

df = df.drop_duplicates(subset=["ListingKey"], keep="last")
print(f"After dedup: {df.shape[0]} rows")

# RESIDENTIAL FILTER
df = df[df["PropertyType"] == "Residential"]
print(f"After Residential filter: {df.shape[0]} rows")

# FEATURE ENGINEERING

df["price_per_sqft"] = df["ListPrice"] / df["LivingArea"]
df["year"] = df["ListingContractDate"].dt.year
df["month"] = df["ListingContractDate"].dt.month
df["property_age"] = df["year"] - df["YearBuilt"]

output_file = "listed_final.csv"
df.to_csv(output_file, index=False)
print(f"Final shape: {df.shape}")
print(f"Saved to {output_file}")