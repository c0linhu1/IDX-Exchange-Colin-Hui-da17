import pandas as pd
import glob as gl

files = sorted(gl.glob("/Users/colin/Downloads/IDX Exchange/raw/CRMLSSold*.csv"))
print(f"Loading {len(files)} sold files...")

df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True)
print(f"After concatenation: {df.shape[0]} rows")

# SAFETY CLEANING

date_cols = ["CloseDate", "ListingContractDate", "PurchaseContractDate"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

numeric_cols = ["ClosePrice", "ListPrice", "LivingArea", "LotSizeSquareFeet"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

df = df.drop_duplicates(subset=["ListingKey"], keep="last")
print(f"After dedup: {df.shape[0]} rows")

# RESIDENTIAL FILTER
df = df[df["PropertyType"] == "Residential"]
print(f"After Residential filter: {df.shape[0]} rows")

# FEATURE ENGINEERING

df["price_per_sqft"] = df["ClosePrice"] / df["LivingArea"]
df["days_to_close"] = (df["CloseDate"] - df["ListingContractDate"]).dt.days
df["year"] = df["CloseDate"].dt.year
df["month"] = df["CloseDate"].dt.month
df["property_age"] = df["year"] - df["YearBuilt"]

output_file = "sold_final.csv"
df.to_csv(output_file, index=False)
print(f"Final shape: {df.shape}")
print(f"Saved to {output_file}")