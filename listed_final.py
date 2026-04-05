import pandas as pd
import glob as gl


files = sorted(gl.glob("/Users/colin/Downloads/IDX Exchange/raw/CRMLSListing*.csv"))

print(f"Loading {len(files)} listed files...")

# remove after intial load
df = pd.concat([pd.read_csv(f, low_memory = False) for f in files], ignore_index=True)

print(f"Initial shape: {df.shape}")

# SAFETY CLEANING

# convert dates
date_cols = ["ListingContractDate", "ContractStatusChangeDate"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors = 'coerce')

# convert numeric columns
numeric_cols = ["ListPrice", "LivingArea", "LotSizeSquareFeet"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors = 'coerce')

# drop duplicates
df = df.drop_duplicates(subset = ["ListingKey"], keep = "last")


# FEATURE ENGINEERING

# price per sqft
df["price_per_sqft"] = df["ListPrice"] / df["LivingArea"]


# Month + Year
df["year"] = df["ListingContractDate"].dt.year
df["month"] = df["ListingContractDate"].dt.month

# property age
df["property_age"] = df["year"] - df["YearBuilt"]


# save dataset

output_file = "listed_final.csv"
df.to_csv(output_file, index = False)

print(f"Final shape: {df.shape}")
print(f"Saved to {output_file}")