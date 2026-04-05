import pandas as pd
import glob as gl


files = sorted(gl.glob("/Users/colin/Downloads/IDX Exchange/raw/CRMLSSold*.csv"))

print(f"Loading {len(files)} sold files...")

# change after run only add new csv file for each month
df = pd.concat([pd.read_csv(f, low_memory = False) for f in files], 
               ignore_index = True)

print(f"Shape of df: {df.shape}")

# SAFETY CLEANING

# convert dates
date_cols = ["CloseDate", "ListingContractDate", "PurchaseContractDate"]
for col in date_cols:
    if col in df.columns: 
        df[col] = pd.to_datetime(df[col], errors = 'coerce')

# convert numeric columns 
numeric_cols = ["ClosePrice", "ListPrice", "LivingArea", "LotSizeSquareFeet"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Drop duplicates
df = df.drop_duplicates(subset=["ListingKey"], keep = "last")



# FEATURE ENGINEERING

# price per sq foot
df["price_per_sqft"] = df["ClosePrice"] / df["LivingArea"]

# days to close
df["days_to_close"] = (df["CloseDate"] - df["ListingContractDate"]).dt.days

# month + year 
df["year"] = df["CloseDate"].dt.year
df["month"] = df["CloseDate"].dt.month

# Property age
df["property_age"] = df["year"] - df["YearBuilt"]


# save final dataset

output_file = "sold_final.csv"
df.to_csv(output_file, index=False)

print(f"final shape: {df.shape}")
print(f"saved to {output_file}")