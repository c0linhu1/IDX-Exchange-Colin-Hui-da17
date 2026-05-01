import pandas as pd
import numpy as np

df = pd.read_csv("sold_enriched.csv", low_memory=False)
rows_start = len(df)
print(f"Rows loaded: {rows_start}, Columns: {df.shape[1]}")

# convert date fields to datetime, coercing bad values to NaT
date_cols = [
    "CloseDate",
    "PurchaseContractDate",
    "ListingContractDate",
    "ContractStatusChangeDate",
]
for col in date_cols:
    if col in df.columns:
        before_nulls = df[col].isnull().sum()
        df[col] = pd.to_datetime(df[col], errors="coerce")
        new_nats = df[col].isnull().sum() - before_nulls
        print(f"{col}: {new_nats} new NaTs from coercion")

# enforce numeric types on key fields
numeric_cols = [
    "ClosePrice", "ListPrice", "OriginalListPrice",
    "LivingArea", "LotSizeAcres", "LotSizeSquareFeet",
    "BedroomsTotal", "BathroomsTotalInteger",
    "DaysOnMarket", "YearBuilt",
    "TaxAnnualAmount", "AssociationFee",
    "GarageSpaces", "ParkingTotal", "Stories",
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# drop columns that are >90% null, unless they are core analysis fields
core_fields = [
    "ClosePrice", "ListPrice", "OriginalListPrice", "LivingArea", "CloseDate",
    "ListingContractDate", "PurchaseContractDate", "ContractStatusChangeDate",
    "CountyOrParish", "City", "PostalCode", "PropertySubType",
    "BedroomsTotal", "BathroomsTotalInteger", "DaysOnMarket",
    "YearBuilt", "Latitude", "Longitude",
]
missing_pct = df.isnull().mean() * 100
high_null_cols = missing_pct[missing_pct > 90].index.tolist()
cols_to_drop = [c for c in high_null_cols if c not in core_fields]
cols_before = df.shape[1]
df.drop(columns=cols_to_drop, inplace=True, errors="ignore")
print(f"Dropped {cols_before - df.shape[1]} high-null columns, {df.shape[1]} remaining")

# flag invalid numeric values
df["flag_invalid_close_price"] = (df["ClosePrice"] <= 0) | df["ClosePrice"].isnull()
df["flag_invalid_living_area"] = (df["LivingArea"] <= 0) | df["LivingArea"].isnull()
df["flag_negative_dom"] = df["DaysOnMarket"] < 0
df["flag_negative_beds_baths"] = (
    (df["BedroomsTotal"] < 0) | (df["BathroomsTotalInteger"] < 0)
)
print(f"Invalid ClosePrice: {df['flag_invalid_close_price'].sum()}")
print(f"Invalid LivingArea: {df['flag_invalid_living_area'].sum()}")
print(f"Negative DaysOnMarket: {df['flag_negative_dom'].sum()}")
print(f"Negative beds/baths: {df['flag_negative_beds_baths'].sum()}")

# remove records where ClosePrice or LivingArea are invalid (these can never be valid)
rows_before_invalid = len(df)
df = df[~df["flag_invalid_close_price"] & ~df["flag_invalid_living_area"]]
print(f"Removed {rows_before_invalid - len(df)} records with invalid price or area")

# date consistency flags - logical order should be listing < purchase < close
df["listing_after_close_flag"] = (
    df["ListingContractDate"].notna() &
    df["CloseDate"].notna() &
    (df["ListingContractDate"] > df["CloseDate"])
)
df["purchase_after_close_flag"] = (
    df["PurchaseContractDate"].notna() &
    df["CloseDate"].notna() &
    (df["PurchaseContractDate"] > df["CloseDate"])
)
df["negative_timeline_flag"] = (
    df["listing_after_close_flag"] |
    df["purchase_after_close_flag"] |
    (
        df["ListingContractDate"].notna() &
        df["PurchaseContractDate"].notna() &
        (df["ListingContractDate"] > df["PurchaseContractDate"])
    )
)
print(f"listing_after_close_flag: {df['listing_after_close_flag'].sum()}")
print(f"purchase_after_close_flag: {df['purchase_after_close_flag'].sum()}")
print(f"negative_timeline_flag: {df['negative_timeline_flag'].sum()}")

# geographic quality flags - California coords are lat 32-42N, lon -114 to -124W
df["flag_missing_coords"] = df["Latitude"].isnull() | df["Longitude"].isnull()
df["flag_zero_coords"] = (df["Latitude"] == 0) | (df["Longitude"] == 0)
df["flag_positive_longitude"] = df["Longitude"] > 0
df["flag_out_of_state_coords"] = (
    df["Latitude"].notna() & df["Longitude"].notna() &
    ~df["flag_missing_coords"] &
    (
        (df["Latitude"] < 32.0) | (df["Latitude"] > 42.5) |
        (df["Longitude"] < -124.5) | (df["Longitude"] > -114.0)
    )
)
geo_flagged = (
    df["flag_missing_coords"] | df["flag_zero_coords"] |
    df["flag_positive_longitude"] | df["flag_out_of_state_coords"]
).sum()
print(f"Records with any geographic flag: {geo_flagged}")

# impute DaysOnMarket from date diff where it is null but dates are available
mask_dom_null = df["DaysOnMarket"].isnull()
mask_dates_available = df["ListingContractDate"].notna() & df["CloseDate"].notna()
df.loc[mask_dom_null & mask_dates_available, "DaysOnMarket"] = (
    (df.loc[mask_dom_null & mask_dates_available, "CloseDate"] -
     df.loc[mask_dom_null & mask_dates_available, "ListingContractDate"]).dt.days
)
print(f"DaysOnMarket imputed for {(mask_dom_null & mask_dates_available).sum()} records")

print(f"\nRows start: {rows_start} | Rows end: {len(df)} | Removed: {rows_start - len(df)}")

df.to_csv("sold_cleaned.csv", index=False)