import pandas as pd

sold = pd.read_csv("sold_final.csv", low_memory=False)
listed = pd.read_csv("listed_final.csv", low_memory=False)

sold["CloseDate"] = pd.to_datetime(sold["CloseDate"], errors="coerce")
listed["ListingContractDate"] = pd.to_datetime(listed["ListingContractDate"], errors="coerce")

# fetching mortage data from FRED

url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE30US"
mortgage = pd.read_csv(url, parse_dates=["observation_date"])
mortgage.columns = ["date", "rate_30yr_fixed"]
print(f"Fetched {len(mortgage)} weekly mortgage rate observations")

# resampling 

mortgage["year_month"] = mortgage["date"].dt.to_period("M")
mortgage_monthly = (
    mortgage.groupby("year_month")["rate_30yr_fixed"]
    .mean()
    .reset_index()
)
print(f"Monthly rates: {len(mortgage_monthly)} months")

# creating the year_month columns/keys on the mls datasets


sold["year_month"] = pd.to_datetime(sold["CloseDate"]).dt.to_period("M")
listed["year_month"] = pd.to_datetime(listed["ListingContractDate"]).dt.to_period("M")

# merging

sold = sold.merge(mortgage_monthly, on="year_month", how="left")
listed = listed.merge(mortgage_monthly, on="year_month", how="left")

# validating - finding nulls
sold_nulls = sold["rate_30yr_fixed"].isnull().sum()
listed_nulls = listed["rate_30yr_fixed"].isnull().sum()
print(f"\nSold — unmatched rates (null): {sold_nulls}")
print(f"Listed — unmatched rates (null): {listed_nulls}")

print("\nSold preview:")
print(sold[["CloseDate", "year_month", "ClosePrice", "rate_30yr_fixed"]].head(10).to_string())

print("\nListed preview:")
print(listed[["ListingContractDate", "year_month", "ListPrice", "rate_30yr_fixed"]].head(10).to_string())

# saving enriched datasets

sold.to_csv("sold_enriched.csv", index=False)
listed.to_csv("listed_enriched.csv", index=False)

print(f"\nSold enriched shape: {sold.shape}")
print(f"Listed enriched shape: {listed.shape}")
