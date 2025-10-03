import requests
import pandas as pd

# 1. Define the API endpoint
# For Test: Metropolitan Police stop and search for Jan 2025
url = "https://data.police.uk/api/stops-force?force=metropolitan&date=2025-01"

response = requests.get(url)
data = response.json()

# Convert to pandas DataFrame

df = pd.DataFrame(data)

print("Shape of dataframe:", df.shape)
print("First few rows:")
print(df.head())

# Save locally as CSV (for testing)
df.to_csv("met_stop_search_jan2025.csv", index=False)
print("Saved to met_stop_search_jan2025.csv")