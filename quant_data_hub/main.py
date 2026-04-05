from quant_data_hub.sources.fred import FredFetcher

fetcher = FredFetcher()
sofr = fetcher.get_data(start_date="2024-01-01", end_date="2024-01-10")
print(sofr.head())