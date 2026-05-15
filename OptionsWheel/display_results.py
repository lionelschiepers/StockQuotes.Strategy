import pandas as pd
import json


def display():
    with open("put_results.json") as f:
        data = json.load(f)

    results = pd.DataFrame(data["results"])
    if results.empty:
        print("No results found.")
        return

    results["StatusOrder"] = results["Status"].apply(lambda x: 0 if x == "PASS" else 1)
    results = results.sort_values(
        ["StatusOrder", "Score", "MonthlyYieldPct"], ascending=[True, False, False]
    )

    cols = [
        "Symbol",
        "Name",
        "Status",
        "Price",
        "Strike",
        "Expiration",
        "DTE",
        "Premium",
        "MonthlyYieldPct",
        "AnnualizedYieldPct",
        "OTMPct",
        "Delta",
        "ImpliedVolatility",
        "OpenInterest",
        "Volume",
        "SpreadPct",
        "Score",
        "Failed Criterion",
    ]
    print(results[cols].to_string(index=False))


if __name__ == "__main__":
    display()
