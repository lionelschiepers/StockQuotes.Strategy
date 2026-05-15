import pandas as pd
import json
import argparse
import os

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(MODULE_DIR, "..", ".."))
DATA_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "output")


def display(option_type="put"):
    filename = "call_results.json" if option_type == "call" else "put_results.json"
    with open(os.path.join(DATA_OUTPUT_DIR, filename)) as f:
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--type",
        dest="option_type",
        choices=["put", "call"],
        default="put",
        help="Type of results to display: 'put' (default) or 'call'.",
    )
    args = parser.parse_args()
    display(args.option_type)


if __name__ == "__main__":
    main()
