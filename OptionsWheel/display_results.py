import pandas as pd
import json

def display():
    with open('analysis_results.json') as f:
        data = json.load(f)
    
    results = pd.DataFrame(data['results'])
    # Sort: Status 'PASS' (0) before 'NEAR' (1), then by DiffPct ascending
    results['StatusOrder'] = results['Status'].apply(lambda x: 0 if x == 'PASS' else 1)
    results = results.sort_values(['StatusOrder', 'DiffPct'])
    
    cols = ['Symbol', 'Name', 'Status', 'Price', 'EMA50', 'DiffPct', 'ADX', 'RSI', 'RVI', 'MACD', 'Failed Criterion']
    print(results[cols].to_string(index=False))

if __name__ == "__main__":
    display()
