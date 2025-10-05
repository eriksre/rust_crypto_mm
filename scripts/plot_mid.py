#!/usr/bin/env python3
import sys
import csv
import math
import matplotlib.pyplot as plt

def main():
    if len(sys.argv) < 2:
        print("Usage: plot_mid.py <all_exchanges.csv>")
        sys.exit(1)

    path = '/Users/eriksreinfelds/Documents/GitHub/rust_test/all_exchanges.csv'
    t0 = None
    series = {}

    with open(path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ts = int(row['ts_ns'])
                ex = row['exchange']
                feed = row['feed']
                price = float(row['price'])
            except Exception:
                continue
            if t0 is None:
                t0 = ts
            t_sec = (ts - t0) / 1e9
            key = f"{ex}:{feed}"
            if key not in series:
                series[key] = {'t': [], 'y': []}
            series[key]['t'].append(t_sec)
            series[key]['y'].append(price)

    plt.figure(figsize=(12,7))
    # Color per exchange, marker per feed
    color_map = {
        'bybit': 'tab:blue',
        'binance': 'tab:orange',
        'gate': 'tab:green',
        'bitget': 'tab:red',
    }
    marker_map = {
        'bbo': 'o',
        'orderbook': 'x',
        'trade': '.',
    }
    for key, data in series.items():
        ex, feed = key.split(':', 1)
        c = color_map.get(ex, 'black')
        m = marker_map.get(feed, '.')
        plt.scatter(data['t'], data['y'], s=6, c=c, marker=m, label=key, alpha=0.7)
    plt.xlabel('Time (s)')
    plt.ylabel('Price')
    plt.title('Mids (BBO vs Orderbook) and Trade Prices across Exchanges')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
