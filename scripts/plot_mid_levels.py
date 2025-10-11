#!/usr/bin/env python3
import sys
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

def main():
    path = '/Users/eriksreinfelds/Documents/GitHub/rust_test/all_exchanges.csv'
    csv_path = Path(path).expanduser()
    if not csv_path.exists():
        print(f"Error: file not found: {csv_path}")
        sys.exit(1)

    cols_required = [
        'ts_ns',
        'exchange',
        'feed',
        'price',
        'bid_px_1',
        'bid_qty_1',
        'ask_px_1',
        'ask_qty_1',
    ]
    cols_optional = [
        'direction',
        'bid_px_2',
        'bid_qty_2',
        'bid_px_3',
        'bid_qty_3',
        'ask_px_2',
        'ask_qty_2',
        'ask_px_3',
        'ask_qty_3',
    ]

    try:
        df = pd.read_csv(csv_path, usecols=cols_required + cols_optional, low_memory=False)
    except ValueError:
        df = pd.read_csv(csv_path, low_memory=False)
        missing = [c for c in cols_required if c not in df.columns]
        if missing:
            print(f"Error: missing required columns {missing}. Available: {list(df.columns)}")
            sys.exit(1)

    df = df.dropna(subset=['ts_ns', 'exchange', 'feed'])
    numeric_cols = [c for c in df.columns if c.startswith(('bid_', 'ask_', 'price'))]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['ts_ns'] = pd.to_numeric(df['ts_ns'], errors='coerce')
    df = df.dropna(subset=['ts_ns'])

    df['exchange'] = df['exchange'].astype(str).str.lower()
    df['feed'] = df['feed'].astype(str).str.lower()
    has_direction = 'direction' in df.columns
    if has_direction:
        df['direction'] = df['direction'].astype(str).str.lower().str.strip()

    def mid_from_row(row):
        bid = row.get('bid_px_1')
        ask = row.get('ask_px_1')
        if pd.notna(bid) and pd.notna(ask) and bid != 0 and ask != 0:
            return 0.5 * (bid + ask)
        return row.get('price')

    df['mid'] = df.apply(mid_from_row, axis=1)
    df = df.dropna(subset=['mid'])

    df['dt'] = pd.to_datetime(df['ts_ns'], unit='ns', errors='coerce')
    df = df.dropna(subset=['dt'])

    plt.figure(figsize=(12, 7))
    color_map = {
        'binance': 'tab:orange',
        'bitget': 'tab:red',
        'bybit': 'tab:green',
        'gateio': 'tab:blue',
        'gate': 'tab:blue',
    }
    marker_map = {
        'bbo': 'o',
        'orderbook': 's',
        'trade': '^',
    }

    feed_order = ['bbo', 'orderbook', 'trade']
    exchanges = sorted(df['exchange'].unique())
    for ex in exchanges:
        ex_df = df[df['exchange'] == ex]
        for feed in feed_order:
            sub = ex_df[ex_df['feed'] == feed]
            if sub.empty:
                continue
            c = color_map.get(ex, 'black')
            m = marker_map.get(feed, '.')
            display_ex = {'gate': 'Gateio', 'gateio': 'Gateio'}.get(ex, ex.title())
            label = f"{display_ex} {feed.upper()}"
            if feed == 'trade' and has_direction:
                buy = sub[sub.get('direction') == 'buy']
                sell = sub[sub.get('direction') == 'sell']
                rest = sub[~sub.get('direction').isin(['buy', 'sell'])]
                if not buy.empty:
                    plt.scatter(buy['dt'], buy['mid'], s=12, c=c, marker='^', label=label, alpha=0.7)
                if not sell.empty:
                    plt.scatter(sell['dt'], sell['mid'], s=12, c=c, marker='v', label='_nolegend_', alpha=0.7)
                if not rest.empty:
                    plt.scatter(rest['dt'], rest['mid'], s=12, c=c, marker=m, label='_nolegend_', alpha=0.7)
            else:
                plt.scatter(sub['dt'], sub['mid'], s=12, c=c, marker=m, label=label, alpha=0.7)

    plt.xticks(rotation=45)
    plt.xlabel('Time')
    plt.ylabel('Mid Price')
    plt.title('Orderbook/BBO Mids and Trade Prices across Exchanges')
    plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
