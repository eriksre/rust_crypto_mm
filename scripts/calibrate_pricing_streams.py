#!/usr/bin/env python3
"""Estimate per-stream pricing diagnostics from market activity CSV logs.

This script is for diagnostics and sanity checks only. Runtime pricing now learns
stream noise and adaptive baselines online; these outputs are not intended to be
copied into static YAML config.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from pathlib import Path
from statistics import median

ALLOWED_FEEDS = {"orderbook", "bbo", "trade"}


def parse_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    return value


def parse_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def median_abs_dev(values: list[float], center: float) -> float:
    deviations = [abs(v - center) for v in values]
    return median(deviations)


def format_map(name: str, values: dict[str, float], float_fmt: str) -> str:
    lines = [f"{name}:"]
    if not values:
        lines.append("      {}")
        return "\n".join(lines)
    for key in sorted(values):
        lines.append(f"      {key}: {float_fmt.format(values[key])}")
    return "\n".join(lines)


def main() -> None:
    max_size = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max_size)
            break
        except OverflowError:
            max_size //= 10
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("logs/lighter_activity.csv"),
        help="Input activity CSV path",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=200,
        help="Minimum sample count required per metric per stream",
    )
    args = parser.parse_args()

    if args.min_samples <= 0:
        raise SystemExit("--min-samples must be > 0")
    if not args.input.exists():
        raise SystemExit(f"input file does not exist: {args.input}")

    prev_price: dict[str, float] = {}
    log_returns: dict[str, list[float]] = defaultdict(list)
    latencies_us: dict[str, list[float]] = defaultdict(list)
    spreads_bps: dict[str, list[float]] = defaultdict(list)
    top_ratios: dict[str, list[float]] = defaultdict(list)

    with args.input.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("event_type", "").strip().lower() != "market":
                continue
            exchange = row.get("exchange", "").strip().lower()
            feed = row.get("feed", "").strip().lower()
            if not exchange or feed not in ALLOWED_FEEDS:
                continue
            stream = f"{exchange}:{feed}"

            price = parse_float(row.get("price"))
            if price is None or price <= 0.0:
                continue

            prev = prev_price.get(stream)
            if prev is not None and prev > 0.0:
                ret = math.log(price / prev)
                if math.isfinite(ret):
                    log_returns[stream].append(ret)
            prev_price[stream] = price

            ts_ns = parse_int(row.get("ts_ns"))
            engine_ns = parse_int(row.get("source_engine_ts_ns"))
            if ts_ns is not None and engine_ns is not None:
                latency = (ts_ns - engine_ns) / 1_000.0
                if math.isfinite(latency):
                    latencies_us[stream].append(latency)

            bid_px = parse_float(row.get("bid_px_1"))
            ask_px = parse_float(row.get("ask_px_1"))
            if (
                bid_px is not None
                and ask_px is not None
                and bid_px > 0.0
                and ask_px > 0.0
                and ask_px >= bid_px
                and price > 0.0
            ):
                spread = ((ask_px - bid_px) / price) * 10_000.0
                if math.isfinite(spread):
                    spreads_bps[stream].append(spread)

            bid_sz_1 = parse_float(row.get("bid_sz_1")) or 0.0
            ask_sz_1 = parse_float(row.get("ask_sz_1")) or 0.0
            bid_depth = parse_float(row.get("bid_depth")) or 0.0
            ask_depth = parse_float(row.get("ask_depth")) or 0.0
            depth = bid_depth + ask_depth
            top = bid_sz_1 + ask_sz_1
            if depth > 0.0 and top >= 0.0:
                ratio = top / depth
                if math.isfinite(ratio):
                    top_ratios[stream].append(ratio)

    r_by_stream: dict[str, float] = {}
    latency_median_us: dict[str, float] = {}
    latency_mad_us: dict[str, float] = {}
    spread_median_bps: dict[str, float] = {}
    spread_mad_bps: dict[str, float] = {}
    top_ratio_median: dict[str, float] = {}
    top_ratio_mad: dict[str, float] = {}

    for stream, values in log_returns.items():
        if len(values) < args.min_samples:
            continue
        center = median(values)
        mad = median_abs_dev(values, center)
        sigma = 1.4826 * mad
        variance = max(sigma * sigma, 1e-10)
        r_by_stream[stream] = variance

    for stream, values in latencies_us.items():
        if len(values) < args.min_samples:
            continue
        center = median(values)
        mad = max(median_abs_dev(values, center), 1e-6)
        latency_median_us[stream] = center
        latency_mad_us[stream] = mad

    for stream, values in spreads_bps.items():
        if len(values) < args.min_samples:
            continue
        center = median(values)
        mad = max(median_abs_dev(values, center), 1e-6)
        spread_median_bps[stream] = center
        spread_mad_bps[stream] = mad

    for stream, values in top_ratios.items():
        if len(values) < args.min_samples:
            continue
        center = median(values)
        mad = max(median_abs_dev(values, center), 1e-9)
        top_ratio_median[stream] = center
        top_ratio_mad[stream] = mad

    print(f"# calibration_source: {args.input}")
    print(f"# min_samples: {args.min_samples}")
    print("diagnostics:")
    print(format_map("    r_by_stream_estimate", r_by_stream, "{:.12g}"))
    print(format_map("    latency_median_us", latency_median_us, "{:.12g}"))
    print(format_map("    latency_mad_us", latency_mad_us, "{:.12g}"))
    print(format_map("    spread_median_bps", spread_median_bps, "{:.12g}"))
    print(format_map("    spread_mad_bps", spread_mad_bps, "{:.12g}"))
    print(format_map("    top_ratio_median", top_ratio_median, "{:.6f}"))
    print(format_map("    top_ratio_mad", top_ratio_mad, "{:.12g}"))


if __name__ == "__main__":
    main()
