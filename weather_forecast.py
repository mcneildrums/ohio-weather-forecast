#!/usr/bin/env python3
# Pulls weighted Ohio 15-day mean temps and writes a CSV with:
# date, weighted_avg_f, day_over_day_change_f, normal_10yr_f, normal_30yr_f

import requests
import pathlib
import csv
from collections import defaultdict
from datetime import datetime

DAYS = 15

# Ohio airports (lat/lon) and weights
AIRPORTS = {
    "Cleveland":  {"lat": 41.4117, "lon": -81.8498, "weight": 0.54},  # CLE
    "Akron":      {"lat": 40.9163, "lon": -81.4422, "weight": 0.32},  # CAK
    "Youngstown": {"lat": 41.2607, "lon": -80.6791, "weight": 0.13},  # YNG
    "Columbus":   {"lat": 39.9980, "lon": -82.8919, "weight": 0.01},  # CMH
}

FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"
CLIMATE_BASE  = "https://climate-api.open-meteo.com/v1/climate"

# Choose 30-year and 10-year windows (inclusive)
NORMALS_30Y = (1991, 2020)   # WMO standard normals
NORMALS_10Y = (2015, 2024)   # last 10 years

def _http_get_json(url: str) -> dict:
    r = requests.get(url, timeout=40)
    r.raise_for_status()
    return r.json()

def fetch_mean_temps(lat: float, lon: float, days: int = DAYS):
    """Fetch 15-day average temperatures (°F) from Open-Meteo forecast API."""
    url = (
        f"{FORECAST_BASE}"
        f"?latitude={lat}&longitude={lon}"
        "&daily=temperature_2m_mean"
        "&temperature_unit=fahrenheit"
        "&timezone=America%2FNew_York"
        f"&forecast_days={days}"
    )
    j = _http_get_json(url)
    return j["daily"]["time"], j["daily"]["temperature_2m_mean"]

def fetch_normals_doy_map(lat: float, lon: float, start_year: int, end_year: int):
    """
    Fetch daily mean temps over a year range and collapse to a
    month-day (MM-DD) -> climatological average (°F) map.
    """
    url = (
        f"{CLIMATE_BASE}"
        f"?latitude={lat}&longitude={lon}"
        "&daily=temperature_2m_mean"
        "&temperature_unit=fahrenheit"
        f"&start_year={start_year}&end_year={end_year}"
        "&timezone=UTC"
    )
    try:
        j = _http_get_json(url)
        times = j.get("daily", {}).get("time")
        vals  = j.get("daily", {}).get("temperature_2m_mean")
        if not times or not vals or len(times) != len(vals):
            return {}
        buckets = defaultdict(list)
        for t, v in zip(times, vals):
            try:
                md = t[5:]  # 'MM-DD'
                if v is not None:
                    buckets[md].append(float(v))
            except Exception:
                continue
        out = {}
        for md, arr in buckets.items():
            if arr:
                out[md] = round(sum(arr) / len(arr), 1)
        return out
    except Exception:
        return {}

def main():
    # 1) Forecast for each airport
    city_series = {}
    dates_ref = None
    for city, meta in AIRPORTS.items():
        dates, means = fetch_mean_temps(meta["lat"], meta["lon"], DAYS)
        city_series[city] = means
        if dates_ref is None:
            dates_ref = dates

    # 2) Weighted forecast
    weighted = []
    for i in range(DAYS):
        v = sum(city_series[city][i] * meta["weight"] for city, meta in AIRPORTS.items())
        weighted.append(round(v, 1))

    # 3) Day-over-day change (new column right next to weighted values)
    dod_change = [""]
    for i in range(1, DAYS):
        dod_change.append(round(weighted[i] - weighted[i - 1], 1))

    # 4) Normals per airport (10y & 30y) and weighted normals
    normals_10_by_city = {}
    normals_30_by_city = {}
    for city, meta in AIRPORTS.items():
        normals_10_by_city[city] = fetch_normals_doy_map(meta["lat"], meta["lon"], *NORMALS_10Y)
        normals_30_by_city[city] = fetch_normals_doy_map(meta["lat"], meta["lon"], *NORMALS_30Y)

    normal_10yr = []
    normal_30yr = []
    for d in dates_ref:
        md = d[5:]  # 'MM-DD'
        # Weighted 10y
        total10, wsum10 = 0.0, 0.0
        for city, meta in AIRPORTS.items():
            val = normals_10_by_city.get(city, {}).get(md)
            if val is not None:
                total10 += val * meta["weight"]
                wsum10  += meta["weight"]
        normal_10yr.append(round(total10, 1) if wsum10 > 0 else "")

        # Weighted 30y
        total30, wsum30 = 0.0, 0.0
        for city, meta in AIRPORTS.items():
            val = normals_30_by_city.get(city, {}).get(md)
            if val is not None:
                total30 += val * meta["weight"]
                wsum30  += meta["weight"]
        normal_30yr.append(round(total30, 1) if wsum30 > 0 else "")

    # 5) Output CSV
    pathlib.Path("data").mkdir(parents=True, exist_ok=True)
    out_path = f"data/weighted_ohio_forecast_{dates_ref[0]}.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "date",
            "weighted_avg_f",
            "day_over_day_change_f",
            "normal_10yr_f",
            "normal_30yr_f",
        ])
        for i in range(DAYS):
            w.writerow([dates_ref[i], weighted[i], dod_change[i], normal_10yr[i], normal_30yr[i]])

    print(",".join(str(x) for x in weighted))
    print(f"Saved: {out_path}")

if __name__ == "__main__":
    main()
