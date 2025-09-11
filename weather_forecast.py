#!/usr/bin/env python3
# Pulls weighted Ohio 15-day mean temps and writes:
# - CSV: data/weighted_ohio_forecast_<first_date>.csv
# - email_body.html: HTML table (with conditional formatting) for embedding into email body

import requests
import pathlib
import csv
from collections import defaultdict
from datetime import datetime
import html
import pandas as pd

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

# Threshold for highlighting day-over-day change (°F)
DOD_HIGHLIGHT = 2.0

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
    """Return {'MM-DD': climatological mean °F} using Open-Meteo climate API."""
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
        return {md: round(sum(arr) / len(arr), 1) for md, arr in buckets.items() if arr}
    except Exception:
        return {}

def build_html_table(df: pd.DataFrame) -> str:
    """Return an HTML table with conditional formatting on the DoD change column."""
    # Inline CSS tuned for email clients
    style = """
    <style>
      body { font-family: Arial, Helvetica, sans-serif; color:#111; }
      h2 { margin: 0 0 8px 0; font-size: 16px; }
      .small { color:#666; font-size: 12px; margin-bottom:10px; }
      table { border-collapse: collapse; width: 100%; max-width: 860px; }
      th, td { border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; text-align: right; }
      th { background:#f4f6f8; text-align: center; }
      td:first-child, th:first-child { text-align: left; }
      /* Row highlights */
      .warm { background: #ffecec; font-weight: 600; }  /* red-ish for warming */
      .cool { background: #eaf4ff; font-weight: 600; }  /* blue-ish for cooling */
      /* Make DoD column stand out slightly even if not highlighted */
      .dod { font-variant-numeric: tabular-nums; }
      .pos { color:#b30000; }  /* red text for positive change */
      .neg { color:#0054b3; }  /* blue text for negative change */
      .zero { color:#555; }
    </style>
    """

    # Build table manually so we can control per-row classes
    cols = ["date", "weighted_avg_f", "day_over_day_change_f", "normal_10yr_f", "normal_30yr_f"]
    headers = ["Date", "Weighted Avg (°F)", "DoD Change (°F)", "Normal 10y (°F)", "Normal 30y (°F)"]

    def fmt(val):
        if val == "" or pd.isna(val):
            return ""
        if isinstance(val, float):
            return f"{val:.1f}"
        return html.escape(str(val))

    lines = []
    lines.append("<table>")
    # header
    lines.append("<thead><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead>")
    lines.append("<tbody>")

    for _, r in df.iterrows():
        dod = r.get("day_over_day_change_f", "")
        cls = ""
        # determine highlight class and signed class for the DoD cell
        dod_txt = ""
        dod_cell_cls = "dod"
        if dod == "" or pd.isna(dod):
            dod_txt = ""
            sign_cls = "zero"
        else:
            try:
                val = float(dod)
                # Bold+color rows if abs change >= threshold
                if val >= DOD_HIGHLIGHT:
                    cls = "warm"
                elif val <= -DOD_HIGHLIGHT:
                    cls = "cool"
                # add +/- sign and color on the DoD cell
                sign = "+" if val > 0 else ("" if val == 0 else "–")
                pretty = f"{sign}{abs(val):.1f}"
                sign_cls = "pos" if val > 0 else ("neg" if val < 0 else "zero")
                dod_txt = pretty
            except Exception:
                sign_cls = "zero"
                dod_txt = html.escape(str(dod))

        tds = []
        tds.append(f"<td>{fmt(r.get('date'))}</td>")
        tds.append(f"<td>{fmt(r.get('weighted_avg_f'))}</td>")
        tds.append(f"<td class='{dod_cell_cls} {sign_cls}'>{dod_txt}</td>")
        tds.append(f"<td>{fmt(r.get('normal_10yr_f'))}</td>")
        tds.append(f"<td>{fmt(r.get('normal_30yr_f'))}</td>")

        tr_open = f"<tr class='{cls}'>" if cls else "<tr>"
        lines.append(tr_open + "".join(tds) + "</tr>")

    lines.append("</tbody></table>")

    heading = "<h2>Ohio Weighted 15-Day Forecast</h2>"
    sub = f"<div class='small'>Generated {datetime.now().strftime('%Y-%m-%d %H:%M %Z')}</div>"

    return f"<!DOCTYPE html><html><head><meta charset='utf-8'>{style}</head><body>{heading}{sub}{''.join(lines)}<p class='small'>(CSV attached.)</p></body></html>"

def main():
    # 1) Forecast per airport
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

    # 3) Day-over-day change
    dod_change = [""]
    for i in range(1, DAYS):
        dod_change.append(round(weighted[i] - weighted[i - 1], 1))

    # 4) Normals (10y & 30y) and weighted normals
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

    # 6) Build HTML (with conditional formatting) for the email body
    df = pd.read_csv(out_path)
    html_doc = build_html_table(df)
    with open("email_body.html", "w", encoding="utf-8") as f:
        f.write(html_doc)

    # Logs
    print(",".join(str(x) for x in weighted))
    print(f"Saved: {out_path}")

if __name__ == "__main__":
    main()
