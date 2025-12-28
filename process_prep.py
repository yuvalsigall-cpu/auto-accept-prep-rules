#!/usr/bin/env python3
"""
process_prep.py

Read order-level Excel, compute preparation-time rules:
- buckets by day of week
- hour buckets (06-12, 12-17, 17-23)
- item-count buckets: 1-5, 6-10, 11-15, ... with jump rules (after 25 -> step 10, after 55 -> step 20)
- compute P75 (75th percentile) prep time per bucket
- merge adjacent buckets if P75 diff <= 4 minutes
- round increments (.5 up)
- remove Saturday buckets with total orders < 20 (optional rule)
Outputs CSV with columns:
day_of_week, hour_bucket, units_min, units_max, p75_seconds, base_p75_seconds, increment_seconds, orders_count
"""

import argparse
import math
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import re
import sys

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", required=True, help="Input Excel file (.xlsx)")
    p.add_argument("--out", "-o", required=True, help="Output CSV file")
    p.add_argument("--venue", default=None, help="Optional venue name filter")
    p.add_argument("--lookback_months", type=int, default=6, help="Lookback months if timestamp present")
    return p.parse_args()

# Helper: try to detect relevant columns
def find_column(cols, patterns):
    cols_l = [c.lower() for c in cols]
    for pat in patterns:
        for i,c in enumerate(cols_l):
            if pat in c:
                return list(cols)[i]
    return None

def parse_duration_to_seconds(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float)):
        # assume already seconds if numeric and reasonable
        return float(x)
    s = str(x).strip()
    # handle hh:mm:ss or mm:ss or mm
    if re.match(r'^\d+:\d+:\d+$', s):
        h,m,sec = s.split(':')
        return int(h)*3600 + int(m)*60 + int(sec)
    if re.match(r'^\d+:\d+$', s):
        m,sec = s.split(':')
        return int(m)*60 + int(sec)
    # numeric-looking
    try:
        return float(s)
    except:
        return np.nan

def hour_to_bucket(hour):
    # hour is 0-23
    if 6 <= hour < 12:
        return "06-12"
    if 12 <= hour < 17:
        return "12-17"
    if 17 <= hour < 23:
        return "17-23"
    return "other"

def build_unit_buckets(max_units=200):
    # create sequence: 1-5, 6-10, 11-15, 16-20, 21-30 (10-step after 25),
    # then 31-40, 41-50, 51-70 (20-step after 55) etc
    buckets = []
    # first few fixed by 5
    ranges = [(1,5),(6,10),(11,15),(16,20),(21,25)]
    # after 25 up to 50 use 10 steps
    i=26
    while i <= 55 and i <= max_units:
        hi = min(i+9, max_units)
        buckets.append((i,hi))
        i = hi+1
    # after 55 use 20-step
    while i <= max_units:
        hi = min(i+19, max_units)
        buckets.append((i,hi))
        i = hi+1
    # combine
    full = ranges + buckets
    # ensure unique and sorted
    final = []
    seen = set()
    for (a,b) in full:
        if a> b: continue
        if (a,b) in seen: continue
        final.append((a,b))
        seen.add((a,b))
    return final

def assign_units_bucket(u, buckets):
    if pd.isna(u): return None
    try:
        ui = int(u)
    except:
        try:
            ui = int(float(u))
        except:
            return None
    for (a,b) in buckets:
        if a <= ui <= b:
            return f"{a}-{b}"
    # if bigger than last bucket return last
    a,b = buckets[-1]
    if ui >= a:
        return f"{a}-{b}"
    return None

def round_half_up(n):
    # n is float seconds/minutes? We'll round to nearest integer (seconds or minutes as used)
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return None
    return int(math.floor(n + 0.5))

def merge_adjacent(rows, max_diff_seconds=4*60):
    # rows: list of dicts sorted by units_min
    if not rows:
        return rows
    merged = []
    cur = rows[0].copy()
    for nxt in rows[1:]:
        # if P75 difference <= threshold -> merge ranges
        if abs(cur['p75_seconds'] - nxt['p75_seconds']) <= max_diff_seconds:
            # extend cur
            cur['units_max'] = nxt['units_max']
            cur['orders_count'] += nxt['orders_count']
            # compute weighted p75? We'll recompute average by orders_count weighted (approx)
            # but better keep max of both - keep the higher (conservative)
            cur['p75_seconds'] = max(cur['p75_seconds'], nxt['p75_seconds'])
            # base_p75 keep minimum across merged
            cur['base_p75_seconds'] = min(cur.get('base_p75_seconds', cur['p75_seconds']), nxt.get('base_p75_seconds', nxt['p75_seconds']))
        else:
            merged.append(cur)
            cur = nxt.copy()
    merged.append(cur)
    return merged

def main():
    args = parse_args()
    df = None
    try:
        xls = pd.ExcelFile(args.input)
        # read all sheets and concat
        parts = []
        for s in xls.sheet_names:
            tmp = pd.read_excel(xls, s)
            tmp['_sheet_name'] = s
            parts.append(tmp)
        df = pd.concat(parts, ignore_index=True)
    except Exception as e:
        print("Error reading Excel:", e, file=sys.stderr)
        sys.exit(2)

    if df.shape[0] == 0:
        print("Empty input file", file=sys.stderr)
        sys.exit(2)

    cols = df.columns.tolist()

    # detect columns
    units_col = find_column(cols, ['units','qty','quantity','items','units_sold','unitssold','items_count','item_count','units sold'])
    prep_col = find_column(cols, ['prep','preparation','preptime','time to prepare','time_to_prepare','preparation_time','prep_time','preparation_seconds','duration'])
    ts_col = find_column(cols, ['timestamp','time','order_time','created','order_created','time_created','timeordered','order_time'])
    venue_col = find_column(cols, ['venue','merchant','store','shop','restaurant','branch','location'])

    # optionally filter by venue
    if args.venue and venue_col:
        df = df[df[venue_col].astype(str).str.contains(args.venue, case=False, na=False)]

    # Prepare columns
    if prep_col:
        df['prep_seconds'] = df[prep_col].apply(parse_duration_to_seconds)
    else:
        df['prep_seconds'] = np.nan

    # if there's a pair of timestamps like start/production we don't handle automatically here
    # derive day_of_week and hour if timestamp present
    if ts_col:
        def parse_date(x):
            if pd.isna(x): return pd.NaT
            if isinstance(x, (pd.Timestamp, datetime)): return pd.Timestamp(x)
            s = str(x)
            # try common formats
            for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%d/%m/%Y %H:%M:%S","%d/%m/%Y %H:%M","%Y-%m-%dT%H:%M:%S"):
                try:
                    return pd.to_datetime(s, format=fmt)
                except:
                    pass
            try:
                return pd.to_datetime(s)
            except:
                return pd.NaT
        df['_ts'] = df[ts_col].apply(parse_date)
    else:
        df['_ts'] = pd.NaT

    # day_of_week numeric and name
    df['day_of_week_num'] = df['_ts'].dt.dayofweek  # Monday=0
    df['day_of_week'] = df['_ts'].dt.day_name()
    # fallback if no timestamp: allow user to have day column
    day_col = find_column(cols, ['day','weekday','day_of_week','dayname'])
    if df['day_of_week'].isna().all() and day_col:
        df['day_of_week'] = df[day_col].astype(str)

    # hour
    df['hour'] = df['_ts'].dt.hour
    # fallback hour col
    hour_col = find_column(cols, ['hour','time_hour'])
    if df['hour'].isna().all() and hour_col:
        df['hour'] = pd.to_numeric(df[hour_col], errors='coerce')

    # units
    if units_col:
        df['units'] = pd.to_numeric(df[units_col], errors='coerce')
    else:
        df['units'] = np.nan

    # If prep_seconds is missing but there is duration in other forms (like two timestamps) we could compute, but skip for now
    # drop rows without prep_seconds or units? We'll keep rows that have prep_seconds and units
    df_valid = df[~df['prep_seconds'].isna() & ~df['units'].isna()].copy()

    if df_valid.shape[0] == 0:
        print("No valid rows with both prep_seconds and units found. Please ensure input has those columns.", file=sys.stderr)
        sys.exit(2)

    # lookback
    if df_valid['_ts'].notna().any():
        max_ts = df_valid['_ts'].max()
        cutoff = max_ts - pd.DateOffset(months=args.lookback_months)
        df_valid = df_valid[df_valid['_ts'] >= cutoff]

    # hour bucket
    df_valid['hour_bucket'] = df_valid['hour'].apply(lambda h: hour_to_bucket(int(h)) if not pd.isna(h) else 'other')

    # build unit buckets
    max_units = int(max( df_valid['units'].max(), 100 ))
    unit_ranges = build_unit_buckets(max_units=max_units)
    df_valid['units_bucket'] = df_valid['units'].apply(lambda u: assign_units_bucket(u, unit_ranges))

    # keep only hour buckets of interest (06-12,12-17,17-23) OR include 'other' if needed
    df_valid = df_valid[df_valid['hour_bucket'].isin(['06-12','12-17','17-23','other'])]

    # aggregate: group by day_of_week, hour_bucket, units_bucket
    groups = df_valid.groupby(['day_of_week','hour_bucket','units_bucket'], dropna=False)

    rows = []
    for (day, hbucket, ubucket), g in groups:
        if ubucket is None: continue
        # parse ubucket into min,max
        m = re.match(r'^(\d+)-(\d+)$', ubucket)
        if not m: continue
        umin = int(m.group(1)); umax = int(m.group(2))
        p75 = int(np.percentile(g['prep_seconds'].dropna(),75))
        orders = len(g)
        rows.append({
            'day_of_week': day,
            'hour_bucket': hbucket,
            'units_min': umin,
            'units_max': umax,
            'p75_seconds': int(p75),
            'orders_count': int(orders)
        })

    # convert to dataframe and sort
    res = pd.DataFrame(rows)
    if res.empty:
        print("No aggregated rows created.", file=sys.stderr)
        sys.exit(2)
    res = res.sort_values(['day_of_week','hour_bucket','units_min']).reset_index(drop=True)

    # compute base per day/hour (units_min == 1 or min bucket)
    def compute_base(group):
        # base = p75 for smallest bucket present in that day/hour (commonly 1-5)
        group = group.sort_values('units_min').copy()
        base_p75 = group.iloc[0]['p75_seconds']
        group['base_p75_seconds'] = base_p75
        group['increment_seconds'] = group['p75_seconds'] - base_p75
        return group

    out_frames = []
    for (day,h), g in res.groupby(['day_of_week','hour_bucket'], sort=False):
        gf = compute_base(g)
        out_frames.append(gf)
    full = pd.concat(out_frames).reset_index(drop=True)

    # Merge adjacent buckets if p75 diff <= 4 minutes (240 sec)
    merged_rows = []
    for (day,h), g in full.groupby(['day_of_week','hour_bucket'], sort=False):
        lst = g.sort_values('units_min').to_dict(orient='records')
        merged = merge_adjacent(lst, max_diff_seconds=4*60)
        # ensure monotonic non-decreasing p75 with units (if not, smooth by max so it never decreases)
        prev = -1
        for r in merged:
            if r['p75_seconds'] < prev:
                r['p75_seconds'] = prev
            prev = r['p75_seconds']
        for r in merged:
            r['day_of_week'] = day
            r['hour_bucket'] = h
            # recompute increment relative to base (we kept base as min across merged)
            r['increment_seconds'] = int(r['p75_seconds'] - r.get('base_p75_seconds', r['p75_seconds']))
            merged_rows.append(r)

    merged_df = pd.DataFrame(merged_rows)
    if merged_df.empty:
        print("No merged rows", file=sys.stderr)
        sys.exit(2)

    # Round increments and p75_seconds (round half up)
    for col in ['p75_seconds','base_p75_seconds','increment_seconds']:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].apply(lambda x: round_half_up(x) if not pd.isna(x) else x)

    # remove Saturday small buckets rule
    merged_df['day_of_week_clean'] = merged_df['day_of_week'].astype(str)
    sat_mask = merged_df['day_of_week_clean'].str.lower().str.contains('saturday') & (merged_df['orders_count'] < 20)
    merged_df = merged_df[~sat_mask]

    # final columns rename and ordering
    merged_df = merged_df.rename(columns={
        'units_min': 'units_min',
        'units_max': 'units_max',
        'p75_seconds': 'p75_seconds',
        'base_p75_seconds': 'base_p75_seconds',
        'increment_seconds': 'increment_seconds',
        'orders_count': 'orders_count'
    })

    final_cols = ['day_of_week','hour_bucket','units_min','units_max','p75_seconds','base_p75_seconds','increment_seconds','orders_count']
    for c in final_cols:
        if c not in merged_df.columns:
            merged_df[c] = np.nan

    merged_df = merged_df[final_cols].sort_values(['day_of_week','hour_bucket','units_min']).reset_index(drop=True)

    # save CSV
    merged_df.to_csv(args.out, index=False)
    print("Wrote", args.out)
    return 0

if __name__ == "__main__":
    sys.exit(main())
