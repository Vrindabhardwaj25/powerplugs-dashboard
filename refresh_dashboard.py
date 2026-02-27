#!/usr/bin/env python3
"""
Powerplugs Dashboard Refresh Script
====================================
Pulls fresh data from Metabase API and injects it into the dashboard template.

Usage:
    python3 refresh_dashboard.py

Requires:
    - .env file with METABASE_URL, METABASE_API_KEY
    - dashboard_template.html in the same directory
    - pip install requests python-dotenv

Output:
    - powerplugs_dashboard.html (generated with fresh data)
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv

# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR / '.env')

METABASE_URL = os.getenv('METABASE_URL', 'https://metabase.ultrahuman.com')
METABASE_API_KEY = os.getenv('METABASE_API_KEY', '')
GOOGLE_SHEETS_ID = os.getenv('GOOGLE_SHEETS_ID', '')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY', '')
GOOGLE_APPS_SCRIPT_URL = os.getenv('GOOGLE_APPS_SCRIPT_URL', '')

TEMPLATE_FILE = SCRIPT_DIR / 'dashboard_template.html'
OUTPUT_FILE = SCRIPT_DIR / 'powerplugs_dashboard.html'

# Metabase card IDs
REVENUE_CARD_ID = 9444      # Native SQL - revenue data
REVENUE_SOURCE_CARD_ID = 9061  # Underlying raw revenue data (has COUNTRY column)
TRIAL_SOURCE_CARD_ID = 19529  # Base card for trial data (MBQL queries)
TRIAL_DATABASE_ID = 2        # Database ID for MBQL queries

# Powerplug name mapping: Metabase API values -> Dashboard display keys
# Revenue card returns mixed names (e.g. "Cardio Adaptability", "CnO Pro", "respiratory_health")
# Trial card returns lowercase (e.g. "afib", "cardio", "cno_pro_n_plus")
PP_MAP = {
    'afib': 'AFib',
    'cardio': 'Cardio',
    'cardio adaptability': 'Cardio',
    'cno_pro_n_plus': 'CnO Pro',
    'cno pro': 'CnO Pro',
    'c&o_pro_offering': 'CnO Pro',
    'cno_pro_offering': 'CnO Pro',
    'respiratory_health': 'Respiratory',
    'respiratory': 'Respiratory',
    'respiratoryhealth': 'Respiratory',
    'tesla': 'Tesla',
}

# Template tag IDs for revenue card 9444
REVENUE_TAG_IDS = {
    'date_level': '46af49f9-31be-479a-8ea2-2055eb340762',
    'start_date': '4d4199ae-2365-4f30-90ae-8b6b26ee08c5',
    'end_date': '3f0a3d54-69ba-4eae-904e-0ca5a7502662',
}

PLUGS = ['AFib', 'Cardio', 'CnO Pro', 'Respiratory', 'Tesla']

# Country name normalization: Metabase returns full country names, we map to dashboard short names
COUNTRY_MAP = {
    'united states of america': 'USA',
    'united states': 'USA',
    'us': 'USA',
    'usa': 'USA',
    'india': 'India',
    'canada': 'Canada',
    'united kingdom of great britain and northern ireland': 'UK + IR',
    'united kingdom': 'UK + IR',
    'ireland': 'UK + IR',
    'uk': 'UK + IR',
    'australia': 'Australia',
    'germany': 'Germany',
    'united arab emirates': 'UAE',
    'uae': 'UAE',
    'czech republic': 'Czech Republic',
    'czechia': 'Czech Republic',
    'thailand': 'Thailand',
    'switzerland': 'Switzerland',
    'spain': 'Spain',
    'netherlands': 'Netherlands',
    'singapore': 'Singapore',
    'philippines': 'Philippines',
    'france': 'France',
    'mexico': 'Mexico',
    'poland': 'Poland',
    'saudi arabia': 'Saudi Arabia',
    'austria': 'Austria',
    'italy': 'Italy',
    'italia': 'Italy',
    'belgium': 'Belgium',
    'new zealand': 'New Zealand',
    'taiwan': 'Taiwan',
    'taiwan, province of china': 'Taiwan',
}

DASHBOARD_COUNTRIES = ['USA', 'India', 'Canada', 'UK + IR', 'Australia', 'Germany',
                       'UAE', 'Czech Republic', 'Thailand', 'Switzerland', 'Spain',
                       'Netherlands', 'Singapore', 'Philippines', 'France', 'Mexico',
                       'Poland', 'Saudi Arabia', 'Austria', 'Italy', 'Belgium', 'New Zealand',
                       'Taiwan']

# Data start date
DATA_START_DATE = '2025-09-01'

# ============================================================
# METABASE API HELPERS
# ============================================================
def mb_headers():
    return {
        'X-Api-Key': METABASE_API_KEY,
        'Content-Type': 'application/json',
    }

def mb_post(endpoint, payload=None, retries=3):
    """POST request to Metabase API with retry logic."""
    import time
    url = f"{METABASE_URL}/api/{endpoint}"
    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=mb_headers(), json=payload or {}, timeout=300)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
            if attempt < retries - 1:
                wait = 10 * (attempt + 1)
                print(f"  Retry {attempt+1}/{retries} after {wait}s: {e}")
                time.sleep(wait)
            else:
                raise

def mb_get(endpoint):
    """GET request to Metabase API."""
    url = f"{METABASE_URL}/api/{endpoint}"
    resp = requests.get(url, headers=mb_headers(), timeout=60)
    resp.raise_for_status()
    return resp.json()

# ============================================================
# FETCH REVENUE DATA (Card 9444 - Native SQL)
# ============================================================
def derive_revenue_from_country_data(country_revenue):
    """
    Derives global REVENUE_DATA by summing across all countries from country_revenue.
    This avoids the need for card 9444 (which can timeout).
    Returns dict in the format:
    {
      "2025-09": {
        "dates": ["2025-09-01", ...],
        "revenue": {"AFib": [1.23, ...], "Cardio": [...], ...},
        "subscriptions": {"AFib": [10, ...], ...}
      }, ...
    }
    """
    print("Deriving global revenue data from country revenue...")

    # Collect all dates per month across all countries
    month_dates = defaultdict(set)
    for country, months in country_revenue.items():
        for month_key, mdata in months.items():
            for d in mdata.get('dates', []):
                month_dates[month_key].add(d)

    # Sum revenue/subs across all countries for each date
    revenue_data = {}
    for month_key in sorted(month_dates.keys()):
        dates = sorted(month_dates[month_key])
        rev_by_pp = {p: [0.0] * len(dates) for p in PLUGS}
        subs_by_pp = {p: [0] * len(dates) for p in PLUGS}

        date_idx_map = {d: i for i, d in enumerate(dates)}

        for country, months in country_revenue.items():
            if month_key not in months:
                continue
            cdata = months[month_key]
            for ci, cd in enumerate(cdata.get('dates', [])):
                if cd in date_idx_map:
                    gi = date_idx_map[cd]
                    for p in PLUGS:
                        rev_by_pp[p][gi] += round(cdata['revenue'].get(p, [0] * len(cdata['dates']))[ci] if ci < len(cdata['revenue'].get(p, [])) else 0, 2)
                        subs_by_pp[p][gi] += cdata['subscriptions'].get(p, [0] * len(cdata['dates']))[ci] if ci < len(cdata['subscriptions'].get(p, [])) else 0

        # Round final values
        for p in PLUGS:
            rev_by_pp[p] = [round(v, 2) for v in rev_by_pp[p]]

        revenue_data[month_key] = {
            'dates': dates,
            'revenue': rev_by_pp,
            'subscriptions': subs_by_pp,
        }

    print(f"  Revenue data: {len(revenue_data)} months ({', '.join(sorted(revenue_data.keys()))})")
    for mk in sorted(revenue_data.keys()):
        total = sum(sum(revenue_data[mk]['revenue'][p]) for p in PLUGS)
        print(f"    {mk}: {len(revenue_data[mk]['dates'])} days, ${total:,.0f}")
    return revenue_data


# ============================================================
# FETCH TRIAL DATA (Card 19529 via MBQL /api/dataset)
# ============================================================
def fetch_trial_data_for_period(start_date, end_date):
    """
    Fetch trial/conversion data for a date range via MBQL query.
    Returns raw rows: [PP_TYPE, DATE, TRIAL_COUNT, CONVERTED_COUNT]
    """
    query = {
        'database': TRIAL_DATABASE_ID,
        'type': 'query',
        'query': {
            'source-table': f'card__{TRIAL_SOURCE_CARD_ID}',
            'aggregation': [
                ['sum', ['field', 'NUM_TRIAL_USERS', {'base-type': 'type/Integer'}]],
                ['sum', ['field', 'SUM_CONVERTED_USERS', {'base-type': 'type/Integer'}]],
            ],
            'breakout': [
                ['field', 'POWERPLUG_TYPE', {'base-type': 'type/Text'}],
                ['field', 'TRIAL_DATE', {'base-type': 'type/Date', 'temporal-unit': 'day'}],
            ],
            'filter': [
                'between',
                ['field', 'TRIAL_DATE', {'base-type': 'type/Date'}],
                start_date,
                end_date,
            ],
        },
    }

    result = mb_post('dataset', query)
    return result.get('data', {}).get('rows', [])


def fetch_trial_data():
    """
    Fetches trial vs converted data for all PPs.
    Returns dict in the format:
    {
      "AFib": {
        "2025-09": {
          "dates": ["2025-09-01", ...],
          "trial": [4, 3, ...],
          "converted": [2, 1, ...]
        }, ...
      }, ...
    }
    """
    print("Fetching trial data via MBQL...")

    today = datetime.now().strftime('%Y-%m-%d')

    # Fetch in monthly chunks to avoid truncation
    all_rows = []
    start = datetime.strptime(DATA_START_DATE, '%Y-%m-%d')
    end = datetime.now()

    current = start
    while current < end:
        # End of month
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)

        period_end = min(next_month - timedelta(days=1), end)
        period_start_str = current.strftime('%Y-%m-%d')
        period_end_str = period_end.strftime('%Y-%m-%d')

        print(f"  Fetching trials {period_start_str} to {period_end_str}...")
        rows = fetch_trial_data_for_period(period_start_str, period_end_str)
        print(f"    Got {len(rows)} rows")
        all_rows.extend(rows)

        current = next_month

    print(f"  Total trial rows: {len(all_rows)}")

    # Group: PP -> month -> date -> {trial, converted}
    pp_monthly = defaultdict(lambda: defaultdict(lambda: {}))

    for row in all_rows:
        pp_raw = row[0]
        date_str = row[1][:10] if row[1] else ''
        trial_count = int(row[2] or 0)
        converted_count = int(row[3] or 0)

        pp = PP_MAP.get(pp_raw.lower() if pp_raw else '', None)
        if not pp or not date_str:
            continue

        month_key = date_str[:7]
        pp_monthly[pp][month_key][date_str] = {
            'trial': trial_count,
            'converted': converted_count,
        }

    # Build final structure with filled-in zeros for missing days
    trial_data = {}
    for pp in PLUGS:
        if pp not in pp_monthly:
            continue

        pp_data = {}
        for month_key in sorted(pp_monthly[pp].keys()):
            year, month = map(int, month_key.split('-'))

            # Determine days in month
            if month == 12:
                days_in_month = (datetime(year + 1, 1, 1) - datetime(year, month, 1)).days
            else:
                days_in_month = (datetime(year, month + 1, 1) - datetime(year, month, 1)).days

            # For current month, only include up to today
            today_dt = datetime.now()
            if year == today_dt.year and month == today_dt.month:
                max_day = today_dt.day
            else:
                max_day = days_in_month

            dates = []
            trials = []
            converted = []

            for day in range(1, max_day + 1):
                d = f"{month_key}-{day:02d}"
                dates.append(d)
                day_data = pp_monthly[pp][month_key].get(d, {'trial': 0, 'converted': 0})
                trials.append(day_data['trial'])
                converted.append(day_data['converted'])

            pp_data[month_key] = {
                'dates': dates,
                'trial': trials,
                'converted': converted,
            }

        trial_data[pp] = pp_data

    for pp in PLUGS:
        months = list(trial_data.get(pp, {}).keys())
        print(f"  {pp}: {len(months)} months ({', '.join(months)})")

    return trial_data


# ============================================================
# BUILD PURCHASE DATA (derived from revenue subscriptions)
# ============================================================
def build_purchase_data(revenue_data):
    """
    Build purchase data from revenue subscriptions counts.
    The subscription count in the revenue data IS the purchase count.
    """
    print("Building purchase data from revenue subscriptions...")
    purchase_data = {}
    for month_key, mdata in revenue_data.items():
        purchase_data[month_key] = {
            'dates': mdata['dates'],
            'purchases': dict(mdata['subscriptions']),  # subs count = purchase count
        }
    print(f"  Purchase data: {len(purchase_data)} months")
    return purchase_data


# ============================================================
# USER DATA — Currently active users (paid subscribers + active trials)
# ============================================================
def fetch_user_data():
    """
    Fetches CURRENTLY ACTIVE powerplug users by combining two sources:

    1. Active Paid Subscribers (from all_purchase):
       Users whose last purchase is within their plan's duration window.
       Monthly → last purchase within 35 days
       Yearly  → last purchase within 370 days
       2-Year  → last purchase within 740 days

    2. Active Trial Users (from RevCat card 19529):
       Users who started a trial within the trial window and haven't converted.
       C&O Pro/Plus → 30 days (longest yearly trial)
       Others       → 7 days (longest yearly trial)
    """
    print("Fetching ACTIVE user data...")

    # ---- Part 1: Active Paid from all_purchase ----
    print("  [1/2] Fetching active paid subscribers from all_purchase...")
    paid_sql = f"""
    WITH user_latest AS (
      SELECT
        EMAIL,
        CASE
          WHEN POWERPLUG_PLAN LIKE 'AFib%' THEN 'AFib'
          WHEN POWERPLUG_PLAN LIKE 'Cardio%' THEN 'Cardio'
          WHEN POWERPLUG_PLAN LIKE 'CnO%' OR POWERPLUG_PLAN LIKE 'cno%' THEN 'CnO Pro'
          WHEN POWERPLUG_PLAN LIKE 'respiratory%' OR POWERPLUG_PLAN LIKE 'Respiratory%' THEN 'Respiratory'
          WHEN POWERPLUG_PLAN LIKE 'tesla%' OR POWERPLUG_PLAN LIKE 'Tesla%' THEN 'Tesla'
          ELSE NULL
        END as PP,
        CASE
          WHEN POWERPLUG_PLAN LIKE '%-monthly' THEN 35
          WHEN POWERPLUG_PLAN LIKE '%-yearly' OR POWERPLUG_PLAN LIKE '%-1 year' THEN 370
          WHEN POWERPLUG_PLAN LIKE '%-2 year%' THEN 740
          ELSE 35
        END as active_window_days,
        MAX(TO_DATE(PURCHASE_DATE)) as last_purchase
      FROM "all_purchase"
      WHERE PRODUCT_CATEGORY = 'powerplug'
        AND POWERPLUG_PLAN IS NOT NULL
      GROUP BY 1, 2, 3
    ),
    active_paid AS (
      SELECT EMAIL, PP
      FROM user_latest
      WHERE PP IS NOT NULL
        AND last_purchase >= DATEADD('day', -active_window_days, CURRENT_DATE())
    )
    SELECT PP, COUNT(DISTINCT EMAIL) as active_paid
    FROM active_paid
    GROUP BY PP
    ORDER BY 2 DESC
    """

    paid_query = {
        'database': TRIAL_DATABASE_ID,
        'type': 'native',
        'native': {'query': paid_sql},
    }

    paid_by_pp = {}
    try:
        result = mb_post('dataset', paid_query)
        rows = result.get('data', {}).get('rows', [])
        for r in rows:
            pp = r[0]
            if pp in PLUGS:
                paid_by_pp[pp] = int(r[1] or 0)
        print(f"    Active paid: {sum(paid_by_pp.values()):,} total — " +
              ", ".join(f"{p}: {paid_by_pp.get(p,0):,}" for p in PLUGS))
    except Exception as e:
        print(f"    WARNING: Failed to fetch active paid: {e}")

    # ---- Part 2: Active Trial Users from RevCat ----
    print("  [2/2] Fetching active trial users from RevCat...")
    # Trial window per PP (max trial duration in days)
    trial_windows = {
        'cno_pro_n_plus': 30,  # 7d monthly, 30d yearly
        'afib': 7,
        'cardio': 7,
        'respiratory': 7,
        'tesla': 7,
    }

    on_trial_by_pp = {}
    for pp_raw, window in trial_windows.items():
        pp = PP_MAP.get(pp_raw)
        if not pp:
            continue
        query = {
            'database': TRIAL_DATABASE_ID,
            'type': 'query',
            'query': {
                'source-table': f'card__{TRIAL_SOURCE_CARD_ID}',
                'aggregation': [
                    ['sum', ['field', 'NUM_TRIAL_USERS', {'base-type': 'type/Integer'}]],
                    ['sum', ['field', 'SUM_CONVERTED_USERS', {'base-type': 'type/Integer'}]],
                ],
                'breakout': [
                    ['field', 'POWERPLUG_TYPE', {'base-type': 'type/Text'}],
                ],
                'filter': [
                    'and',
                    ['=', ['field', 'POWERPLUG_TYPE', {'base-type': 'type/Text'}], pp_raw],
                    ['>=', ['field', 'TRIAL_DATE', {'base-type': 'type/Date'}],
                     ['relative-datetime', -window, 'day']],
                ],
            },
        }
        try:
            result = mb_post('dataset', query)
            rows = result.get('data', {}).get('rows', [])
            if rows:
                trials = int(rows[0][1] or 0)
                converted = int(rows[0][2] or 0)
                on_trial = max(0, trials - converted)
                on_trial_by_pp[pp] = on_trial
        except Exception as e:
            print(f"    WARNING: Failed to fetch trial data for {pp}: {e}")

    print(f"    On trial: {sum(on_trial_by_pp.values()):,} total — " +
          ", ".join(f"{p}: {on_trial_by_pp.get(p,0):,}" for p in PLUGS))

    # ---- Part 3: Gender split from RevCat (all-time, just for %) ----
    print("  [3/3] Fetching gender split from RevCat...")
    gender_query = {
        'database': TRIAL_DATABASE_ID,
        'type': 'query',
        'query': {
            'source-table': f'card__{TRIAL_SOURCE_CARD_ID}',
            'aggregation': [
                ['sum', ['field', 'NUM_TRIAL_USERS', {'base-type': 'type/Integer'}]],
            ],
            'breakout': [
                ['field', 'POWERPLUG_TYPE', {'base-type': 'type/Text'}],
                ['field', 'GENDER', {'base-type': 'type/Text'}],
            ],
            'filter': [
                'between',
                ['field', 'TRIAL_DATE', {'base-type': 'type/Date'}],
                DATA_START_DATE,
                datetime.now().strftime('%Y-%m-%d'),
            ],
        },
    }

    gender_by_pp = defaultdict(lambda: {'male': 0, 'female': 0, 'other': 0, 'total': 0})
    try:
        result = mb_post('dataset', gender_query)
        rows = result.get('data', {}).get('rows', [])
        for r in rows:
            pp_raw, gender, count = r[0], r[1], int(r[2] or 0)
            pp = PP_MAP.get(pp_raw.lower() if pp_raw else '', None)
            if not pp:
                continue
            gender_by_pp[pp]['total'] += count
            g = (gender or '').lower()
            if g == 'male':
                gender_by_pp[pp]['male'] += count
            elif g == 'female':
                gender_by_pp[pp]['female'] += count
            else:
                gender_by_pp[pp]['other'] += count
    except Exception as e:
        print(f"    WARNING: Failed to fetch gender data: {e}")

    # ---- Build output ----
    user_data = {}
    grand_male, grand_female, grand_total_g = 0, 0, 0
    for pp in PLUGS:
        paid = paid_by_pp.get(pp, 0)
        on_trial = on_trial_by_pp.get(pp, 0)
        total_active = paid + on_trial
        gd = gender_by_pp[pp]
        gt = gd['total'] or 1
        male_pct = round(gd['male'] / gt * 100)
        female_pct = round(gd['female'] / gt * 100)
        user_data[pp] = {
            'users': total_active,
            'paid': paid,
            'on_trial': on_trial,
            'male': male_pct,
            'female': female_pct,
        }
        grand_male += gd['male']
        grand_female += gd['female']
        grand_total_g += gd['total']
        print(f"    {pp}: {total_active:,} active ({paid:,} paid + {on_trial:,} on trial, {male_pct}%M/{female_pct}%F)")

    ggt = grand_total_g or 1
    user_data['_total'] = {
        'users': 0,
        'male': round(grand_male / ggt * 100),
        'female': round(grand_female / ggt * 100),
    }

    print(f"  Total active (sum, not deduped): {sum(paid_by_pp.values()) + sum(on_trial_by_pp.values()):,}")
    return user_data


def fetch_country_user_data():
    """
    Fetches per-country userbase data so the dashboard JS can filter by country.
    Uses:
      1. Snowflake all_purchase + geo_data_nr_mp for active paid subscribers by country
      2. Card 19529 (RevCat) with COUNTRY breakout for trial users + gender by country
    Returns dict: { country: { pp: { paid, on_trial, users, male%, female% } } }
    """
    print("Fetching country-level user data...")

    # ---- Part 1: Active paid by country from Snowflake ----
    print("  [1/3] Active paid subscribers by country...")
    paid_country_sql = f"""
    WITH uid_map AS (
      SELECT DISTINCT "original_user_id" AS uid, "id" AS user_id, "email"
      FROM "users"
      WHERE "original_user_id" IS NOT NULL
    ),
    geo AS (
      SELECT bb.user_id, bb."email",
        COALESCE(d."name", f.COUNTRY) as country_name
      FROM "geo_data_nr_mp" f
      LEFT JOIN uid_map bb ON bb.uid = f."USER_ID"
      LEFT JOIN "country_code_mapping" d ON f.COUNTRY = d."country_code"
      QUALIFY ROW_NUMBER() OVER(PARTITION BY bb.user_id ORDER BY f."TIMESTAMP" DESC) = 1
    ),
    user_latest AS (
      SELECT
        ap.EMAIL,
        CASE
          WHEN POWERPLUG_PLAN LIKE 'AFib%' THEN 'AFib'
          WHEN POWERPLUG_PLAN LIKE 'Cardio%' THEN 'Cardio'
          WHEN POWERPLUG_PLAN LIKE 'CnO%' OR POWERPLUG_PLAN LIKE 'cno%' THEN 'CnO Pro'
          WHEN POWERPLUG_PLAN LIKE 'respiratory%' OR POWERPLUG_PLAN LIKE 'Respiratory%' THEN 'Respiratory'
          WHEN POWERPLUG_PLAN LIKE 'tesla%' OR POWERPLUG_PLAN LIKE 'Tesla%' THEN 'Tesla'
          ELSE NULL
        END as PP,
        CASE
          WHEN POWERPLUG_PLAN LIKE '%-monthly' THEN 35
          WHEN POWERPLUG_PLAN LIKE '%-yearly' OR POWERPLUG_PLAN LIKE '%-1 year' THEN 370
          WHEN POWERPLUG_PLAN LIKE '%-2 year%' THEN 740
          ELSE 35
        END as active_window_days,
        MAX(TO_DATE(PURCHASE_DATE)) as last_purchase
      FROM "all_purchase" ap
      WHERE PRODUCT_CATEGORY = 'powerplug'
        AND POWERPLUG_PLAN IS NOT NULL
      GROUP BY 1, 2, 3
    ),
    active_paid AS (
      SELECT EMAIL, PP
      FROM user_latest
      WHERE PP IS NOT NULL
        AND last_purchase >= DATEADD('day', -active_window_days, CURRENT_DATE())
    )
    SELECT COALESCE(g.country_name, 'Unknown') as country, ap.PP,
           COUNT(DISTINCT ap.EMAIL) as active_paid
    FROM active_paid ap
    LEFT JOIN geo g ON LOWER(ap.EMAIL) = LOWER(g."email")
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    paid_by_country_pp = defaultdict(lambda: defaultdict(int))
    try:
        result = mb_post('dataset', {
            'database': TRIAL_DATABASE_ID,
            'type': 'native',
            'native': {'query': paid_country_sql},
        })
        rows = result.get('data', {}).get('rows', [])
        print(f"    Got {len(rows)} rows")
        for r in rows:
            raw_country = (r[0] or 'Unknown').strip()
            country = COUNTRY_MAP.get(raw_country.lower(), None)
            pp = r[1]
            count = int(r[2] or 0)
            if country and pp in PLUGS:
                paid_by_country_pp[country][pp] += count
            elif pp in PLUGS:
                # Countries not in our map go to 'Other'
                paid_by_country_pp['Other'][pp] += count
    except Exception as e:
        print(f"    WARNING: Failed to fetch paid by country: {e}")

    # ---- Part 2: Trial users by country from card 19529 ----
    print("  [2/3] Trial users by country...")
    trial_windows = {
        'cno_pro_n_plus': 30,
        'afib': 7,
        'cardio': 7,
        'respiratory': 7,
        'tesla': 7,
    }
    trial_by_country_pp = defaultdict(lambda: defaultdict(int))
    for pp_raw, window in trial_windows.items():
        pp = PP_MAP.get(pp_raw)
        if not pp:
            continue
        query = {
            'database': TRIAL_DATABASE_ID,
            'type': 'query',
            'query': {
                'source-table': f'card__{TRIAL_SOURCE_CARD_ID}',
                'aggregation': [
                    ['sum', ['field', 'NUM_TRIAL_USERS', {'base-type': 'type/Integer'}]],
                    ['sum', ['field', 'SUM_CONVERTED_USERS', {'base-type': 'type/Integer'}]],
                ],
                'breakout': [
                    ['field', 'POWERPLUG_TYPE', {'base-type': 'type/Text'}],
                    ['field', 'COUNTRY', {'base-type': 'type/Text'}],
                ],
                'filter': [
                    'and',
                    ['=', ['field', 'POWERPLUG_TYPE', {'base-type': 'type/Text'}], pp_raw],
                    ['>=', ['field', 'TRIAL_DATE', {'base-type': 'type/Date'}],
                     ['relative-datetime', -window, 'day']],
                ],
            },
        }
        try:
            result = mb_post('dataset', query)
            rows = result.get('data', {}).get('rows', [])
            for r in rows:
                raw_country = (r[1] or 'Unknown').strip()
                country = COUNTRY_MAP.get(raw_country.lower(), None)
                trials = int(r[2] or 0)
                converted = int(r[3] or 0)
                on_trial = max(0, trials - converted)
                if country:
                    trial_by_country_pp[country][pp] += on_trial
                else:
                    trial_by_country_pp['Other'][pp] += on_trial
        except Exception as e:
            print(f"    WARNING: Failed trial by country for {pp}: {e}")

    # ---- Part 3: Gender split by country from card 19529 ----
    print("  [3/3] Gender split by country...")
    gender_query = {
        'database': TRIAL_DATABASE_ID,
        'type': 'query',
        'query': {
            'source-table': f'card__{TRIAL_SOURCE_CARD_ID}',
            'aggregation': [
                ['sum', ['field', 'NUM_TRIAL_USERS', {'base-type': 'type/Integer'}]],
            ],
            'breakout': [
                ['field', 'POWERPLUG_TYPE', {'base-type': 'type/Text'}],
                ['field', 'COUNTRY', {'base-type': 'type/Text'}],
                ['field', 'GENDER', {'base-type': 'type/Text'}],
            ],
            'filter': [
                'between',
                ['field', 'TRIAL_DATE', {'base-type': 'type/Date'}],
                DATA_START_DATE,
                datetime.now().strftime('%Y-%m-%d'),
            ],
        },
    }
    # gender_data[country][pp] = {male: N, female: N, other: N, total: N}
    gender_data = defaultdict(lambda: defaultdict(lambda: {'male': 0, 'female': 0, 'other': 0, 'total': 0}))
    try:
        result = mb_post('dataset', gender_query)
        rows = result.get('data', {}).get('rows', [])
        print(f"    Got {len(rows)} gender rows")
        for r in rows:
            pp_raw = (r[0] or '').lower()
            raw_country = (r[1] or 'Unknown').strip()
            gender = (r[2] or '').lower()
            count = int(r[3] or 0)
            pp = PP_MAP.get(pp_raw)
            country = COUNTRY_MAP.get(raw_country.lower(), None)
            if not pp or not country:
                continue
            gender_data[country][pp]['total'] += count
            if gender == 'male':
                gender_data[country][pp]['male'] += count
            elif gender == 'female':
                gender_data[country][pp]['female'] += count
            else:
                gender_data[country][pp]['other'] += count
    except Exception as e:
        print(f"    WARNING: Failed gender by country: {e}")

    # ---- Build output ----
    all_countries = set(list(paid_by_country_pp.keys()) + list(trial_by_country_pp.keys()))
    country_user_data = {}
    for country in sorted(all_countries):
        if country == 'Other':
            continue
        country_user_data[country] = {}
        for pp in PLUGS:
            paid = paid_by_country_pp[country].get(pp, 0)
            on_trial = trial_by_country_pp[country].get(pp, 0)
            gd = gender_data[country][pp]
            gt = gd['total'] or 1
            male_pct = round(gd['male'] / gt * 100)
            female_pct = round(gd['female'] / gt * 100)
            country_user_data[country][pp] = {
                'paid': paid,
                'on_trial': on_trial,
                'users': paid + on_trial,
                'male': male_pct,
                'female': female_pct,
            }

    print(f"  Country user data for {len(country_user_data)} countries")
    for c in list(sorted(country_user_data.keys()))[:5]:
        total = sum(d['users'] for d in country_user_data[c].values())
        print(f"    {c}: {total:,} total active")

    return country_user_data


def _hardcoded_user_data():
    """Fallback hardcoded user data."""
    return {
        'AFib':        {'users': 3700,  'paid': 3700,  'on_trial': 0, 'male': 62, 'female': 38},
        'Cardio':      {'users': 34211, 'paid': 34211, 'on_trial': 0, 'male': 58, 'female': 42},
        'CnO Pro':     {'users': 24835, 'paid': 24835, 'on_trial': 0, 'male': 55, 'female': 45},
        'Respiratory': {'users': 7288,  'paid': 7288,  'on_trial': 0, 'male': 51, 'female': 49},
        'Tesla':       {'users': 99,    'paid': 99,    'on_trial': 0, 'male': 65, 'female': 35},
        '_total':      {'users': 0, 'male': 57, 'female': 43},
    }


# ============================================================
# FETCH USER OVERLAP DATA (active paid users only, deduplicated)
# ============================================================
def fetch_user_overlap():
    """
    Fetches deduplicated ACTIVE user counts across powerplugs.
    Only counts users whose last purchase is within their plan's active window:
    Monthly → 35 days, Yearly → 370 days, 2-Year → 740 days.
    Uses EMAIL as user identifier.
    """
    print("Fetching active user overlap data via Metabase native query...")

    sql = f"""
    WITH user_latest AS (
      SELECT
        EMAIL,
        CASE
          WHEN POWERPLUG_PLAN LIKE 'AFib%' THEN 'AFib'
          WHEN POWERPLUG_PLAN LIKE 'Cardio%' THEN 'Cardio'
          WHEN POWERPLUG_PLAN LIKE 'CnO%' OR POWERPLUG_PLAN LIKE 'cno%' THEN 'CnO Pro'
          WHEN POWERPLUG_PLAN LIKE 'respiratory%' OR POWERPLUG_PLAN LIKE 'Respiratory%' THEN 'Respiratory'
          WHEN POWERPLUG_PLAN LIKE 'tesla%' OR POWERPLUG_PLAN LIKE 'Tesla%' THEN 'Tesla'
          ELSE NULL
        END as PP,
        CASE
          WHEN POWERPLUG_PLAN LIKE '%-monthly' THEN 35
          WHEN POWERPLUG_PLAN LIKE '%-yearly' OR POWERPLUG_PLAN LIKE '%-1 year' THEN 370
          WHEN POWERPLUG_PLAN LIKE '%-2 year%' THEN 740
          ELSE 35
        END as active_window_days,
        MAX(TO_DATE(PURCHASE_DATE)) as last_purchase
      FROM "all_purchase"
      WHERE PRODUCT_CATEGORY = 'powerplug'
        AND POWERPLUG_PLAN IS NOT NULL
      GROUP BY 1, 2, 3
    ),
    active_paid AS (
      SELECT EMAIL, PP
      FROM user_latest
      WHERE PP IS NOT NULL
        AND last_purchase >= DATEADD('day', -active_window_days, CURRENT_DATE())
    ),
    per_user AS (
      SELECT
        EMAIL,
        COUNT(DISTINCT PP) as num_pps,
        LISTAGG(DISTINCT PP, ' + ') WITHIN GROUP (ORDER BY PP) as combo
      FROM active_paid
      GROUP BY EMAIL
    ),
    overlap_counts AS (
      SELECT num_pps, COUNT(*) as user_count
      FROM per_user GROUP BY num_pps
    ),
    combo_counts AS (
      SELECT combo, COUNT(*) as users
      FROM per_user WHERE num_pps >= 2
      GROUP BY combo ORDER BY users DESC LIMIT 10
    ),
    pp_unique AS (
      SELECT PP, COUNT(DISTINCT EMAIL) as unique_users
      FROM active_paid
      GROUP BY PP
    ),
    total_unique AS (
      SELECT COUNT(DISTINCT EMAIL) as total FROM active_paid
    )
    SELECT 'overlap' as qtype, CAST(num_pps AS VARCHAR) as key1, NULL as key2, user_count as val FROM overlap_counts
    UNION ALL
    SELECT 'combo' as qtype, combo as key1, NULL as key2, users as val FROM combo_counts
    UNION ALL
    SELECT 'pp_unique' as qtype, PP as key1, NULL as key2, unique_users as val FROM pp_unique
    UNION ALL
    SELECT 'total' as qtype, 'total_unique_users' as key1, NULL as key2, total as val FROM total_unique
    """

    query = {
        'database': TRIAL_DATABASE_ID,
        'type': 'native',
        'native': {'query': sql},
    }

    try:
        result = mb_post('dataset', query)
        rows = result.get('data', {}).get('rows', [])
        print(f"  Got {len(rows)} user overlap rows")
    except Exception as e:
        print(f"  WARNING: Failed to fetch user overlap: {e}")
        return _hardcoded_user_overlap()

    if not rows:
        print("  WARNING: No user overlap rows, using fallback")
        return _hardcoded_user_overlap()

    # Parse into structured dict
    overlap_data = {
        'total_unique': 0,
        'per_pp': {},
        'overlap': {},
        'top_combos': [],
    }

    for row in rows:
        qtype, key1, _, val = row[0], row[1], row[2], int(row[3] or 0)
        if qtype == 'total':
            overlap_data['total_unique'] = val
        elif qtype == 'pp_unique':
            overlap_data['per_pp'][key1] = val
        elif qtype == 'overlap':
            overlap_data['overlap'][key1] = val
        elif qtype == 'combo':
            overlap_data['top_combos'].append({'combo': key1, 'users': val})

    naive_sum = sum(overlap_data['per_pp'].values())
    multi_pp = naive_sum - overlap_data['total_unique']
    print(f"  True unique users: {overlap_data['total_unique']:,}")
    print(f"  Naive sum across PPs: {naive_sum:,} (overlap of {multi_pp:,})")
    for pp, count in sorted(overlap_data['per_pp'].items()):
        print(f"    {pp}: {count:,}")

    return overlap_data


def _hardcoded_user_overlap():
    """Fallback hardcoded user overlap data."""
    return {
        'total_unique': 42162,
        'per_pp': {'AFib': 2927, 'Cardio': 13003, 'CnO Pro': 24179, 'Respiratory': 8412, 'Tesla': 114},
        'overlap': {'1': 36297, '2': 5281, '3': 561, '4': 22, '5': 1},
        'top_combos': [
            {'combo': 'Cardio + CnO Pro', 'users': 2007},
            {'combo': 'Cardio + Respiratory', 'users': 1464},
            {'combo': 'CnO Pro + Respiratory', 'users': 931},
            {'combo': 'Cardio + CnO Pro + Respiratory', 'users': 399},
            {'combo': 'AFib + CnO Pro', 'users': 364},
            {'combo': 'AFib + Respiratory', 'users': 321},
            {'combo': 'AFib + Cardio', 'users': 162},
        ],
    }


# ============================================================
# FETCH CUMULATIVE USERS (Snowflake all_purchase - running total per PP per month)
# ============================================================
def fetch_cumulative_users():
    """
    Fetches cumulative unique users per PP per month from Snowflake all_purchase table.
    Uses EMAIL to deduplicate — counts how many unique users have made at least one purchase
    for a given PP up to and including each month.
    Also computes a deduplicated total across all PPs per month.

    Returns dict: { "2025-09": { "AFib": 1234, "Cardio": 5678, ..., "_total": 9999 }, ... }
    """
    print("Fetching cumulative user data via Metabase native query...")

    today = datetime.now().strftime('%Y-%m-%d')
    today_month = datetime.now().strftime('%Y-%m')

    sql = f"""
    WITH months AS (
      SELECT DISTINCT TO_CHAR(TO_DATE(PURCHASE_DATE), 'YYYY-MM') as mk
      FROM "all_purchase"
      WHERE PRODUCT_CATEGORY = 'powerplug'
        AND TO_DATE(PURCHASE_DATE) >= '{DATA_START_DATE}'
        AND TO_DATE(PURCHASE_DATE) <= '{today}'
    ),
    user_first_pp AS (
      SELECT
        EMAIL,
        CASE
          WHEN POWERPLUG_PLAN LIKE 'AFib%' THEN 'AFib'
          WHEN POWERPLUG_PLAN LIKE 'Cardio%' THEN 'Cardio'
          WHEN POWERPLUG_PLAN LIKE 'CnO%' THEN 'CnO Pro'
          WHEN POWERPLUG_PLAN LIKE 'respiratory%' THEN 'Respiratory'
          WHEN POWERPLUG_PLAN LIKE 'tesla%' THEN 'Tesla'
        END as PP,
        TO_CHAR(MIN(TO_DATE(PURCHASE_DATE)), 'YYYY-MM') as first_month
      FROM "all_purchase"
      WHERE PRODUCT_CATEGORY = 'powerplug'
        AND TO_DATE(PURCHASE_DATE) >= '{DATA_START_DATE}'
        AND POWERPLUG_PLAN IS NOT NULL
      GROUP BY EMAIL, PP
    ),
    user_first_any AS (
      SELECT
        EMAIL,
        TO_CHAR(MIN(TO_DATE(PURCHASE_DATE)), 'YYYY-MM') as first_month
      FROM "all_purchase"
      WHERE PRODUCT_CATEGORY = 'powerplug'
        AND TO_DATE(PURCHASE_DATE) >= '{DATA_START_DATE}'
        AND POWERPLUG_PLAN IS NOT NULL
      GROUP BY EMAIL
    ),
    per_pp AS (
      SELECT m.mk as month, uf.PP, COUNT(DISTINCT uf.EMAIL) as cumulative_users
      FROM months m
      JOIN user_first_pp uf ON uf.first_month <= m.mk
      WHERE uf.PP IS NOT NULL
      GROUP BY m.mk, uf.PP
    ),
    total_dedup AS (
      SELECT m.mk as month, COUNT(DISTINCT ufa.EMAIL) as cumulative_users
      FROM months m
      JOIN user_first_any ufa ON ufa.first_month <= m.mk
      GROUP BY m.mk
    )
    SELECT 'pp' as qtype, month, PP as key1, cumulative_users as val FROM per_pp
    UNION ALL
    SELECT 'total' as qtype, month, '_total' as key1, cumulative_users as val FROM total_dedup
    ORDER BY month, qtype, key1
    """

    query = {
        'database': TRIAL_DATABASE_ID,
        'type': 'native',
        'native': {'query': sql},
    }

    try:
        result = mb_post('dataset', query)
        rows = result.get('data', {}).get('rows', [])
        print(f"  Got {len(rows)} cumulative user rows")
    except Exception as e:
        print(f"  WARNING: Failed to fetch cumulative users: {e}")
        return _hardcoded_cumulative_users()

    if not rows:
        print("  WARNING: No cumulative user rows, using fallback")
        return _hardcoded_cumulative_users()

    # Parse: { month: { PP: count, "_total": count } }
    cumulative = defaultdict(dict)
    for row in rows:
        qtype, month, key, val = row[0], row[1], row[2], int(row[3] or 0)
        cumulative[month][key] = val

    # Print summary
    for month in sorted(cumulative.keys()):
        total = cumulative[month].get('_total', 0)
        pp_parts = ', '.join(f"{p}:{cumulative[month].get(p, 0):,}" for p in PLUGS)
        print(f"  {month}: total={total:,} ({pp_parts})")

    return dict(cumulative)


def _hardcoded_cumulative_users():
    """Fallback hardcoded cumulative users data."""
    return {}


# ============================================================
# FETCH PLAN MIX DATA (Monthly / Yearly / 2-Year revenue split)
# ============================================================
def fetch_plan_mix():
    """
    Fetches plan type revenue breakdown per PP per month via Metabase native SQL.
    Returns: { "2025-09": { "AFib": { "Monthly": 1234, "Yearly": 5678, "2-Year": 910 }, ... }, ... }
    """
    print("Fetching plan mix data via Metabase native query...")
    today = datetime.now().strftime('%Y-%m-%d')

    sql = f"""
    SELECT
      TO_CHAR(TO_DATE(PURCHASE_DATE), 'YYYY-MM') as month,
      CASE
        WHEN POWERPLUG_PLAN LIKE 'AFib%' THEN 'AFib'
        WHEN POWERPLUG_PLAN LIKE 'Cardio%' THEN 'Cardio'
        WHEN POWERPLUG_PLAN LIKE 'CnO%' THEN 'CnO Pro'
        WHEN POWERPLUG_PLAN LIKE 'respiratory%' THEN 'Respiratory'
        WHEN POWERPLUG_PLAN LIKE 'tesla%' THEN 'Tesla'
        ELSE NULL
      END as pp,
      CASE
        WHEN POWERPLUG_PLAN LIKE '%-monthly' THEN 'Monthly'
        WHEN POWERPLUG_PLAN LIKE '%-yearly' OR POWERPLUG_PLAN LIKE '%-1 year' THEN 'Yearly'
        WHEN POWERPLUG_PLAN LIKE '%-2 year%' THEN '2-Year'
        ELSE 'Other'
      END as plan_type,
      ROUND(SUM(AMOUNT_USD), 2) as revenue,
      COUNT(*) as purchases
    FROM "all_purchase"
    WHERE PRODUCT_CATEGORY = 'powerplug'
      AND TO_DATE(PURCHASE_DATE) >= '{DATA_START_DATE}'
      AND TO_DATE(PURCHASE_DATE) <= '{today}'
    GROUP BY 1, 2, 3
    HAVING pp IS NOT NULL
    ORDER BY 1, 2, 3
    """

    try:
        data = mb_post('dataset', {
            'database': 2,
            'type': 'native',
            'native': {'query': sql},
        })

        rows = data.get('data', {}).get('rows', [])
        if not rows:
            print("  WARNING: No plan mix data returned, using empty dict")
            return {}

        print(f"  Got {len(rows)} plan mix rows")

        # Build nested structure: month -> pp -> plan_type -> { revenue, purchases }
        result = {}
        for row in rows:
            mk, pp, plan_type, revenue, purchases = row[0], row[1], row[2], float(row[3] or 0), int(row[4] or 0)
            pp_mapped = PP_MAP.get(pp.lower(), pp) if pp else None
            if not pp_mapped or pp_mapped not in PLUGS:
                continue
            if mk not in result:
                result[mk] = {}
            if pp_mapped not in result[mk]:
                result[mk][pp_mapped] = {}
            result[mk][pp_mapped][plan_type] = {
                'revenue': round(revenue, 2),
                'purchases': purchases,
            }

        # Print summary
        for mk in sorted(result.keys()):
            total = sum(
                result[mk][pp][pt]['revenue']
                for pp in result[mk]
                for pt in result[mk][pp]
            )
            plan_totals = {}
            for pp in result[mk]:
                for pt in result[mk][pp]:
                    plan_totals[pt] = plan_totals.get(pt, 0) + result[mk][pp][pt]['revenue']
            parts = ', '.join(f"{pt}: ${v:,.0f}" for pt, v in sorted(plan_totals.items()))
            print(f"  {mk}: ${total:,.0f} ({parts})")

        return result

    except Exception as e:
        print(f"  ERROR fetching plan mix: {e}")
        return {}


# ============================================================
# FETCH COUNTRY REVENUE (Card 9061 - raw revenue data with COUNTRY)
# ============================================================
def fetch_country_revenue():
    """
    Fetches DAILY revenue data broken down by country and powerplug.
    Uses card 9061 (underlying raw revenue data that includes COUNTRY column).

    Returns dict with per-country daily data matching REVENUE_DATA structure:
    {
      "USA": {
        "2025-09": {
          "dates": ["2025-09-01", "2025-09-02", ...],
          "revenue": {"AFib": [10.5, 20.3, ...], "Cardio": [...], ...},
          "subscriptions": {"AFib": [1, 2, ...], ...}
        }, ...
      },
      "India": { ... },
      ...
    }
    """
    print("Fetching daily country-wise revenue from card 9061 via MBQL...")

    today = datetime.now().strftime('%Y-%m-%d')

    # Fetch in 2-week chunks to avoid gateway timeouts on heavy months
    all_rows = []
    start = datetime.strptime(DATA_START_DATE, '%Y-%m-%d')
    end = datetime.now()

    current = start
    while current < end:
        period_end = min(current + timedelta(days=14), end)
        period_start_str = current.strftime('%Y-%m-%d')
        period_end_str = period_end.strftime('%Y-%m-%d')

        print(f"  Fetching country revenue {period_start_str} to {period_end_str}...")

        query = {
            'database': TRIAL_DATABASE_ID,
            'type': 'query',
            'query': {
                'source-table': f'card__{REVENUE_SOURCE_CARD_ID}',
                'aggregation': [
                    ['sum', ['field', 'AMOUNT_USD', {'base-type': 'type/Float'}]],
                    ['count'],
                ],
                'breakout': [
                    ['field', 'COUNTRY', {'base-type': 'type/Text'}],
                    ['field', 'POWERPLUG_TYPE', {'base-type': 'type/Text'}],
                    ['field', 'PURCHASE_DATE', {'base-type': 'type/DateTimeWithLocalTZ', 'temporal-unit': 'day'}],
                ],
                'filter': [
                    'between',
                    ['field', 'PURCHASE_DATE', {'base-type': 'type/DateTimeWithLocalTZ'}],
                    period_start_str,
                    period_end_str,
                ],
            },
        }

        try:
            result = mb_post('dataset', query)
            rows = result.get('data', {}).get('rows', [])
            print(f"    Got {len(rows)} rows")
            all_rows.extend(rows)
        except Exception as e:
            print(f"    WARNING: Failed to fetch country revenue for {period_start_str}: {e}")

        current = period_end + timedelta(days=1)

    print(f"  Total country revenue rows: {len(all_rows)}")

    if not all_rows:
        print("  WARNING: No country revenue data returned!")
        return {}

    # Rows: [COUNTRY, POWERPLUG_TYPE, PURCHASE_DATE(day), SUM_AMOUNT_USD, COUNT]
    # Group by country -> month -> date -> PP -> {revenue, subs}
    raw = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'revenue': 0, 'subs': 0})))

    for row in all_rows:
        country_raw = row[0] or ''
        pp_raw = row[1] or ''
        date_str = row[2][:10] if row[2] else ''  # "2025-09-01T00:00:00" -> "2025-09-01"
        amount = float(row[3] or 0)
        count = int(row[4] or 0)

        if not date_str:
            continue

        # Normalize country name
        country = COUNTRY_MAP.get(country_raw.lower().strip(), None)
        if not country:
            country = 'Other'

        # Map PP name
        pp = PP_MAP.get(pp_raw.lower().strip(), None)
        if not pp:
            for k, v in PP_MAP.items():
                if v.lower() == pp_raw.lower().strip():
                    pp = v
                    break
            if not pp:
                continue

        month_key = date_str[:7]
        raw[country][(month_key, date_str)][pp]['revenue'] += round(amount, 2)
        raw[country][(month_key, date_str)][pp]['subs'] += count

    # Build final structure: country -> month -> {dates, revenue{PP: [...]}, subscriptions{PP: [...]}}
    country_revenue = {}
    for country in sorted(raw.keys()):
        # Collect all (month, date) pairs for this country
        month_dates = defaultdict(set)
        for (mk, d) in raw[country].keys():
            month_dates[mk].add(d)

        country_months = {}
        for month_key in sorted(month_dates.keys()):
            dates = sorted(month_dates[month_key])
            rev_by_pp = {p: [] for p in PLUGS}
            subs_by_pp = {p: [] for p in PLUGS}

            for d in dates:
                for p in PLUGS:
                    day_data = raw[country].get((month_key, d), {}).get(p, {'revenue': 0, 'subs': 0})
                    rev_by_pp[p].append(round(day_data['revenue'], 2))
                    subs_by_pp[p].append(day_data['subs'])

            country_months[month_key] = {
                'dates': dates,
                'revenue': rev_by_pp,
                'subscriptions': subs_by_pp,
            }

        country_revenue[country] = country_months

    # Print summary
    for country in sorted(country_revenue.keys()):
        months = sorted(country_revenue[country].keys())
        total_rev = 0
        for mk in months:
            for p in PLUGS:
                total_rev += sum(country_revenue[country][mk]['revenue'].get(p, []))
        print(f"  {country}: {len(months)} months, ${total_rev:,.0f} total")

    return country_revenue


# ============================================================
# TEMPLATE INJECTION
# ============================================================
def inject_data(template, revenue_data, purchase_data, trial_data, user_data, country_revenue, user_overlap, cumulative_users, plan_mix, country_user_data=None):
    """Replace placeholder tokens in the template with real data."""
    print("Injecting data into template...")

    now = datetime.now().strftime('%b %d, %Y at %I:%M %p')

    output = template
    output = output.replace('/*__REVENUE_DATA__*/{}', json.dumps(revenue_data, separators=(',', ':')))
    output = output.replace('/*__PURCHASE_DATA__*/{}', json.dumps(purchase_data, separators=(',', ':')))
    output = output.replace('/*__TRIAL_DATA__*/{}', json.dumps(trial_data, separators=(',', ':')))
    output = output.replace('/*__USER_DATA__*/{}', json.dumps(user_data, separators=(',', ':')))
    output = output.replace('/*__COUNTRY_REVENUE_DATA__*/{}', json.dumps(country_revenue, separators=(',', ':')))
    output = output.replace('/*__USER_OVERLAP__*/{}', json.dumps(user_overlap, separators=(',', ':')))
    output = output.replace('/*__CUMULATIVE_USERS__*/{}', json.dumps(cumulative_users, separators=(',', ':')))
    output = output.replace('/*__PLAN_MIX__*/{}', json.dumps(plan_mix, separators=(',', ':')))
    output = output.replace('/*__COUNTRY_USER_DATA__*/{}', json.dumps(country_user_data or {}, separators=(',', ':')))
    output = output.replace('/*__LAST_UPDATED__*/', now)

    # Google Sheets config
    output = output.replace('%%GOOGLE_SHEETS_ID%%', GOOGLE_SHEETS_ID)
    output = output.replace('%%GOOGLE_API_KEY%%', GOOGLE_API_KEY)
    output = output.replace('%%GOOGLE_APPS_SCRIPT_URL%%', GOOGLE_APPS_SCRIPT_URL)

    return output


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("Powerplugs Dashboard Refresh")
    print("=" * 60)
    print(f"Metabase: {METABASE_URL}")
    print(f"Template: {TEMPLATE_FILE}")
    print(f"Output:   {OUTPUT_FILE}")
    print()

    if not METABASE_API_KEY:
        print("ERROR: METABASE_API_KEY not set in .env")
        sys.exit(1)

    if not TEMPLATE_FILE.exists():
        print(f"ERROR: Template not found: {TEMPLATE_FILE}")
        sys.exit(1)

    # Read template
    template = TEMPLATE_FILE.read_text()

    # Fetch data — country revenue first, then derive global revenue from it
    # (avoids card 9444 which frequently times out)
    try:
        country_revenue = fetch_country_revenue()
    except Exception as e:
        print(f"ERROR fetching country revenue: {e}")
        sys.exit(1)

    # Derive global revenue by summing across all countries
    revenue_data = derive_revenue_from_country_data(country_revenue)

    try:
        trial_data = fetch_trial_data()
    except Exception as e:
        print(f"ERROR fetching trial data: {e}")
        sys.exit(1)

    purchase_data = build_purchase_data(revenue_data)
    user_data = fetch_user_data()
    user_overlap = fetch_user_overlap()
    cumulative_users = fetch_cumulative_users()
    plan_mix = fetch_plan_mix()
    country_user_data = fetch_country_user_data()

    # Inject into template
    output = inject_data(template, revenue_data, purchase_data, trial_data, user_data, country_revenue, user_overlap, cumulative_users, plan_mix, country_user_data)

    # Write output
    OUTPUT_FILE.write_text(output)
    print(f"\nDashboard written to: {OUTPUT_FILE}")
    print(f"File size: {len(output):,} chars")
    print("Done!")


if __name__ == '__main__':
    main()
