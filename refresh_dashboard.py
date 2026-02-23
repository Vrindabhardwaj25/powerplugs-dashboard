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
}

DASHBOARD_COUNTRIES = ['USA', 'India', 'Canada', 'UK + IR', 'Australia', 'Germany',
                       'UAE', 'Czech Republic', 'Thailand', 'Switzerland', 'Spain',
                       'Netherlands', 'Singapore', 'Philippines', 'France', 'Mexico',
                       'Poland', 'Saudi Arabia', 'Austria', 'Italy', 'Belgium', 'New Zealand']

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
# USER DATA (from Metabase card 19529 — trial/paid per PP + gender)
# ============================================================
def fetch_user_data():
    """
    Fetches user data from card 19529 (Trial Vs Paid - Powerplugs).
    Gets total trial + converted users per PP with gender breakdown.
    NUM_TRIAL_USERS = total users who started a trial (includes converted)
    SUM_CONVERTED_USERS = users who converted to paid
    So: paid = converted, on_trial = total - converted, total = total
    """
    print("Fetching user data from card 19529 via MBQL...")

    today = datetime.now().strftime('%Y-%m-%d')

    # Query: PP x Gender -> sum(trial), sum(converted)
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
                ['field', 'GENDER', {'base-type': 'type/Text'}],
            ],
            'filter': [
                'between',
                ['field', 'TRIAL_DATE', {'base-type': 'type/Date'}],
                DATA_START_DATE,
                today,
            ],
        },
    }

    try:
        result = mb_post('dataset', query)
        rows = result.get('data', {}).get('rows', [])
        print(f"  Got {len(rows)} user data rows")
    except Exception as e:
        print(f"  WARNING: Failed to fetch user data: {e}")
        print("  Falling back to hardcoded data...")
        return _hardcoded_user_data()

    if not rows:
        print("  WARNING: No user data rows returned, using hardcoded fallback")
        return _hardcoded_user_data()

    # Aggregate per PP: total users, paid, male%, female%
    pp_data = defaultdict(lambda: {'total': 0, 'paid': 0, 'male': 0, 'female': 0, 'other': 0})
    grand_total = {'total': 0, 'paid': 0, 'male': 0, 'female': 0, 'other': 0}

    for row in rows:
        pp_raw, gender, trial_sum, converted_sum = row[0], row[1], int(row[2] or 0), int(row[3] or 0)
        pp = PP_MAP.get(pp_raw.lower() if pp_raw else '', None)
        if not pp:
            continue

        pp_data[pp]['total'] += trial_sum
        pp_data[pp]['paid'] += converted_sum
        grand_total['total'] += trial_sum
        grand_total['paid'] += converted_sum

        gender_lower = (gender or '').lower()
        if gender_lower == 'male':
            pp_data[pp]['male'] += trial_sum
            grand_total['male'] += trial_sum
        elif gender_lower == 'female':
            pp_data[pp]['female'] += trial_sum
            grand_total['female'] += trial_sum
        else:
            pp_data[pp]['other'] += trial_sum
            grand_total['other'] += trial_sum

    # Build output format
    user_data = {}
    for pp in PLUGS:
        d = pp_data[pp]
        total = d['total'] or 1
        male_pct = round(d['male'] / total * 100)
        female_pct = round(d['female'] / total * 100)
        user_data[pp] = {
            'users': d['total'],
            'paid': d['paid'],
            'male': male_pct,
            'female': female_pct,
        }
        print(f"  {pp}: {d['total']:,} users ({d['paid']:,} paid, {male_pct}% male, {female_pct}% female)")

    # Grand total (deduplicated count not available, so use sum with disclaimer)
    gt = grand_total['total'] or 1
    user_data['_total'] = {
        'users': 0,  # Keep 0 to indicate "not deduplicated"
        'male': round(grand_total['male'] / gt * 100),
        'female': round(grand_total['female'] / gt * 100),
    }

    return user_data


def _hardcoded_user_data():
    """Fallback hardcoded user data."""
    return {
        'AFib':        {'users': 3434,  'paid': 3400,  'male': 62, 'female': 38},
        'Cardio':      {'users': 30648, 'paid': 30549, 'male': 58, 'female': 42},
        'CnO Pro':     {'users': 25653, 'paid': 24643, 'male': 55, 'female': 45},
        'Respiratory': {'users': 7757,  'paid': 7391,  'male': 51, 'female': 49},
        'Tesla':       {'users': 147,   'paid': 131,   'male': 65, 'female': 35},
        '_total':      {'users': 0, 'male': 57, 'female': 43},
    }


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
                    ['field', 'PRODUCT_ID', {'base-type': 'type/Text'}],
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

    # Rows: [COUNTRY, PRODUCT_ID, PURCHASE_DATE(day), SUM_AMOUNT_USD, COUNT]
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
def inject_data(template, revenue_data, purchase_data, trial_data, user_data, country_revenue):
    """Replace placeholder tokens in the template with real data."""
    print("Injecting data into template...")

    now = datetime.now().strftime('%b %d, %Y at %I:%M %p')

    output = template
    output = output.replace('/*__REVENUE_DATA__*/{}', json.dumps(revenue_data, separators=(',', ':')))
    output = output.replace('/*__PURCHASE_DATA__*/{}', json.dumps(purchase_data, separators=(',', ':')))
    output = output.replace('/*__TRIAL_DATA__*/{}', json.dumps(trial_data, separators=(',', ':')))
    output = output.replace('/*__USER_DATA__*/{}', json.dumps(user_data, separators=(',', ':')))
    output = output.replace('/*__COUNTRY_REVENUE_DATA__*/{}', json.dumps(country_revenue, separators=(',', ':')))
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

    # Inject into template
    output = inject_data(template, revenue_data, purchase_data, trial_data, user_data, country_revenue)

    # Write output
    OUTPUT_FILE.write_text(output)
    print(f"\nDashboard written to: {OUTPUT_FILE}")
    print(f"File size: {len(output):,} chars")
    print("Done!")


if __name__ == '__main__':
    main()
