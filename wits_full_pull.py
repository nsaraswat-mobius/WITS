"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          WITS FULL DATA PULL — ALL COUNTRIES, ALL YEARS                     ║
║          Computes: Bound-Applied Gap, NTM Burden,                           ║
║                    Tariff Escalation, Political Economy Score               ║
╚══════════════════════════════════════════════════════════════════════════════╝

HOW TO RUN:
    1. Install requirements:   pip install requests pandas tqdm
    2. Run:                    python wits_full_pull.py
    3. Output files:
           wits_raw_tariffs.csv          ← every country/HS/year raw pull
           wits_signals_final.csv        ← your 4 computed signals per country/year
           wits_missing_log.csv          ← log of missing/failed data points

ESTIMATED TIME:
    ~150 countries × 10 years × 2 HS codes = ~3,000 API calls
    At 1.5s delay = ~75 minutes total
    Checkpoints saved every 50 rows so you can resume if it stops.
"""

import requests
import pandas as pd
import time
import os
import json
from datetime import datetime
from tqdm import tqdm   # progress bar  →  pip install tqdm

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  —  Edit these if needed
# ──────────────────────────────────────────────────────────────────────────────

START_YEAR   = 2010          # Change to whatever year range you need
END_YEAR     = 2022
DELAY        = 1.5           # Seconds between API calls (do not set below 1.0)
CHECKPOINT   = 50            # Save progress every N rows

# Output file names
RAW_FILE      = "wits_raw_tariffs.csv"
SIGNALS_FILE  = "wits_signals_final.csv"
MISSING_FILE  = "wits_missing_log.csv"
PROGRESS_FILE = "wits_progress_checkpoint.json"

# ──────────────────────────────────────────────────────────────────────────────
# ALL COUNTRIES  (ISO3 codes — 180+ countries)
# ──────────────────────────────────────────────────────────────────────────────

ALL_COUNTRIES = [
    "AFG","ALB","DZA","AGO","ARG","ARM","AUS","AUT","AZE","BGD",
    "BLR","BEL","BEN","BOL","BIH","BWA","BRA","BGR","BFA","KHM",
    "CMR","CAN","CAF","CHL","CHN","COL","COD","COG","CRI","CIV",
    "HRV","CUB","CYP","CZE","DNK","DOM","ECU","EGY","SLV","ETH",
    "EST","FIN","FRA","GAB","GMB","GEO","DEU","GHA","GRC","GTM",
    "GIN","HND","HUN","IND","IDN","IRN","IRQ","IRL","ISR","ITA",
    "JAM","JPN","JOR","KAZ","KEN","KOR","KWT","KGZ","LAO","LVA",
    "LBN","LTU","LUX","MDG","MWI","MYS","MDV","MLI","MLT","MRT",
    "MUS","MEX","MDA","MNG","MAR","MOZ","MMR","NAM","NPL","NLD",
    "NZL","NIC","NER","NGA","MKD","NOR","OMN","PAK","PAN","PNG",
    "PRY","PER","PHL","POL","PRT","QAT","ROU","RUS","RWA","SAU",
    "SEN","SRB","SLE","SGP","SVK","SVN","SOM","ZAF","ESP","LKA",
    "SDN","SWE","CHE","SYR","TJK","TZA","THA","TGO","TUN","TUR",
    "TKM","UGA","UKR","ARE","GBR","USA","URY","UZB","VEN","VNM",
    "YEM","ZMB","ZWE","BWA","CPV","COM","DJI","ERI","SWZ","LSO",
    "LBR","MDG","MWI","MLI","MRT","MOZ","NAM","NER","RWA","SLE",
    "SOM","SDN","TGO","UGA","ZMB","ZWE"
]

# Remove duplicates
ALL_COUNTRIES = list(dict.fromkeys(ALL_COUNTRIES))

YEARS = list(range(START_YEAR, END_YEAR + 1))

# ──────────────────────────────────────────────────────────────────────────────
# DOMAIN HS CHAPTERS
# These are the HS chapters used across all L2 domains in the project.
# We pull ALL of them for every country so the data can be sliced by domain later.
# ──────────────────────────────────────────────────────────────────────────────

# Format: { "chapter_code": "description" }
HS_CHAPTERS = {
    "01": "Live Animals",
    "02": "Meat",
    "03": "Fish",
    "04": "Dairy",
    "07": "Vegetables",
    "08": "Fruit",
    "10": "Cereals",
    "12": "Oil Seeds",
    "15": "Fats & Oils",
    "27": "Fuels / Energy",
    "28": "Inorganic Chemicals",
    "29": "Organic Chemicals",
    "30": "Pharmaceuticals",
    "39": "Plastics",
    "72": "Iron & Steel",
    "84": "Machinery",
    "85": "Electrical Equipment",
    "87": "Vehicles",
    "88": "Aircraft",
    "90": "Medical Instruments",
    "94": "Furniture",
}

# For tariff ESCALATION we pair raw material → processed good
# Format: (raw_hs, processed_hs, label)
ESCALATION_PAIRS = [
    ("07", "20", "Vegetables → Preserved Food"),
    ("10", "19", "Cereals → Processed Food"),
    ("27", "84", "Raw Fuels → Energy Machinery"),
    ("28", "30", "Chemicals → Pharmaceuticals"),
    ("72", "84", "Steel → Machinery"),
    ("01", "02", "Live Animals → Meat"),
]

# ──────────────────────────────────────────────────────────────────────────────
# API FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://wits.worldbank.org/API/V1/SDMX/V21/rest/data"

def safe_get_value(url):
    """Call WITS API and return the first numeric value. Returns None on any error."""
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        series = data["data"]["dataSets"][0]["series"]
        if not series:
            return None
        first_series = list(series.values())[0]
        observations = first_series.get("observations", {})
        if not observations:
            return None
        value = list(observations.values())[0][0]
        return float(value) if value is not None else None
    except Exception:
        return None


def get_applied_tariff(country, hs_chapter, year):
    """MFN Applied tariff — what is actually charged"""
    url = f"{BASE_URL}/df_wits_tariff_trains/{country}.{hs_chapter}.MFN.AHS.{year}?format=JSON"
    return safe_get_value(url)


def get_bound_tariff(country, hs_chapter, year):
    """WTO Bound tariff — the legal maximum the country committed to"""
    url = f"{BASE_URL}/df_wits_tariff_wto/{country}.{hs_chapter}.BND.AHS.{year}?format=JSON"
    return safe_get_value(url)


def get_ntm_count(country, hs_chapter, year):
    """
    NTM (Non-Tariff Measures) — count of affected tariff lines.
    Note: WITS NTM data coverage is patchy — gaps are expected and logged.
    """
    url = f"{BASE_URL}/df_wits_ntm/{country}.{hs_chapter}.ALL.{year}?format=JSON"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        series = data["data"]["dataSets"][0]["series"]
        return len(series) if series else 0
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# SIGNAL COMPUTATION
# ──────────────────────────────────────────────────────────────────────────────

def compute_bound_applied_gap(avg_bound, avg_applied):
    """
    Signal 1: Bound-Applied Gap
    Formula : (avg_bound - avg_applied) / avg_bound
    Meaning : Close to 1.0 = huge gap = regulatory theater
              Close to 0.0 = law matches reality = good enforcement
    """
    if avg_bound and avg_bound > 0 and avg_applied is not None:
        return round((avg_bound - avg_applied) / avg_bound, 4)
    return None


def compute_tariff_escalation(raw_tariff, processed_tariff):
    """
    Signal 3: Tariff Escalation
    Formula : (tariff_processed - tariff_raw) / tariff_raw
    Meaning : Positive = govt protects processors (regulatory rent)
              Negative = favours raw material exports over processing
    """
    if raw_tariff and raw_tariff > 0 and processed_tariff is not None:
        return round((processed_tariff - raw_tariff) / raw_tariff, 4)
    return None


def compute_political_economy_score(avg_applied, ntm_count):
    """
    Signal 4: Political Economy Score
    Formula : 50% weight on tariff level + 50% weight on NTM burden
    Meaning : High = protectionist trade policy
              Low  = open trade policy
    Range   : 0 to 1
    """
    if avg_applied is None and ntm_count is None:
        return None
    tariff_component = min((avg_applied or 0) / 50, 1.0)   # normalise: 50% tariff = max
    ntm_component    = min((ntm_count   or 0) / 30, 1.0)   # normalise: 30 NTMs = max
    return round(tariff_component * 0.5 + ntm_component * 0.5, 4)


# ──────────────────────────────────────────────────────────────────────────────
# CHECKPOINT HELPERS  (so you can resume if script stops)
# ──────────────────────────────────────────────────────────────────────────────

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed": []}


def save_progress(completed_keys):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"completed": completed_keys}, f)


# ──────────────────────────────────────────────────────────────────────────────
# MAIN PULL LOOP
# ──────────────────────────────────────────────────────────────────────────────

def main():
    import sys
    sys.stderr.write("DEBUG: main() called\n")
    sys.stderr.flush()
    print("=" * 70)
    print("  WITS FULL DATA PULL  —  All Countries, All Years")
    print(f"  Countries : {len(ALL_COUNTRIES)}")
    print(f"  Years     : {START_YEAR} → {END_YEAR}  ({len(YEARS)} years)")
    print(f"  HS Chapters: {len(HS_CHAPTERS)}")
    print(f"  Estimated API calls: ~{len(ALL_COUNTRIES) * len(YEARS) * len(HS_CHAPTERS) * 2:,}")
    print(f"  Estimated time     : ~{round(len(ALL_COUNTRIES)*len(YEARS)*len(HS_CHAPTERS)*2*DELAY/3600,1)} hours")
    print("=" * 70)
    print()

    # ── Load checkpoint (resume support) ─────────────────────────────────────
    progress      = load_progress()
    completed_set = set(progress["completed"])

    raw_rows     = []
    signal_rows  = []
    missing_rows = []

    # ── Load existing output if resuming ─────────────────────────────────────
    if os.path.exists(RAW_FILE):
        raw_rows = pd.read_csv(RAW_FILE).to_dict("records")
        print(f"  ▶ Resuming — loaded {len(raw_rows)} existing raw rows")

    # ── Build task list ───────────────────────────────────────────────────────
    tasks = [
        (country, hs_code, year)
        for country in ALL_COUNTRIES
        for year    in YEARS
        for hs_code in HS_CHAPTERS.keys()
    ]

    total     = len(tasks)
    counter   = 0

    print(f"\n🚀 Starting pull... ({total:,} total calls)\n")

    for country, hs_code, year in tqdm(tasks, desc="Pulling WITS data"):

        key = f"{country}_{hs_code}_{year}"
        if key in completed_set:
            counter += 1
            continue   # Already done — skip

        # ── Pull applied tariff ───────────────────────────────────────────────
        applied = get_applied_tariff(country, hs_code, year)
        time.sleep(DELAY)

        # ── Pull bound tariff ─────────────────────────────────────────────────
        bound = get_bound_tariff(country, hs_code, year)
        time.sleep(DELAY)

        # ── Pull NTM count ────────────────────────────────────────────────────
        ntm = get_ntm_count(country, hs_code, year)
        time.sleep(DELAY)

        # ── Store raw row ─────────────────────────────────────────────────────
        raw_row = {
            "country"     : country,
            "hs_chapter"  : hs_code,
            "hs_desc"     : HS_CHAPTERS[hs_code],
            "year"        : year,
            "applied_tariff_pct" : applied,
            "bound_tariff_pct"   : bound,
            "ntm_count"          : ntm,
        }
        raw_rows.append(raw_row)

        # ── Log missing data ──────────────────────────────────────────────────
        if applied is None:
            missing_rows.append({"country": country, "hs": hs_code, "year": year, "missing": "applied_tariff"})
        if bound is None:
            missing_rows.append({"country": country, "hs": hs_code, "year": year, "missing": "bound_tariff"})
        if ntm is None:
            missing_rows.append({"country": country, "hs": hs_code, "year": year, "missing": "ntm_count"})

        # ── Mark complete ─────────────────────────────────────────────────────
        completed_set.add(key)
        counter += 1

        # ── Checkpoint every N rows ───────────────────────────────────────────
        if counter % CHECKPOINT == 0:
            pd.DataFrame(raw_rows).to_csv(RAW_FILE, index=False)
            save_progress(list(completed_set))
            tqdm.write(f"  💾 Checkpoint saved — {counter:,}/{total:,} done")

    # ── Save final raw file ───────────────────────────────────────────────────
    df_raw = pd.DataFrame(raw_rows)
    df_raw.to_csv(RAW_FILE, index=False)
    print(f"\n✅ Raw data saved → {RAW_FILE}  ({len(df_raw):,} rows)")

    # ──────────────────────────────────────────────────────────────────────────
    # COMPUTE THE 4 SIGNALS  (aggregate per country × year)
    # ──────────────────────────────────────────────────────────────────────────

    print("\n⚙️  Computing 4 signals per country × year...")

    for country in tqdm(ALL_COUNTRIES, desc="Computing signals"):
        for year in YEARS:

            subset = df_raw[(df_raw["country"] == country) & (df_raw["year"] == year)]

            if subset.empty:
                continue

            # ── Signal 1: Bound-Applied Gap ───────────────────────────────────
            applied_vals = subset["applied_tariff_pct"].dropna().tolist()
            bound_vals   = subset["bound_tariff_pct"].dropna().tolist()

            avg_applied = sum(applied_vals) / len(applied_vals) if applied_vals else None
            avg_bound   = sum(bound_vals)   / len(bound_vals)   if bound_vals   else None

            gap = compute_bound_applied_gap(avg_bound, avg_applied)

            # ── Signal 2: Domain NTM Burden ───────────────────────────────────
            ntm_vals    = subset["ntm_count"].dropna().tolist()
            ntm_burden  = int(sum(ntm_vals)) if ntm_vals else None

            # ── Signal 3: Tariff Escalation (average across all pairs) ────────
            escalation_list = []
            for raw_hs, proc_hs, label in ESCALATION_PAIRS:
                raw_row_  = subset[subset["hs_chapter"] == raw_hs]["applied_tariff_pct"]
                proc_row_ = subset[subset["hs_chapter"] == proc_hs]["applied_tariff_pct"]

                if not raw_row_.empty and not proc_row_.empty:
                    raw_val  = raw_row_.values[0]
                    proc_val = proc_row_.values[0]
                    esc      = compute_tariff_escalation(raw_val, proc_val)
                    if esc is not None:
                        escalation_list.append(esc)

            avg_escalation = (
                round(sum(escalation_list) / len(escalation_list), 4)
                if escalation_list else None
            )

            # ── Signal 4: Political Economy Score ────────────────────────────
            pol_score = compute_political_economy_score(avg_applied, ntm_burden)

            # ── Interpretation labels (human readable) ────────────────────────
            gap_label = (
                "Regulatory Theater (laws not enforced)" if gap and gap > 0.6 else
                "Moderate enforcement gap"               if gap and gap > 0.3 else
                "Good enforcement"                       if gap is not None    else
                "No data"
            )

            esc_label = (
                "High protection of processors (rent)"   if avg_escalation and avg_escalation > 0.5 else
                "Moderate escalation"                    if avg_escalation and avg_escalation > 0    else
                "Negative escalation (favours raw)"      if avg_escalation and avg_escalation <= 0   else
                "No data"
            )

            pol_label = (
                "Highly Protectionist"    if pol_score and pol_score > 0.6 else
                "Moderately Protectionist" if pol_score and pol_score > 0.3 else
                "Open Trade Policy"       if pol_score is not None         else
                "No data"
            )

            # ── Store signal row ──────────────────────────────────────────────
            signal_rows.append({
                "country"                : country,
                "year"                   : year,
                # Raw averages
                "avg_applied_tariff_pct" : round(avg_applied, 3) if avg_applied is not None else None,
                "avg_bound_tariff_pct"   : round(avg_bound,   3) if avg_bound   is not None else None,
                # Signal 1
                "bound_applied_gap"      : gap,
                "gap_interpretation"     : gap_label,
                # Signal 2
                "domain_ntm_burden"      : ntm_burden,
                # Signal 3
                "tariff_escalation_index": avg_escalation,
                "escalation_interpretation": esc_label,
                # Signal 4
                "political_economy_score": pol_score,
                "political_interpretation": pol_label,
            })

    # ── Save signals file ─────────────────────────────────────────────────────
    df_signals = pd.DataFrame(signal_rows)
    df_signals = df_signals.sort_values(["country", "year"]).reset_index(drop=True)
    df_signals.to_csv(SIGNALS_FILE, index=False)
    print(f"✅ Signals saved  → {SIGNALS_FILE}  ({len(df_signals):,} rows)")

    # ── Save missing log ──────────────────────────────────────────────────────
    if missing_rows:
        df_missing = pd.DataFrame(missing_rows)
        df_missing.to_csv(MISSING_FILE, index=False)
        print(f"⚠️  Missing log   → {MISSING_FILE}  ({len(df_missing):,} missing data points)")

    # ── Print sample of results ───────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SAMPLE OUTPUT (first 10 rows)")
    print("=" * 70)
    print(df_signals.head(10).to_string(index=False))

    print("\n" + "=" * 70)
    print("  SUMMARY STATISTICS")
    print("=" * 70)
    print(f"  Total country-year rows : {len(df_signals):,}")
    print(f"  Countries with data     : {df_signals['country'].nunique()}")
    print(f"  Years covered           : {df_signals['year'].min()} → {df_signals['year'].max()}")
    print(f"  Rows with gap data      : {df_signals['bound_applied_gap'].notna().sum():,}")
    print(f"  Rows with NTM data      : {df_signals['domain_ntm_burden'].notna().sum():,}")
    print(f"  Rows with escalation    : {df_signals['tariff_escalation_index'].notna().sum():,}")
    print(f"  Rows with pol. score    : {df_signals['political_economy_score'].notna().sum():,}")
    print()
    print("  🎉 DONE! Your 3 output files are ready.")
    print(f"     → {RAW_FILE}")
    print(f"     → {SIGNALS_FILE}    ← THIS IS YOUR MAIN OUTPUT")
    print(f"     → {MISSING_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
