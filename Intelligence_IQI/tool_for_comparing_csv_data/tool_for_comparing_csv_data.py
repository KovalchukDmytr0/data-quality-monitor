import pandas as pd
from rapidfuzz import fuzz
from datetime import datetime, timedelta
import logging

# === 🔧 CONFIGURATION ===
file1_path = '/Users/dmytrokovalchuk/Desktop/homeIQ/query_executor/tod_levitt_3_db.csv'
file2_path = '/Users/dmytrokovalchuk/Desktop/homeIQ/query_executor/TODD LEVITT-REAL_ESTATE_AGENT-transactions.csv'

similarity_threshold = 90
date_tolerance_days = 90  # ±3 months

comparison_keys = [
    (['street_address', 'unit_number', 'city', 'state', 'zip_code'], 
     ['Address', 'City', 'State', 'Zip Code']),
    (['sale_date'], ['Sale Date'])
]

# === 📜 LOGGING SETUP ===
log_filename = 'matching_log.txt'
logging.basicConfig(
    filename=log_filename,
    filemode='w',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger()

# === 🛠️ HELPERS ===

def normalize_address(row, cols):
    return ' '.join(
        str(row[col]).strip().lower()
        for col in cols if col in row and pd.notnull(row[col]) and row[col] != 'NaT'
    )

def normalize_dates(df, columns):
    for col in columns:
        if col in df.columns:
            log.debug(f"🧪 Raw '{col}' column values:")
            log.debug(df[col].head(10).to_string(index=False))

            def try_parse_date(x):
                if pd.isna(x):
                    log.debug("⚠️ Date is NaN (pandas-level NaT)")
                    return pd.NaT

                original_value = x

                # Convert float like 20250506.0 → '20250506'
                if isinstance(x, float) and x == int(x):
                    x_clean = str(int(x))
                else:
                    x_str = str(x).strip()
                    x_clean = x_str.replace('\u200b', '').replace('\u00a0', '').replace(' ', '')

                log.debug(f"🧪 Date parsing attempt:")
                log.debug(f"   ↪ Raw input: '{original_value}' (type: {type(original_value)})")
                log.debug(f"   ↪ Cleaned string: '{x_clean}'")
                log.debug(f"   ↪ Length: {len(x_clean)}, isdigit: {x_clean.isdigit()}")

                if not x_clean or x_clean.lower() == 'nan':
                    log.debug(f"⚠️ Empty or NaN string after cleaning: '{x_clean}'")
                    return pd.NaT

                try:
                    if x_clean.isdigit() and len(x_clean) == 8:
                        parsed = pd.to_datetime(x_clean, format='%Y%m%d', errors='coerce')
                        log.debug(f"📅 Used format: 'YYYYMMDD'")
                    else:
                        parsed = pd.to_datetime(x_clean, errors='coerce')
                        log.debug(f"📅 Used format: 'flexible infer'")

                    if pd.isna(parsed):
                        log.debug(f"❌ Final parse failed → Returned: NaT for '{x_clean}'")
                    else:
                        log.debug(f"✅ Final parsed result: {parsed}")

                    return parsed

                except Exception as e:
                    log.warning(f"⚠️ Exception during date parsing for '{x_clean}': {e}")
                    return pd.NaT

            df[col] = df[col].apply(try_parse_date)
    return df



def prepare_df(df, key_sets, file_label):
    df = df.copy()
    all_date_cols = [col for keys in key_sets for col in keys if 'date' in col.lower()]
    all_str_cols = [col for keys in key_sets for col in keys if col not in all_date_cols]

    for col in all_str_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip().str.lower()

    df = normalize_dates(df, all_date_cols)

    for i, cols in enumerate(key_sets):
        if all('date' in c.lower() for c in cols):
            df[f'match_key_{i}'] = df[cols[0]] if cols[0] in df.columns else pd.NaT
        else:
            df[f'match_key_{i}'] = df.apply(lambda row: normalize_address(row, cols), axis=1)

    df['__source'] = file_label
    return df

# === 🔍 MATCHING ===
def match_data(df1, df2, num_keys, threshold=90):
    matched = []
    unmatched_rows_file1 = []

    original_file1_cols = [col for col in df1.columns if not col.startswith('match_key') and col != '__source']
    original_file2_cols = [col for col in df2.columns if not col.startswith('match_key') and col != '__source']

    for idx1, row1 in df1.iterrows():
        found_match = False
        log.debug(f"\n🔍 Matching row {idx1} from file1:")
        log.debug(f"   Address key: {row1['match_key_0']}")
        log.debug(f"   Date key   : {row1['match_key_1']}")

        for idx2, row2 in df2.iterrows():
            scores = []

            for i in range(num_keys):
                val1 = row1[f'match_key_{i}']
                val2 = row2[f'match_key_{i}']

                if isinstance(val1, pd.Timestamp) or isinstance(val2, pd.Timestamp):
                    log.debug(f"🛠️ Date comparison check: file1={val1}, file2={val2}")

                if isinstance(val1, pd.Timestamp) and isinstance(val2, pd.Timestamp):
                    if pd.isnull(val1) or pd.isnull(val2):
                        log.debug(f"⚠️ Null date — file1: {val1}, file2: {val2}")
                        scores.append(0)
                    else:
                        days_diff = abs((val1 - val2).days)
                        log.debug(f"📅 Comparing dates — file1: {val1.date()}, file2: {val2.date()}, diff: {days_diff} days")
                        scores.append(100 if days_diff <= date_tolerance_days else 0)
                else:
                    score = fuzz.token_sort_ratio(str(val1), str(val2))
                    if i == 0:
                        log.debug(f"🏠 Address score: {score} → file1: '{val1}' vs file2: '{val2}'")
                    scores.append(score)

            avg_score = sum(scores) / len(scores)

            if avg_score >= threshold:
                found_match = True
                log.debug(f"✅ Match found — Similarity: {avg_score}")
                log.debug(f"    Matched address: file2[{idx2}] → {row2['match_key_0']}")
                log.debug(f"    Matched date: file1={row1['match_key_1']}, file2={row2['match_key_1']}")

                match = {
                    f'file1_{col}': row1.get(col, '') for col in original_file1_cols
                }
                match.update({
                    f'file2_{col}': row2.get(col, '') for col in original_file2_cols
                })
                match['similarity'] = round(avg_score, 2)
                matched.append(match)

        if not found_match:
            unmatched_rows_file1.append(row1)
            log.debug("❌ No match found for this row.\n")

    # All unmatched from file1
    unmatched_df1 = pd.DataFrame(unmatched_rows_file1)
    unmatched_df1.drop(columns=[c for c in unmatched_df1.columns if c.startswith('match_key') or c == '__source'], inplace=True, errors='ignore')

    # Optional: remove matched rows from file2 to create unmatched_df2
    matched_indexes_file2 = set()
    for match in matched:
        val = match.get(f'file2_{original_file2_cols[0]}')
        idxs = df2[df2[original_file2_cols[0]] == val].index.tolist()
        matched_indexes_file2.update(idxs)

    unmatched_df2 = df2.drop(index=matched_indexes_file2, errors='ignore')
    unmatched_df2.drop(columns=[c for c in unmatched_df2.columns if c.startswith('match_key') or c == '__source'], inplace=True, errors='ignore')

    return pd.DataFrame(matched), unmatched_df1, unmatched_df2


# === 🚀 MAIN EXECUTION ===
def main():
    print("📥 Loading CSVs...")
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    file1_keys = [pair[0] for pair in comparison_keys]
    file2_keys = [pair[1] for pair in comparison_keys]

    print("🔧 Preparing data...")
    df1_prepared = prepare_df(df1, file1_keys, 'file1')
    df2_prepared = prepare_df(df2, file2_keys, 'file2')

    log.debug("📊 Sample parsed dates from file1:")
    log.debug(df1_prepared[['sale_date']].dropna().head().to_string(index=False))

    log.debug("📊 Sample parsed dates from file2:")
    log.debug(df2_prepared[['Sale Date']].dropna().head().to_string(index=False))

    print("🔍 Matching...")
    matched_df, unmatched_df1, unmatched_df2 = match_data(df1_prepared, df2_prepared, len(comparison_keys), similarity_threshold)

    # === 📤 OUTPUT FILES ===
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    matched_file = f"matched_{timestamp}.csv"
    unmatched_file1 = f"unmatched_from_file1_{timestamp}.csv"
    unmatched_file2 = f"unmatched_from_file2_{timestamp}.csv"

    matched_df.to_csv(matched_file, index=False)
    unmatched_df1.to_csv(unmatched_file1, index=False)
    unmatched_df2.to_csv(unmatched_file2, index=False)

    print("✅ DONE")
    print(f"📄 Matched: {matched_file}")
    print(f"📄 Unmatched File 1: {unmatched_file1}")
    print(f"📄 Unmatched File 2: {unmatched_file2}")
    print(f"🪵 Log: {log_filename}")

if __name__ == "__main__":
    main()
