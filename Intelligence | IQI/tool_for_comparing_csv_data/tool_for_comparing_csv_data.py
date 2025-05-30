import pandas as pd
from rapidfuzz import fuzz
from datetime import datetime

# === 🔧 CONFIGURATION ===
file1_path = '/Users/dmytrokovalchuk/Desktop/homeIQ/query_executor/sandavol_v3.csv'
file2_path = '/Users/dmytrokovalchuk/Desktop/homeIQ/query_executor/Michael Sandoval 12_01_17-05_19_25.csv'

comparison_keys = [
    (['street_address', 'unit_number', 'city', 'state', 'zip_code'], 
     ['Address - Street Number', 'Address - Street Name', 'Address - Street Suffix',
      'Address - Unit', 'Address - City', 'Address - State', 'Address - Zip Code']),
    (['sale_date'], ['Close Date'])
]

similarity_threshold = 85

# === 🛠️ HELPERS ===

def normalize_address(row, cols):
    """Join and clean address parts into standard US format"""
    return ' '.join(
        str(row[col]).strip().lower()
        for col in cols if col in row and pd.notnull(row[col]) and row[col] != 'NaT'
    )

def normalize_dates(df, columns):
    """Only convert true date columns, skip nulls"""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '')
    return df

def prepare_df(df, key_sets, file_label):
    df = df.copy()
    
    # Identify date and non-date columns
    date_cols = {'sale_date', 'Close Date'}
    non_date_cols = {col for keys in key_sets for col in keys if col not in date_cols}

    # Normalize string-based columns
    for col in non_date_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip().str.lower()

    df = normalize_dates(df, date_cols)

    # Create match keys
    for i, cols in enumerate(key_sets):
        df[f'match_key_{i}'] = df.apply(lambda row: normalize_address(row, cols), axis=1)

    df['__source'] = file_label
    return df

# === 🔍 MATCHING ===
def match_data(df1, df2, num_keys, threshold=90):
    matched = []
    used_df2_indexes = set()
    unmatched_rows_file1 = []
    unmatched_rows_file2 = df2.copy()

    original_file1_cols = [col for col in df1.columns if not col.startswith('match_key') and col != '__source']
    original_file2_cols = [col for col in df2.columns if not col.startswith('match_key') and col != '__source']

    for idx1, row1 in df1.iterrows():
        best_score = 0
        best_match_idx = None

        for idx2, row2 in df2.iterrows():
            if idx2 in used_df2_indexes:
                continue

            scores = [
                fuzz.token_sort_ratio(row1[f'match_key_{i}'], row2[f'match_key_{i}'])
                for i in range(num_keys)
            ]
            avg_score = sum(scores) / len(scores)

            if avg_score > best_score:
                best_score = avg_score
                best_match_idx = idx2

        if best_score >= threshold and best_match_idx is not None:
            row2 = df2.loc[best_match_idx]

            match = {
                f'file1_{col}': '' if pd.isna(row1.get(col)) else row1.get(col) for col in original_file1_cols
            }
            match.update({
                f'file2_{col}': '' if pd.isna(row2.get(col)) else row2.get(col) for col in original_file2_cols
            })
            match['similarity'] = round(best_score, 2)
            matched.append(match)
            used_df2_indexes.add(best_match_idx)
        else:
            unmatched_rows_file1.append(row1)

    unmatched_df2 = unmatched_rows_file2.drop(index=used_df2_indexes)
    unmatched_df1 = pd.DataFrame(unmatched_rows_file1)

    unmatched_df1.drop(columns=[c for c in unmatched_df1.columns if c.startswith('match_key') or c == '__source'], inplace=True, errors='ignore')
    unmatched_df2.drop(columns=[c for c in unmatched_df2.columns if c.startswith('match_key') or c == '__source'], inplace=True, errors='ignore')

    return pd.DataFrame(matched), unmatched_df1, unmatched_df2

# === 🚀 EXECUTION ===
def main():
    print("📥 Loading CSVs...")
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    file1_keys = [pair[0] for pair in comparison_keys]
    file2_keys = [pair[1] for pair in comparison_keys]

    print("🔧 Preparing data...")
    df1_prepared = prepare_df(df1, file1_keys, 'file1')
    df2_prepared = prepare_df(df2, file2_keys, 'file2')

    print("🔍 Matching...")
    matched_df, unmatched_df1, unmatched_df2 = match_data(df1_prepared, df2_prepared, len(comparison_keys), similarity_threshold)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    matched_df.to_csv(f"matched_{timestamp}.csv", index=False, na_rep='')
    unmatched_df1.to_csv(f"unmatched_from_file1_{timestamp}.csv", index=False, na_rep='')
    unmatched_df2.to_csv(f"unmatched_from_file2_{timestamp}.csv", index=False, na_rep='')

    print("✅ DONE")
    print(f"📄 Matched: matched_{timestamp}.csv")
    print(f"📄 Unmatched File 1: unmatched_from_file1_{timestamp}.csv")
    print(f"📄 Unmatched File 2: unmatched_from_file2_{timestamp}.csv")

if __name__ == "__main__":
    main()
