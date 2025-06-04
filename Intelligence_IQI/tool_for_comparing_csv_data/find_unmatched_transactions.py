import os
import sys
import logging
import pandas as pd
import psycopg2
import boto3
from datetime import datetime
from rapidfuzz import fuzz
from dotenv import load_dotenv

# === 📝 LOGGING SETUP ===
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("address_matching_log.txt", mode='w')
    ]
)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# === 🔐 ENV CONFIGURATION ===
load_dotenv()
ENV = 'dev'

REGION = os.getenv('REGION')
DB_HOST = os.getenv(f'{ENV.upper()}_DB_HOST')
DB_NAME = os.getenv(f'{ENV.upper()}_DB_NAME')
DB_USER = os.getenv(f'{ENV.upper()}_DB_USER')

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv(f'{ENV.upper()}_AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(f'{ENV.upper()}_AWS_SECRET_ACCESS_KEY')
os.environ['AWS_SESSION_TOKEN'] = os.getenv(f'{ENV.upper()}_AWS_SESSION_TOKEN')

# === 📦 DB CONNECTION ===
def get_iam_token():
    client = boto3.client('rds', region_name=REGION)
    return client.generate_db_auth_token(
        DBHostname=DB_HOST,
        Port=5432,
        DBUsername=DB_USER
    )

def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=get_iam_token(),
        port=5432,
        sslmode='require'
    )

# === 🧼 ADDRESS NORMALIZATION ===
def build_address(row, cols):
    return ' '.join(
        str(row.get(col, '')).strip()
        for col in cols if pd.notnull(row.get(col)) and row.get(col) != 'NaT'
    ).lower()

# === 🔎 FUZZY MATCHING ===
def best_fuzzy_match(csv_full_address, db_results):
    best_score = 0
    best_result = None
    for db_addr, db_id in db_results:
        if db_addr:
            score = fuzz.token_sort_ratio(csv_full_address, db_addr.lower())
            log.debug(f"🔁 Comparing: '{csv_full_address}' ↔ '{db_addr}' → Score: {score}")
            if score > best_score:
                best_score = score
                best_result = (db_addr, db_id)
    if best_score >= 80:  # 👈 lowered from 90 to 80
        log.info(f"✅ Match: '{csv_full_address}' → '{best_result[0]}' (Score: {best_score})")
    else:
        log.info(f"❌ No match: '{csv_full_address}' (Best Score: {best_score})")
    return best_result if best_score >= 80 else None  # 👈 also updated threshold here


# === 🚀 MAIN EXECUTION ===
def main():
    log.info("📥 Loading CSV...")
    csv_path = '/Users/dmytrokovalchuk/unmatched_from_file2_20250604_183151.csv'

    if not os.path.exists(csv_path):
        log.error(f"❌ File not found: {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        log.error(f"❌ Failed to load CSV: {e}")
        return

    # Clean object columns
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].fillna('')

    # Define column sets
    street_cols = ['CLIP']
    full_cols = ['Address', 'City', 'State', 'Zip Code']

    log.info("🔧 Building address columns...")
    df['street_address'] = df.apply(lambda row: build_address(row, street_cols), axis=1)
    df['full_address'] = df.apply(lambda row: build_address(row, full_cols), axis=1)

    # === 🔍 Optimized Bulk CLIP Query ===
    log.info("🚀 Querying DB once with all unique CLIPs...")

    clip_col = 'CLIP'
    if clip_col not in df.columns:
        log.error(f"❌ Column '{clip_col}' not found in CSV.")
        return

    df[clip_col] = df[clip_col].astype(str).str.strip()
    unique_clips = df[clip_col].dropna().unique().tolist()

    placeholders = ','.join(['%s'] * len(unique_clips))
    query = f"""
        SELECT clip, CONCAT_WS(' ', street_address, unit_number, city, state, zip_code) AS full_address, id
        FROM properties
        WHERE clip IN ({placeholders})
    """

    street_query_results = {}
    try:
        with connect_db() as conn:
            with conn.cursor() as cur:
                log.info(f"🔎 Sending bulk query for {len(unique_clips)} CLIPs...")
                cur.execute(query, unique_clips)
                for clip, address, pid in cur.fetchall():
                    clip_key = str(clip).strip()
                    street_query_results.setdefault(clip_key, []).append((address, pid))
    except Exception as e:
        log.error(f"❌ Bulk CLIP query failed: {e}")
        return

    # === 🔗 Fuzzy Matching with Cached Results ===
    log.info("🔗 Fuzzy matching...")
    matched, unmatched = [], []

    for _, row in df.iterrows():
        full_csv_addr = row['full_address']
        street = row['street_address']
        db_matches = street_query_results.get(street, [])
        best_match = best_fuzzy_match(full_csv_addr, db_matches)

        output = {
            'original_address': full_csv_addr,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        if best_match:
            output.update({
                'matched_address': best_match[0],
                'property_id': best_match[1]
            })
            matched.append(output)
        else:
            unmatched.append(output)

        # === 📤 OUTPUT FILES ===
    matched_df = pd.DataFrame(matched)
    unmatched_df = pd.DataFrame(unmatched)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    matched_file = f"matched_transactions_comparison_with_db_{timestamp}.csv"
    unmatched_file = f"unmatched_transactions_comparison_with_db_{timestamp}.csv"

    matched_df.to_csv(matched_file, index=False)
    unmatched_df.to_csv(unmatched_file, index=False)

    log.info("✅ DONE")
    log.info(f"📄 Matched file: {matched_file}")
    log.info(f"📄 Unmatched file: {unmatched_file}")


if __name__ == "__main__":
    main()
