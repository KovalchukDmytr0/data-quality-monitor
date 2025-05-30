import os
import sys
import json
import boto3
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment
load_dotenv()
ENV = 'PROD_HIQ'

REGION = os.getenv('REGION')
DB_HOST = os.getenv(f'{ENV}_DB_HOST')
DB_NAME = os.getenv(f'{ENV}_DB_NAME')
DB_USER = os.getenv(f'{ENV}_DB_USER')

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv(f'{ENV}_AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(f'{ENV}_AWS_SECRET_ACCESS_KEY')
os.environ['AWS_SESSION_TOKEN'] = os.getenv(f'{ENV}_AWS_SESSION_TOKEN')

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

def fetch_home_shopper_data():
    query = "SELECT id, home_shopper FROM properties WHERE home_shopper_active = true;"
    with connect_db() as conn:
        return pd.read_sql_query(query, conn)

def classify_shoppers(df):
    today = pd.Timestamp.utcnow()  # ✅ timezone-aware UTC
    results = []

    for _, row in df.iterrows():
        property_id = row['id']
        shopper_json = row['home_shopper']

        if not shopper_json or not isinstance(shopper_json, dict):
            continue

        history = shopper_json.get('hs_history', [])
        if not history or not isinstance(history, list):
            continue

        entry = history[-1]

        try:
            first_observed = pd.to_datetime(entry.get('first_observed'), utc=True)
            last_observed = pd.to_datetime(entry.get('last_observed'), utc=True)

            # Fallback to recorded_date if needed
            if pd.isna(first_observed) or pd.isna(last_observed):
                recorded_date_raw = entry.get('recorded_date')
                if recorded_date_raw:
                    fallback_date = pd.to_datetime(recorded_date_raw, utc=True)
                    if pd.isna(first_observed):
                        first_observed = fallback_date
                    if pd.isna(last_observed):
                        last_observed = fallback_date

            if pd.isna(first_observed) or pd.isna(last_observed):
                continue

            visits = int(entry.get('unique_obs_count', 0))

        except Exception:
            continue

        days_first = (today - first_observed).days
        days_last = (today - last_observed).days

        category = 'Uncategorized'

        # 🔥 Hot Buyer
        if visits >= 2 and last_observed > (today - pd.Timedelta(days=31)):
            category = 'Hot Buyer'

        # 🟢 Just Starting
        elif visits == 1 and days_first <= 30:
            category = 'Just Starting'

        # 🟡 Passive Buyer
        elif visits == 1 and first_observed < (today - pd.Timedelta(days=30)):
            category = 'Passive Buyer'


        # 🟠 Cooling Off
        elif visits >= 1 and 31 <= days_last <= 90:
            category = 'Cooling Off'

        # 🔴 Inactive
        elif days_last > 90:
            category = 'Inactive'

        results.append({
            'property_id': property_id,
            'first_observed': first_observed,
            'last_observed': last_observed,
            'unique_obs_count': visits,
            'category': category
        })

    return pd.DataFrame(results)



def main():
    print("🚀 Fetching home shopper data...")
    df = fetch_home_shopper_data()

    print(f"🔍 Processing {len(df)} records...")
    shopper_df = classify_shoppers(df)

    if not shopper_df.empty:
        output_path = f"home_shoppers_classified_{ENV}.csv"
        shopper_df.to_csv(output_path, index=False)
        print(f"✅ Saved {len(shopper_df)} classified records to: {output_path}")
        print("\n📊 Category Breakdown:")
        print(shopper_df['category'].value_counts())
    else:
        print("✅ No home shopper records classified.")

if __name__ == "__main__":
    main()
