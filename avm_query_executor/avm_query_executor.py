import os
import sys
import json
import boto3
import psycopg2
import pandas as pd
from dotenv import load_dotenv

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

def fetch_properties_with_avm():
    query = "SELECT id, avm_history FROM properties;"
    with connect_db() as conn:
        df = pd.read_sql_query(query, conn)
    return df

import pandas as pd

import pandas as pd

def analyze_disparity(df, threshold=0.2):
    """
    Analyze property AVM disparities.

    Parameters:
        df (pd.DataFrame): DataFrame with `id` and `avm_history`.
        threshold (float): Minimum % difference to flag, e.g. 0.3 = 30%

    Returns:
        pd.DataFrame: Unique property records with highest disparities.
    """
    flagged = []

    for _, row in df.iterrows():
        property_id = row['id']
        avm_entries = row['avm_history']

        if not isinstance(avm_entries, list):
            continue

        for entry in avm_entries:
            zillow = entry.get('zillow')
            corelogic = entry.get('corelogic')
            month = entry.get('month')

            if zillow is not None and corelogic is not None and month:
                try:
                    zillow = float(zillow)
                    corelogic = float(corelogic)
                    diff_ratio = abs(zillow - corelogic) / max(zillow, corelogic)

                    if diff_ratio > threshold:
                        if zillow > corelogic:
                            higher = 'zillow'
                        elif corelogic > zillow:
                            higher = 'corelogic'
                        else:
                            higher = 'equal'

                        flagged.append({
                            'property_id': property_id,
                            'month': month,
                            'zillow': zillow,
                            'corelogic': corelogic,
                            'disparity_pct': round(diff_ratio * 100, 2),
                            'higher_value_source': higher
                        })
                except (ValueError, ZeroDivisionError):
                    continue

    results_df = pd.DataFrame(flagged)

    if not results_df.empty:
        results_df['month'] = pd.to_datetime(results_df['month'])
        results_df = results_df.sort_values(by='month', ascending=False)
        results_df = results_df.drop_duplicates(subset='property_id', keep='first')

        zillow_bigger_count = results_df[results_df['higher_value_source'] == 'zillow']['property_id'].nunique()
        corelogic_bigger_count = results_df[results_df['higher_value_source'] == 'corelogic']['property_id'].nunique()

        print(f"📊 Unique properties where Zillow is higher: {zillow_bigger_count}")
        print(f"📊 Unique properties where CoreLogic is higher: {corelogic_bigger_count}")
        print(f"📦 Total unique properties with >{int(threshold * 100)}% disparity: {len(results_df)}")
    else:
        print(f"✅ No disparities greater than {int(threshold * 100)}% found.")

    return results_df







def main():
    print("🚀 Fetching data from DB...")
    properties_df = fetch_properties_with_avm()
    print(f"🔍 Analyzing {len(properties_df)} property records...")

    results_df = analyze_disparity(properties_df)

    if not results_df.empty:
        results_df.to_csv(f"avm_disparity_{ENV}.csv", index=False)
        print(f"✅ Saved {len(results_df)} discrepancies to 'avm_disparity_{ENV}.csv'")
    else:
        print("✅ No AVM disparities over 50% found.")

if __name__ == "__main__":
    main()
