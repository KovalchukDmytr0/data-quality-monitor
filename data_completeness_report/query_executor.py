import os
import sys
import boto3
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure module paths work when running as script
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_queries.data_completeness_queries import (
    loan_officers_query, sales_query, realtors_query, loans_query, zebra_query, 
    suspicious_realtor_patterns
)

# Load environment variables
load_dotenv()
ENV = 'PROD' # 👈 Change to prod, dev, or stage as needed

REGION = os.getenv('REGION')
DB_HOST = os.getenv(f'{ENV.upper()}_DB_HOST')
DB_NAME = os.getenv(f'{ENV.upper()}_DB_NAME')
DB_USER = os.getenv(f'{ENV.upper()}_DB_USER')

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv(f'{ENV.upper()}_AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(f'{ENV.upper()}_AWS_SECRET_ACCESS_KEY')
os.environ['AWS_SESSION_TOKEN'] = os.getenv(f'{ENV.upper()}_AWS_SESSION_TOKEN')

# Choose which query set to use
QUERIES = suspicious_realtor_patterns  # 👈 Change to loans_query, etc. as needed

# Infer query type for filename
query_type = (
    'realtor' if QUERIES == realtors_query else 
    'loan_officer' if QUERIES == loan_officers_query else
    'loan' if QUERIES == loans_query else
    'zebra' if QUERIES == zebra_query else
    'suspicious_realtor_patterns' if QUERIES == suspicious_realtor_patterns else
    'sale'
)
output_filename = f"{query_type}_counts_{ENV}.csv"

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

def run_query(name_query_tuple):
    name, query = name_query_tuple
    try:
        with connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()[0]
        return {'Query Description': name, 'Count': result}
    except Exception as e:
        return {'Query Description': name, 'Count': f"Error: {str(e)}"}

def main():
    max_threads = min(30, len(QUERIES))
    results = []

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(run_query, item) for item in QUERIES.items()]
        for future in as_completed(futures):
            results.append(future.result())

    # Convert results to a DataFrame
    df = pd.DataFrame(results)

    # Save DataFrame to CSV
    df.to_csv(output_filename, index=False)
    print(f"✅ All counts saved to '{output_filename}'")

if __name__ == "__main__":
    main()
