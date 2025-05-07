import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import csv
import boto3
import psycopg2
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from sql_queries.data_completeness_queries import loan_officers_query, sales_query, realtors_query, loans_query

# Load .env file
load_dotenv()

# Set ENV manually: 'dev', 'stage', 'prod'
ENV = 'prod'

# Read credentials dynamically from env variables
REGION = os.getenv('REGION')
DB_HOST = os.getenv(f'{ENV.upper()}_DB_HOST')
DB_NAME = os.getenv(f'{ENV.upper()}_DB_NAME')
DB_USER = os.getenv(f'{ENV.upper()}_DB_USER')

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv(f'{ENV.upper()}_AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(f'{ENV.upper()}_AWS_SECRET_ACCESS_KEY')
os.environ['AWS_SESSION_TOKEN'] = os.getenv(f'{ENV.upper()}_AWS_SESSION_TOKEN')

# --- Get IAM Token ---
def get_iam_token():
    client = boto3.client('rds', region_name=REGION)
    return client.generate_db_auth_token(
        DBHostname=DB_HOST,
        Port=5432,
        DBUsername=DB_USER
    )

# --- Connect to DB (1 per thread) ---
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=get_iam_token(),
        port=5432,
        sslmode='require'
    )

# --- SQL Queries ---
QUERIES = sales_query

# Determine output filename based on query type
query_type = 'realtor' if QUERIES == realtors_query else 'loan_officer' if QUERIES == loan_officers_query else 'loan' if QUERIES == loans_query else 'sale'
output_filename = f"{query_type}_counts_{ENV}.csv"

# --- Run Query Using Shared DB Connection ---
def run_query_with_connection(name_query_tuple):
    name, query = name_query_tuple
    try:
        conn = connect_db()
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()[0]
        conn.close()
        return (name, result)
    except Exception as e:
        return (name, f"Error: {e}")

# --- Main ---
def main():
    results = []
    max_threads = min(15, len(QUERIES))  # Use up to 10 parallel threads

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(run_query_with_connection, item) for item in QUERIES.items()]
        for future in as_completed(futures):
            results.append(future.result())

    # Save to CSV
    with open(output_filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Query Description", "Count"])
        writer.writerows(results)

    print(f"✅ All counts saved to '{output_filename}'")

if __name__ == "__main__":
    main()
