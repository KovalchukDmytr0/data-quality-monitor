import os
import csv
import boto3
import psycopg2
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load .env file
load_dotenv()

# Set ENV manually: 'dev', 'stage', 'prod'
ENV = 'dev'

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
QUERIES = {
    'Realtors Count': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false;''',
    'Realtors Missing Name and Full Name': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false and name is null and full_name is null;''',
    'Realtors Missing Company Name': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false and (name is not null OR full_name is not null) and company_name is null;''',
    'Realtors Missing Company License': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false and (name is not null OR full_name is not null) and company_license_number  is null;''',
    'Realtors Missing City': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false and city is null;''',
    'Realtors Missing State': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false and state is null;''',
    'Realtors Missing Zip': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false and zip_code is null;''',
    'Realtors Missing Avatar URLs': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false and avatar_urls  is null;''',
    'Realtors Connected Users': '''SELECT COUNT(*) FROM realtors WHERE duplicate IS FALSE AND hidden IS false and external_id   is not null;''',
    'Realtors Hidden Users': '''SELECT COUNT(*) FROM realtors WHERE hidden is true;''',
}

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
    max_threads = min(15, len(QUERIES))  # Use up to 15 parallel threads

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(run_query_with_connection, item) for item in QUERIES.items()]
        for future in as_completed(futures):
            results.append(future.result())

    # Save to CSV
    with open(f"realtor_counts_{ENV}.csv", mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Query Description", "Count"])
        writer.writerows(results)

    print(f"✅ All counts saved to 'realtor_counts_{ENV}.csv'")

if __name__ == "__main__":
    main()
