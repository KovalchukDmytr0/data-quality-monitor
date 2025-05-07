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

# --- SQL Queries ---
QUERIES = {
    'Loans in Past 5 Years': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years';''',
    'Loans Not Connected to Sale': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and sale_id is null;''',
    'Loans Sale ID Null, Loan Officer ID Not Null': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and sale_id is null and loan_officer_id is not null;''',
    'Loans Not Connected to Loan Officers': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and loan_officer_id is  null;''',
    'Loans Without Property ID': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and property_id  is  null;''',
    'Loans Without Loan Amount': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and loan_amount  is  null;''',
    'Loans Without Loan Date': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and loan_date  is  null;''',
    'Loans Mortgage Type 0 Junior': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and mortgage_type = 0;''',
    'Loans Mortgage Type 1 Purchase Primary': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and mortgage_type = 1;''',
    'Loans Mortgage Type 2 Refinance Primary': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and mortgage_type = 2;''',
    'Equity Loans Count': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and equity_loan  = true;''',
    'Deleted Loans Count': '''SELECT COUNT(*) FROM loans WHERE deleted = true AND loan_date >= CURRENT_DATE - INTERVAL '5 years';''',
    'Cash Buyer Loans Count': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and cash_buyer = true;''',
    'Loans Owner Name Null': '''SELECT COUNT(*) FROM loans WHERE deleted = false AND loan_date >= CURRENT_DATE - INTERVAL '5 years' and owner_name  is null;''',
}

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

def main():
    results = []
    max_threads = min(15, len(QUERIES))

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(run_query_with_connection, item) for item in QUERIES.items()]
        for future in as_completed(futures):
            results.append(future.result())

    with open(f"loans_counts_{ENV}.csv", mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Query Description", "Count"])
        writer.writerows(results)

    print(f"✅ All counts saved to 'loans_counts_{ENV}.csv'")

if __name__ == "__main__":
    main()
