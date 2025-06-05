import os
import sys
import logging
import pandas as pd
import psycopg2
import boto3
from datetime import datetime
from dotenv import load_dotenv

# === 📝 LOGGING SETUP ===
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bulk_address_query_log.txt", mode='w')
    ]
)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# === 🔐 ENV CONFIGURATION ===
load_dotenv()
ENV = 'lab'
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

# === 📘 US STATE NORMALIZATION ===
STATE_ABBREVIATIONS = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
    'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
    'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID', 'ILLINOIS': 'IL',
    'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA',
    'MAINE': 'ME', 'MARYLAND': 'MD', 'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI',
    'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS', 'MISSOURI': 'MO', 'MONTANA': 'MT',
    'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND',
    'OHIO': 'OH', 'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA',
    'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC', 'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN',
    'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA',
    'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY'
}

def normalize_state(state_name):
    return STATE_ABBREVIATIONS.get(state_name.strip().upper(), state_name.strip().upper())

# === 🚀 MAIN EXECUTION ===
def main():
    log.info("📥 Loading CSV...")
    csv_path = '/Users/dmytrokovalchuk/Desktop/homeIQ/query_executor/Agent investigations - Michael Horwitz (2025-05-29)_ unmatched_2.csv'
    if not os.path.exists(csv_path):
        log.error(f"❌ File not found: {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        log.error(f"❌ Failed to load CSV: {e}")
        return

    required_cols = ['Address', 'City', 'State', 'Zip Code']
    for col in required_cols:
        if col not in df.columns:
            log.error(f"❌ Missing required column: {col}")
            return

    df = df[required_cols].dropna()
    df['State'] = df['State'].apply(normalize_state)

    log.info("🧠 Building WHERE clauses...")
    conditions = []
    for _, row in df.iterrows():
        city = row['City'].strip().lower()
        zip_code = str(row['Zip Code']).strip()
        state = row['State'].strip()
        street = row['Address'].strip().lower()

        clause = f"(city ILIKE '%%{city}%%' AND zip_code = '{zip_code}' AND street_address ILIKE '%%{street}%%')"
        conditions.append(clause)

    state_list = "', '".join(sorted(set(df['State'].dropna())))
    state_filter = f"state IN ('{state_list}')"
    where_clause = " OR\n    ".join(conditions)

    final_query = f"""
        SELECT id AS property_id,
               CONCAT(p.street_address,
                   CASE WHEN p.unit_number IS NOT NULL AND p.unit_number <> '' THEN ' ' || p.unit_number ELSE '' END,
                   ', ', p.city, ', ', p.state, ' ', p.zip_code) AS full_address
        FROM properties p
        WHERE {state_filter}
          AND (
            {where_clause}
          );
    """

    # Log and print query
    log.info("📄 Final Query to Execute:\n" + final_query)
    print("\n📄 Final Query to Execute:\n" + final_query)

    # === 💾 Run Query and Save Results ===
    try:
        with connect_db() as conn:
            with conn.cursor() as cur:
                # Save query to text file
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                query_file = f"executed_sql_query_{timestamp}.sql"
                abs_query_path = os.path.abspath(query_file)
                with open(abs_query_path, 'w') as f:
                    f.write(final_query)

                log.info(f"📝 SQL query saved to: {abs_query_path}")
                print(f"\n📝 SQL query saved to: {abs_query_path}")

                # Execute query
                cur.execute(final_query)
                data = cur.fetchall()

                df_result = pd.DataFrame(data, columns=['property_id', 'full_address'])

                out_file = f"matched_properties_by_address_{timestamp}.csv"
                abs_out_path = os.path.abspath(out_file)

                df_result.to_csv(abs_out_path, index=False)

                log.info("✅ DONE")
                log.info(f"📄 Output file saved at: {abs_out_path}")
                print(f"\n📄 Output file saved at: {abs_out_path}\n")

    except Exception as e:
        log.error(f"❌ Failed DB query execution: {e}")

if __name__ == "__main__":
    main()
