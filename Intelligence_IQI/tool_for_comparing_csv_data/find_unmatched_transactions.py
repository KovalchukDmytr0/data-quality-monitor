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
    csv_path = '/Users/dmytrokovalchuk/Desktop/homeIQ/query_executor/Michael Horwitz - carrie_lingo.csv'
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

    df = df[required_cols + [col for col in df.columns if col.strip().lower() == 'clip']]
    df['State'] = df['State'].apply(normalize_state)
    df = df.dropna(subset=required_cols)

    log.info("🧠 Building WHERE clauses...")
    conditions = []
    for _, row in df.iterrows():
        city = row['City'].strip().lower()
        zip_code = str(row['Zip Code']).strip()
        street = row['Address'].strip().lower()
        clause = f"(city ILIKE '%%{city}%%' AND zip_code = '{zip_code}' AND street_address ILIKE '%%{street}%%')"
        conditions.append(clause)

    state_list = "', '".join(sorted(set(df['State'].dropna())))
    state_filter = f"state IN ('{state_list}')"
    where_clause = " OR\n    ".join(conditions)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # === 🧾 First Query - Property Matching
    property_query = f"""
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
    property_query_file = f"executed_property_query_{timestamp}.sql"
    with open(property_query_file, 'w') as f:
        f.write(property_query)

    # === 🧾 Second Query - Sales Info by Clip
    clip_column = next((col for col in df.columns if col.strip().lower() == 'clip'), None)
    if not clip_column:
        log.error("❌ No 'clip' column found in CSV. Cannot proceed with sales query.")
        return

    clips = df[clip_column].dropna().astype(str).str.strip().unique()
    if len(clips) == 0:
        log.warning("⚠️ No valid clip values found in CSV for sales query.")
        return

    clip_str = ', '.join(f"'{c}'" for c in clips)

    sales_query = f"""
        SELECT s.duplicate, s.deleted,
               s.listing_agent_id, s.listing_co_agent_id,
               s.buyer_agent_id, s.buyer_co_agent_id,
               CONCAT(p.street_address,
                   CASE WHEN p.unit_number IS NOT NULL AND p.unit_number <> '' THEN ' ' || p.unit_number ELSE '' END,
                   ', ', p.city, ', ', p.state, ' ', p.zip_code
               ) AS full_address,
               s.sale_date, s.sale_price, s.id AS sale_id,
               p.clip AS property_clip, s.clip AS sale_clip, p.id
        FROM sales s
        INNER JOIN properties p ON p.id = s.property_id
        WHERE s.clip IN ({clip_str})
        ORDER BY p.street_address DESC;
    """
    sales_query_file = f"executed_sales_query_{timestamp}.sql"
    with open(sales_query_file, 'w') as f:
        f.write(sales_query)

    log.info(f"📄 Property query saved: {os.path.abspath(property_query_file)}")
    log.info(f"📄 Sales query saved: {os.path.abspath(sales_query_file)}")

    # === 🧪 DB Execution
    try:
        with connect_db() as conn:
            with conn.cursor() as cur:
                # Property query execution
                cur.execute(property_query)
                df_props = pd.DataFrame(cur.fetchall(), columns=['property_id', 'full_address'])
                prop_csv = f"matched_properties_by_address_{timestamp}.csv"
                abs_prop_csv = os.path.abspath(prop_csv)
                df_props.to_csv(abs_prop_csv, index=False)
                log.info(f"✅ Property CSV saved at: {abs_prop_csv}")
                print(f"\n✅ Property CSV saved at: {abs_prop_csv}")

                # Show property SQL path in console
                abs_property_query_path = os.path.abspath(property_query_file)
                print(f"\n📄 Property SQL query saved to: {abs_property_query_path}")

                # Sales query execution
                cur.execute(sales_query)
                sales_columns = [
                    'duplicate', 'deleted', 'listing_agent_id', 'listing_co_agent_id',
                    'buyer_agent_id', 'buyer_co_agent_id', 'full_address', 'sale_date',
                    'sale_price', 'sale_id', 'property_clip', 'sale_clip', 'property_id'
                ]
                df_sales = pd.DataFrame(cur.fetchall(), columns=sales_columns)
                sales_csv = f"matched_sales_by_clip_{timestamp}.csv"
                abs_sales_csv = os.path.abspath(sales_csv)
                df_sales.to_csv(abs_sales_csv, index=False)
                log.info(f"✅ Sales CSV saved at: {abs_sales_csv}")
                print(f"\n✅ Sales CSV saved at: {abs_sales_csv}")

                # Show sales SQL path in console
                abs_sales_query_path = os.path.abspath(sales_query_file)
                print(f"\n📄 Sales SQL query saved to: {abs_sales_query_path}")

    except Exception as e:
        log.error(f"❌ Failed DB query execution: {e}")


if __name__ == "__main__":
    main()
