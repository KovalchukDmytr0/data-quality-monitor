__all__ = ['loan_officers_query', 'sales_query', 'realtors_query', 
           'loans_query', 'zebra_query', 'suspicious_realtor_patterns', 'active_buyer_completeness_report']

# Table name for zebra queries
zebra_tables = {
    'md': 'zebra_md',
    'ut': 'zebra_ut',
    'utV2': 'zebra_ut_v2',
    'utV2full': 'zebra_ut_v2_full',
    'id': 'zebra_id_v2',
    'dc': 'zebra_dс_v2',
    'de': 'zebra_de_v2',
    'pa': 'zebra_pa_v2'
}
ZEBRA_TABLE = zebra_tables['pa']

loan_officers_query = {
    # Total Counts
    'Loan Officers Count': '''SELECT COUNT(*) FROM loan_officers;''',
    
    # Data Completeness
    'Loan Officers Missing License Number': '''SELECT COUNT(*) FROM loan_officers WHERE license_number IS NULL;''',
    
    'Loan Officers Missing Name and Full Name': '''SELECT COUNT(*) FROM loan_officers WHERE name IS NULL AND full_name IS NULL;''',
    
    'Loan Officers Missing Company Name': '''SELECT COUNT(*) FROM loan_officers 
        WHERE (name IS NOT NULL OR full_name IS NOT NULL) 
        AND company_name IS NULL 
        AND company_name_2 IS NULL 
        AND verified_company_name IS NULL;''',
    
    'Loan Officers Missing Company License Number': '''SELECT COUNT(*) FROM loan_officers 
        WHERE (name IS NOT NULL OR full_name IS NOT NULL) 
        AND company_license_number IS NULL 
        AND company_license_number_2 IS NULL 
        AND verified_company_license_number IS NULL;''',
    
    # Location Data
    'Loan Officers Missing City': '''SELECT COUNT(*) FROM loan_officers WHERE city IS NULL;''',
    
    'Loan Officers Missing State': '''SELECT COUNT(*) FROM loan_officers WHERE state IS NULL;''',
    
    'Loan Officers Missing Zip': '''SELECT COUNT(*) FROM loan_officers WHERE zip_code IS NULL;''',
    
    # External Data
    'Loan Officers External ID Not Null': '''SELECT COUNT(*) FROM loan_officers WHERE external_id IS NOT NULL;'''
}

sales_query = {
    # Total Counts
    'Sales Amount': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years';''',
    
    # Agent Data Completeness
    'Sales Missed Buyer Agent': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND buyer_agent_id IS NULL;''',
    
    'Sales Missed Listing Agent': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND listing_agent_id IS NULL;''',
    
    'Sales Missed Co Listing Agent': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND listing_co_agent_id IS NULL;''',
    
    'Sales Missed Co Buyer Agent': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND buyer_co_agent_id IS NULL;''',
    
    # Transaction Data
    'Sales Missed Sale Price': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND sale_price IS NULL;''',
    
    'Sales Missed Sale Date': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND sale_date IS NULL;''',
    
    'Sales Cash Buyers Amount': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND cash_buyer = TRUE;''',
    
    # Property Data
    'Sales Missed Property ID': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND property_id IS NULL;''',
    
    'Sales Not Connected to Loan and Not Cash Buyers': '''SELECT COUNT(*) FROM sales 
        WHERE deleted = FALSE 
        AND duplicate = FALSE 
        AND sale_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND loan_id IS NULL 
        AND cash_buyer = FALSE;''',
    
    # Data Quality
    'Deleted Sales Count': '''SELECT COUNT(*) FROM sales WHERE deleted = TRUE;''',
    
    'Duplicate Sales Count': '''SELECT COUNT(*) FROM sales WHERE duplicate = TRUE;'''
}

realtors_query = {
    # Total Counts
    'Realtors Count': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE;''',
    
    # Data Completeness
    'Realtors Missing Name and Full Name': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE 
        AND name IS NULL 
        AND full_name IS NULL;''',
    
    'Realtors Missing Company Name': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE 
        AND (name IS NOT NULL OR full_name IS NOT NULL) 
        AND company_name IS NULL;''',
    
    'Realtors Missing Company License': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE 
        AND (name IS NOT NULL OR full_name IS NOT NULL) 
        AND company_license_number IS NULL;''',
    
    # Location Data
    'Realtors Missing City': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE 
        AND city IS NULL;''',
    
    'Realtors Missing State': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE 
        AND state IS NULL;''',
    
    'Realtors Missing Zip': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE 
        AND zip_code IS NULL;''',
    
    # Media Data
    'Realtors Missing Avatar URLs': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE 
        AND avatar_urls IS NULL;''',
    
    # External Data
    'Realtors Connected Users': '''SELECT COUNT(*) FROM realtors 
        WHERE duplicate IS FALSE 
        AND hidden IS FALSE 
        AND external_id IS NOT NULL;''',
    
    'Realtors Hidden Users': '''SELECT COUNT(*) FROM realtors WHERE hidden IS TRUE;'''
}

loans_query = {
    # Total Counts
    'Loans in Past 5 Years': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years';''',
    
    # Connection Data
    'Loans Not Connected to Sale': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND sale_id IS NULL;''',
    
    'Loans Sale ID Null, Loan Officer ID Not Null': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND sale_id IS NULL 
        AND loan_officer_id IS NOT NULL;''',
    
    'Loans Not Connected to Loan Officers': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND loan_officer_id IS NULL;''',
    
    # Loan Details
    'Loans Without Property ID': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND property_id IS NULL;''',
    
    'Loans Without Loan Amount': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND loan_amount IS NULL;''',
    
    'Loans Without Loan Date': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND loan_date IS NULL;''',
    
    # Loan Types
    'Loans Mortgage Type 0 Junior': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND mortgage_type = 0;''',
    
    'Loans Mortgage Type 1 Purchase Primary': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND mortgage_type = 1;''',
    
    'Loans Mortgage Type 2 Refinance Primary': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND mortgage_type = 2;''',
    
    'Equity Loans Count': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND equity_loan = TRUE;''',
    
    # Data Quality
    'Deleted Loans Count': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = TRUE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years';''',
    
    'Cash Buyer Loans Count': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND cash_buyer = TRUE;''',
    
    'Loans Owner Name Null': '''SELECT COUNT(*) FROM loans 
        WHERE deleted = FALSE 
        AND loan_date >= CURRENT_DATE - INTERVAL '5 years' 
        AND owner_name IS NULL;'''
}

zebra_query = {
    # Total Records
    'Total Records': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE};''',
    
    # Agent Data Completeness
    'Missing Listing Agent Data': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE (main_agent IS NULL OR main_agent = '' OR main_agent = 'null') 
        AND (main_agent_company IS NULL OR main_agent_company = '' OR main_agent_company = 'null') 
        AND (main_agent_license IS NULL OR main_agent_license = '' OR main_agent_license = 'null');''',
    
    'Missing Buyer Agent Data': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE (buyer_agent IS NULL OR buyer_agent = '') 
        AND (buyer_agent_company IS NULL OR buyer_agent_company = '') 
        AND (buyer_agent_license IS NULL OR buyer_agent_license = '');''',
    
    'Main Agent Company Missing': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE main_agent <> '' 
        AND main_agent_company = '';''',
    
    'Buyer Agent Company Missing': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE buyer_agent <> '' 
        AND buyer_agent_company = '';''',
    
    'Co-Agent Company Missing': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE co_agent <> '' 
        AND co_agent_company = '';''',
    
    # Photo Completeness
    'Missing Main Agent Photo': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE (main_agent <> '' OR main_agent IS NOT NULL) 
        AND (main_agent_photo = 'null' OR main_agent_photo = '' OR main_agent_photo IS NULL);''',
    
    'Missing Buyer Agent Photo': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE (buyer_agent <> '' OR buyer_agent IS NOT NULL) 
        AND (buyer_agent_photo = 'null' OR buyer_agent_photo = '' OR buyer_agent_photo IS NULL);''',
    
    'Missing Home Photo': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE home_photo = '' OR home_photo = 'null';''',
    
    # Property Data Completeness
    'Missing Sale Date': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE (sold_date IS NULL OR sold_date = '') 
        AND (property_history = '' OR property_history IS NULL);''',
    
    'Missing Sold Price': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE (sold_price IS NULL OR sold_price = '') 
        AND (property_history = '' OR property_history IS NULL);''',
    
    'Missing Property History': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE property_history = '' OR property_history IS NULL;''',
    
    'Missing Environment Factors': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE environment_factors = '' OR environment_factors IS NULL;''',
    
    'Sold Date "Not Listed for Sale"': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE sold_date ILIKE '%NOT LISTED FOR SALE%' 
        AND property_history = '';''',
    
    # Data Quality Metrics
    'Complete Records in Valuable Fields': f'''SELECT COUNT(*) FROM {ZEBRA_TABLE} 
        WHERE (buyer_agent IS NOT NULL AND buyer_agent <> '') 
        AND (buyer_agent_license IS NOT NULL AND buyer_agent_license <> '') 
        AND (main_agent_license IS NOT NULL AND buyer_agent <> '') 
        AND (main_agent IS NOT NULL AND main_agent <> '') 
        AND (sold_price IS NOT NULL AND sold_price <> '') 
        AND (sold_date IS NOT NULL AND sold_date <> '') 
        AND (property_address IS NOT NULL AND property_address <> '');''',
    
    # Unique Records Analysis
    'Unique Records (deduplicated)': f'''WITH DeduplicatedRecords AS (
            SELECT property_address, sold_date, sold_price,
                   ROW_NUMBER() OVER (
                       PARTITION BY TRIM(LOWER(property_address)), sold_date, sold_price
                       ORDER BY 
                           CASE
                               WHEN sold_date ~ '^[A-Za-z]{{3}} \\d{{1,2}}, \\d{{4}}$'
                               THEN TO_DATE(sold_date, 'Mon DD, YYYY')
                               ELSE NULL
                           END DESC
                   ) AS row_num
            FROM {ZEBRA_TABLE}
            WHERE sold_date ~ '^[A-Za-z]{{3}} \\d{{1,2}}, \\d{{4}}$'
        )
        SELECT COUNT(*) FROM DeduplicatedRecords WHERE row_num = 1;''',
    
    'Unique Agents': f'''WITH agent_data AS (
            SELECT main_agent AS agent_name, main_agent_company AS company
            FROM {ZEBRA_TABLE}
            WHERE main_agent IS NOT NULL
            UNION
            SELECT buyer_agent, buyer_agent_company
            FROM {ZEBRA_TABLE}
            WHERE buyer_agent IS NOT NULL
        )
        SELECT COUNT(DISTINCT ROW(agent_name, company)) AS unique_agents_total 
        FROM agent_data;''',
    
    'Unique License Numbers': f'''WITH agent_data AS (
            SELECT main_agent AS agent_name, main_agent_company AS company, main_agent_license AS license
            FROM {ZEBRA_TABLE}
            WHERE main_agent <> ''
            UNION
            SELECT buyer_agent, buyer_agent_company, buyer_agent_license
            FROM {ZEBRA_TABLE}
            WHERE buyer_agent <> ''
        )
        SELECT COUNT(DISTINCT license) FILTER (WHERE license <> 'null') AS unique_licenses_total
        FROM agent_data;''',
    
    'Unique Phone Numbers': f'''WITH agent_data AS (
            SELECT main_agent AS agent_name, main_agent_company AS company, main_agent_phone AS phone
            FROM {ZEBRA_TABLE}
            WHERE main_agent IS NOT NULL
            UNION
            SELECT buyer_agent, buyer_agent_company, buyer_agent_phone
            FROM {ZEBRA_TABLE}
            WHERE buyer_agent IS NOT NULL
        )
        SELECT COUNT(DISTINCT phone) FILTER (WHERE phone IS NOT NULL) AS unique_phone_total
        FROM agent_data;''',
        
    'Unique Photos': f'''WITH DeduplicatedRecords AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY TRIM(LOWER(property_address)), sold_date, sold_price
                       ORDER BY
                           CASE
                               WHEN sold_date ~ '^[A-Za-z]{{3}} \\d{{1,2}}, \\d{{4}}$'
                               THEN TO_DATE(sold_date, 'Mon DD, YYYY')
                               ELSE NULL
                           END DESC
                   ) AS row_num
            FROM {ZEBRA_TABLE}
            WHERE sold_date ~ '^[A-Za-z]{{3}} \\d{{1,2}}, \\d{{4}}$'
        ),
        AgentPhotos AS (
            SELECT TRIM(main_agent) AS agent_name
            FROM DeduplicatedRecords
            WHERE row_num = 1
              AND main_agent IS NOT NULL
              AND main_agent_photo IS NOT NULL

            UNION

            SELECT TRIM(buyer_agent) AS agent_name
            FROM DeduplicatedRecords
            WHERE row_num = 1
              AND buyer_agent IS NOT NULL
              AND buyer_agent_photo IS NOT NULL
        )
        SELECT COUNT(DISTINCT agent_name) AS unique_agents_with_photos FROM AgentPhotos;'''
}

suspicious_realtor_patterns = {
    # Test Patterns
    'Realtors Name Starts With "test"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE 'test%' OR r.full_name ILIKE 'test%');''',
    
    'Realtors Name Ends With "test"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%test' OR r.full_name ILIKE '%test');''',
    
    # Member Status Patterns
    'Realtors Name Contains "not a member"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%not a member%' OR r.full_name ILIKE '%not a member%');''',
    
    'Realtors Name Contains "out of state"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%out of state%' OR r.full_name ILIKE '%out of state%');''',
    
    # None/Non Patterns
    'Realtors Name Starts With "none"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE 'none %' OR r.full_name ILIKE 'none %');''',
    
    'Realtors Name Ends With "none"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '% none' OR r.full_name ILIKE '% none');''',
    
    'Realtors Name Ends With "non"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '% non' OR r.full_name ILIKE '% non');''',
    
    'Realtors Name Starts With "non"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE 'non %' OR r.full_name ILIKE 'non %');''',
    
    'Realtors Name Contains "nonmember"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%nonmember%' OR r.full_name ILIKE '%nonmember%');''',
    
    # Other Patterns
    'Realtors Name Starts With "other"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE 'other %' OR r.full_name ILIKE 'other %');''',
    
    'Realtors Name Contains "Member"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%Member%' OR r.full_name ILIKE '%Member%');''',
    
    'Realtors Name Contains "Nonmls"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%Nonmls%' OR r.full_name ILIKE '%Nonmls%');''',
    
    'Realtors Name Contains "Outside"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%Outside%' OR r.full_name ILIKE '%Outside%');''',
    
    # System Patterns
    'Realtors Name Contains "null"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%null %' OR r.full_name ILIKE '%null %');''',
    
    'Realtors Name Contains "AGENT"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%AGENT%' OR r.full_name ILIKE '%AGENT%');''',
    
    'Realtors Name Contains "Unidentified"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%Unidentified%' OR r.full_name ILIKE '%Unidentified%');''',
    
    'Realtors Name Contains "RMLS"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%RMLS%' OR r.full_name ILIKE '%RMLS%');''',
    
    'Realtors Name Contains "Default"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%Default%' OR r.full_name ILIKE '%Default%');''',
    
    'Realtors Name Contains "Subscriber"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%Subscriber%' OR r.full_name ILIKE '%Subscriber%');''',
    
    'Realtors Name Contains "NON-MBR"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%NON-MBR%' OR r.full_name ILIKE '%NON-MBR%');''',
    
    'Realtors Name Contains "Represented"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%Represented%' OR r.full_name ILIKE '%Represented%');''',
    
    'Realtors Name Contains "listing"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%listing%' OR r.full_name ILIKE '%listing%');''',
    
    'Realtors Name Contains "Participant"': '''SELECT COUNT(*) FROM realtors r 
        WHERE r.duplicate = FALSE 
        AND r.hidden = FALSE 
        AND (r.name ILIKE '%Participant%' OR r.full_name ILIKE '%Participant%');'''
}

active_buyer_completeness_report = {
    # Total Counts
    'Total Active Home Shoppers': '''SELECT COUNT(*) FROM properties 
        WHERE home_shopper_active = TRUE;''',
    
    # Owner Occupancy Status
    'Owner Occupied: True': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (last_entry.elem ->> 'owner_occupied')::boolean = TRUE;''',
    
    'Owner Occupied: False': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (last_entry.elem ->> 'owner_occupied')::boolean = FALSE;''',
    
    # Shopper Location Analysis
    'In-State Shopper Only': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE
        AND (last_entry.elem ->> 'in_state_shopper')::boolean = TRUE
        AND (last_entry.elem ->> 'out_of_state_shopper')::boolean = FALSE;''',
    
    'Out-of-State Shopper Only': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE
        AND (last_entry.elem ->> 'in_state_shopper')::boolean = FALSE
        AND (last_entry.elem ->> 'out_of_state_shopper')::boolean = TRUE;''',
    
    'Both In-State and Out-of-State Shopper': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE
        AND (last_entry.elem ->> 'in_state_shopper')::boolean = TRUE
        AND (last_entry.elem ->> 'out_of_state_shopper')::boolean = TRUE;''',
    
    # Visit Frequency Analysis
    'Visits > 1': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (last_entry.elem ->> 'unique_obs_count')::int > 1;''',
    
    'Visits > 3': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (last_entry.elem ->> 'unique_obs_count')::int > 3;''',
    
    'Visits > 5': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (last_entry.elem ->> 'unique_obs_count')::int > 5;''',
    
    'Visits > 7': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (last_entry.elem ->> 'unique_obs_count')::int > 7;''',
    
    'Visits > 9': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (last_entry.elem ->> 'unique_obs_count')::int > 9;''',
    
    # Recent Activity Analysis
    'Last Visit Within 10 Days': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '10 days') 
          OR (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '10 days')
        );''',
    
    'Last Visit Within 30 Days': '''SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = TRUE 
        AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '30 days') 
          OR (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '30 days')
        );'''
}

agents_monthly_testing = REALTORS_QUERY = {
    'Sarah Turner': """
        SELECT id FROM realtors
        WHERE (name = 'Sarah Turner' OR full_name = 'Sarah Turner')
          AND company_name = 'SB Dev Corp'
          AND duplicate = false;
    """,

    "Tanya O'neil": """
        SELECT id FROM realtors
        WHERE (name = 'Tanya O''neil' OR full_name = 'Tanya O''neil')
          AND company_name = 'Great Western Realty'
          AND duplicate = false;
    """,

    'Todd Levitt': """
        SELECT id FROM realtors
        WHERE (name = 'Todd Levitt' OR full_name = 'Todd Levitt')
          AND company_name = 'Black Mountain Valley Realty'
          AND duplicate = false;
    """,

    'The Limbird Team': """
        SELECT id FROM realtors
        WHERE (name = 'The Limbird Team' OR full_name = 'The Limbird Team')
          AND company_name = 'Limbird Real Estate Group'
          AND duplicate = false;
    """,

    'Catmy Bui': """
        SELECT id FROM realtors
        WHERE (name = 'Catmy Bui' OR full_name = 'Catmy Bui')
          AND company_name = 'Providential Investments'
          AND duplicate = false;
    """,

    'David Butler': """
        SELECT id FROM realtors
        WHERE (name = 'David Butler' OR full_name = 'David Butler')
          AND company_name = 'Real Broker'
          AND duplicate = false;
    """,

    'Premier Group': """
        SELECT id FROM realtors
        WHERE (name = 'Premier Group' OR full_name = 'Premier Group')
          AND company_name = 'Premier Realty Group'
          AND duplicate = false;
    """,

    'CARRIE LINGO': """
        SELECT id FROM realtors
        WHERE (name = 'CARRIE LINGO' OR full_name = 'CARRIE LINGO')
          AND company_name = 'Jack Lingo - Lewes'
          AND duplicate = false;
    """,

    'Lynn Harris': """
        SELECT id FROM realtors
        WHERE (name = 'Lynn Harris' OR full_name = 'Lynn Harris')
          AND company_name = 'ERA American Suncoast Realty'
          AND duplicate = false;
    """,

    'Emily Hanley': """
        SELECT id FROM realtors
        WHERE (name = 'Emily Hanley' OR full_name = 'Emily Hanley')
          AND company_name = 'Bolst, Inc.'
          AND duplicate = false;
    """,

    'Bryn W Kaufman': """
        SELECT id FROM realtors
        WHERE (name = 'Bryn W Kaufman' OR full_name = 'Bryn W Kaufman')
          AND company_name = 'OahuRE.com'
          AND duplicate = false;
    """,

    'ERIKA LOPEZ': """
        SELECT id FROM realtors
        WHERE (name = 'ERIKA LOPEZ' OR full_name = 'ERIKA LOPEZ')
          AND company_name = 'HUBBLE HOMES, LLC'
          AND duplicate = false;
    """,

    'Kristine Nalley': """
        SELECT id FROM realtors
        WHERE (name = 'Kristine Nalley' OR full_name = 'Kristine Nalley')
          AND company_name = 'Fayette County Real Estate'
          AND duplicate = false;
    """,

    'Mike Mccatty': """
        SELECT id FROM realtors
        WHERE (name = 'Mike Mccatty' OR full_name = 'Mike Mccatty')
          AND company_name = 'Century 21 Circle'
          AND duplicate = false;
    """,

    'Jon Fisher': """
        SELECT id FROM realtors
        WHERE (name = 'Jon Fisher' OR full_name = 'Jon Fisher')
          AND company_name = 'MR. LANDMAN'
          AND duplicate = false;
    """,

    'Ella Carter': """
        SELECT id FROM realtors
        WHERE (name = 'Ella Carter' OR full_name = 'Ella Carter')
          AND company_name = 'KELLER WILLIAMS REALTY'
          AND duplicate = false;
    """,

    'Bart W Medlock': """
        SELECT id FROM realtors
        WHERE (name = 'Bart W Medlock' OR full_name = 'Bart W Medlock')
          AND company_name = 'Re/max First'
          AND duplicate = false;
    """,

    'DONAVON REDMON': """
        SELECT id FROM realtors
        WHERE (name = 'DONAVON REDMON' OR full_name = 'DONAVON REDMON')
          AND company_name = 'STEVEN SEALE PROPERTIES, LLC'
          AND duplicate = false;
    """,

    'Erin Proulx': """
        SELECT id FROM realtors
        WHERE (name = 'Erin Proulx' OR full_name = 'Erin Proulx')
          AND company_name = 'KW Coastal and Lakes & Mountains Realty/Portsmouth'
          AND duplicate = false;
    """,

    'Jason Forry': """
        SELECT id FROM realtors
        WHERE (name = 'Jason Forry' OR full_name = 'Jason Forry')
          AND company_name = 'Cummings & Co. Realtors'
          AND duplicate = false;
    """,

    'Celeste Fournier': """
        SELECT id FROM realtors
        WHERE (name = 'Celeste Fournier' OR full_name = 'Celeste Fournier')
          AND company_name = 'Blu Sky Real Estate'
          AND duplicate = false;
    """,

    'Pamela Riley': """
        SELECT id FROM realtors
        WHERE (name = 'Pamela Riley' OR full_name = 'Pamela Riley')
          AND company_name = 'Robertson Brothers Company'
          AND duplicate = false;
    """,

    'Leonard Chute': """
        SELECT id FROM realtors
        WHERE (name = 'Leonard Chute' OR full_name = 'Leonard Chute')
          AND company_name = 'Edina Realty, Corp. - Siren'
          AND duplicate = false;
    """,

    'Courtney Kattengell': """
        SELECT id FROM realtors
        WHERE (name = 'Courtney Kattengell' OR full_name = 'Courtney Kattengell')
          AND company_name = 'TCK REALTY LLC'
          AND duplicate = false;
    """,

    'Ronnie Cline': """
        SELECT id FROM realtors
        WHERE (name = 'Ronnie Cline' OR full_name = 'Ronnie Cline')
          AND company_name = 'Poplar Bluff Realty Inc'
          AND duplicate = false;
    """,

    'Ehren Alessi': """
        SELECT id FROM realtors
        WHERE (name = 'Ehren Alessi' OR full_name = 'Ehren Alessi')
          AND company_name = 'Life Realty District'
          AND duplicate = false;
    """,

    'Aaron Taylor': """
        SELECT id FROM realtors
        WHERE (name = 'Aaron Taylor' OR full_name = 'Aaron Taylor')
          AND company_name = 'Exp Realty'
          AND duplicate = false;
    """,

    'Kameron L. Kildea': """
        SELECT id FROM realtors
        WHERE (name = 'Kameron L. Kildea' OR full_name = 'Kameron L. Kildea')
          AND company_name = 'Coldwell Banker Premier'
          AND duplicate = false;
    """,

    'ROBERT DEKANSKI': """
        SELECT id FROM realtors
        WHERE (name = 'ROBERT DEKANSKI' OR full_name = 'ROBERT DEKANSKI')
          AND company_name = 'RE/MAX 1st ADVANTAGE'
          AND duplicate = false;
    """,

    'Susan Flores': """
        SELECT id FROM realtors
        WHERE (name = 'Susan Flores' OR full_name = 'Susan Flores')
          AND company_name = 'Keller Williams Realty'
          AND duplicate = false;
    """,

    'David L. White': """
        SELECT id FROM realtors
        WHERE (name = 'David L. White' OR full_name = 'David L. White')
          AND company_name = 'Owner Entry.com'
          AND duplicate = false;
    """,

    'LINDA K. BECK': """
        SELECT id FROM realtors
        WHERE (name = 'LINDA K. BECK' OR full_name = 'LINDA K. BECK')
          AND company_name = 'ALLEN TATE HIGH POINT'
          AND duplicate = false;
    """,

    'Joanna Hvezda': """
        SELECT id FROM realtors
        WHERE (name = 'Joanna Hvezda' OR full_name = 'Joanna Hvezda')
          AND company_name = 'Real Estate by Jo, LLC'
          AND duplicate = false;
    """,

    'Junior E Miller': """
        SELECT id FROM realtors
        WHERE (name = 'Junior E Miller' OR full_name = 'Junior E Miller')
          AND company_name = 'Kaufman Realty & Auction, LLC'
          AND duplicate = false;
    """,

    'Renee A Musat': """
        SELECT id FROM realtors
        WHERE (name = 'Renee A Musat' OR full_name = 'Renee A Musat')
          AND company_name = 'Keller Williams Citywide'
          AND duplicate = false;
    """,

    'KIM BRUCE': """
        SELECT id FROM realtors
        WHERE (name = 'KIM BRUCE' OR full_name = 'KIM BRUCE')
          AND company_name = 'GOLDWINGS REAL ESTATE GROUP'
          AND duplicate = false;
    """,

    'Doreen Walters': """
        SELECT id FROM realtors
        WHERE (name = 'Doreen Walters' OR full_name = 'Doreen Walters')
          AND company_name = 'Realty One Group Gold Standard'
          AND duplicate = false;
    """,

    'Trudy Arthur': """
        SELECT id FROM realtors
        WHERE (name = 'Trudy Arthur' OR full_name = 'Trudy Arthur')
          AND company_name = 'Lowcountry Real Estate, Inc.'
          AND duplicate = false;
    """,

    'Barb Maxon': """
        SELECT id FROM realtors
        WHERE (name = 'Barb Maxon' OR full_name = 'Barb Maxon')
          AND company_name = 'Century 21 ProLink'
          AND duplicate = false;
    """,

    'Sophia Chase': """
        SELECT id FROM realtors
        WHERE (name = 'Sophia Chase' OR full_name = 'Sophia Chase')
          AND company_name = 'KW Cleveland'
          AND duplicate = false;
    """,

    'Jamie McMartin': """
        SELECT id FROM realtors
        WHERE (name = 'Jamie McMartin' OR full_name = 'Jamie McMartin')
          AND company_name = 'Compass RE Texas, LLCKaty'
          AND duplicate = false;
    """,

    'Laura Johnston': """
        SELECT id FROM realtors
        WHERE (name = 'Laura Johnston' OR full_name = 'Laura Johnston')
          AND company_name = 'RE/MAX DOWNTOWN'
          AND duplicate = false;
    """,

    'Stacie Callan': """
        SELECT id FROM realtors
        WHERE (name = 'Stacie Callan' OR full_name = 'Stacie Callan')
          AND company_name = 'MRC'
          AND duplicate = false;
    """,

    'AMANDA TUCKER': """
        SELECT id FROM realtors
        WHERE (name = 'AMANDA TUCKER' OR full_name = 'AMANDA TUCKER')
          AND company_name = 'CAROLINA TRIAD CHOICE REALTY'
          AND duplicate = false;
    """,

    'KATY MASON': """
        SELECT id FROM realtors
        WHERE (name = 'KATY MASON' OR full_name = 'KATY MASON')
          AND company_name = 'SILVERCREEK REALTY GROUP'
          AND duplicate = false;
    """,

    'Julie L Clark': """
        SELECT id FROM realtors
        WHERE (name = 'Julie L Clark' OR full_name = 'Julie L Clark')
          AND company_name = 'BHHS Professional Realty'
          AND duplicate = false;
    """,

    'Mary Wallace': """
        SELECT id FROM realtors
        WHERE (name = 'Mary Wallace' OR full_name = 'Mary Wallace')
          AND company_name = 'Coldwell Banker Realty'
          AND duplicate = false;
    """,

    'Bill Hays': """
        SELECT id FROM realtors
        WHERE (name = 'Bill Hays' OR full_name = 'Bill Hays')
          AND company_name = 'RE/MAX Preferred'
          AND duplicate = false;
    """
}







