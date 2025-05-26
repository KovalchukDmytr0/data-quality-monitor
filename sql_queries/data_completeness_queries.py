__all__ = ['loan_officers_query', 'sales_query', 'realtors_query', 
           'loans_query', 'zebra_query', 'suspicious_realtor_patterns', 'active_buyer_completeness_report']

# Table name for zebra queries
zebra_tables = {
    'md': 'zebra_md',
    'ut': 'zebra_ut',
    'utV2': 'zebra_ut_v2',
    'utV2full': 'zebra_ut_v2_full'
}
ZEBRA_TABLE = zebra_tables['utV2full']

loan_officers_query = {
    'Loan Officers Count': '''select COUNT(*) FROM loan_officers;''',
    'Loan Officers Missing License Number': '''select COUNT(*) FROM loan_officers where license_number is null;''',
    'Loan Officers Missing Name and Full Name': '''select COUNT(*) FROM loan_officers where name is  null and full_name is null;''',
    'Loan Officers Missing Company Name': '''select COUNT(*) FROM loan_officers where (name is not null OR full_name is not null) and company_name is null and company_name_2 is null and verified_company_name is null;''',
    'Loan Officers Missing Company License Number': '''select COUNT(*) FROM loan_officers where (name is not null OR full_name is not null) and company_license_number  is null and company_license_number_2  is null and verified_company_license_number  is null;''',
    'Loan Officers Missing City': '''select COUNT(*) FROM loan_officers where city is null;''',
    'Loan Officers Missing State': '''select COUNT(*) FROM loan_officers where state is null;''',
    'Loan Officers Missing Zip': '''select COUNT(*) FROM loan_officers where zip_code  is null;''',
    'Loan Officers External ID Not Null': '''select COUNT(*) FROM loan_officers where external_id is not null;''',
}

sales_query = {
    'Sales Amount': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years';''',
    'Sales Missed Buyer Agent': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and buyer_agent_id is null;''',
    'Sales Missed Listing Agent': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and listing_agent_id  is null;''',
    'Sales Missed Co Listing Agent': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and listing_co_agent_id   is null;''',
    'Sales Missed Co Buyer Agent': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and buyer_co_agent_id is null;''',
    'Sales Missed Sale Price': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and sale_price  is null;''',
    'Sales Missed Sale Date': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and sale_date  is null;''',
    'Sales Cash Buyers Amount': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and cash_buyer = true;''',
    'Sales Missed Property ID': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and property_id  is null;''',
    'Sales Not Connected to Loan and Not Cash Buyers': '''select COUNT(*) from sales where deleted  = false and duplicate = false AND sale_date  >= CURRENT_DATE - INTERVAL '5 years' and loan_id is null and cash_buyer = false;''',
    'Deleted Sales Count': '''select COUNT(*) from sales where deleted  = true;''',
    'Duplicate Sales Count': '''select COUNT(*) from sales where duplicate  = true;''',
}

realtors_query = {
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

loans_query = {
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

zebra_query = {
    'Total Records': f'SELECT COUNT(*) FROM {ZEBRA_TABLE};',
    'Missing Listing Agent Data': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE (main_agent IS NULL OR main_agent = \'\' OR main_agent = \'null\') AND (main_agent_company IS NULL OR main_agent_company = \'\' OR main_agent_company = \'null\') AND (main_agent_license IS NULL OR main_agent_license = \'\' OR main_agent_license = \'null\');',
    'Missing Sale Date': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE (sold_date IS NULL OR sold_date = \'\') AND (property_history = \'\' OR property_history IS NULL);',
    'Missing Sold Price': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE (sold_price IS NULL OR sold_price = \'\') AND (property_history = \'\' OR property_history IS NULL);',
    'Complete Records in Valuable Fields': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE (buyer_agent IS NOT NULL AND buyer_agent <> \'\') AND (buyer_agent_license IS NOT NULL AND buyer_agent_license <> \'\') AND (main_agent_license IS NOT NULL AND buyer_agent <> \'\') AND (main_agent IS NOT NULL AND main_agent <> \'\') AND (sold_price IS NOT NULL AND sold_price <> \'\') AND (sold_date IS NOT NULL AND sold_date <> \'\') AND (property_address IS NOT NULL AND property_address <> \'\');',
    'Unique Records (deduplicated)': f'WITH DeduplicatedRecords AS (SELECT property_address, sold_date, sold_price, ROW_NUMBER() OVER (PARTITION BY TRIM(LOWER(property_address)), sold_date, sold_price ORDER BY CASE WHEN sold_date ~ \'^[A-Za-z]{{3}} \\d{{1,2}}, \\d{{4}}$\' THEN TO_DATE(sold_date, \'Mon DD, YYYY\') ELSE NULL END DESC) AS row_num FROM {ZEBRA_TABLE} WHERE sold_date ~ \'^[A-Za-z]{{3}} \\d{{1,2}}, \\d{{4}}$\') SELECT COUNT(*) FROM DeduplicatedRecords WHERE row_num = 1;',
    'Missing Buyer Agent Data': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE (buyer_agent IS NULL OR buyer_agent = \'\') AND (buyer_agent_company IS NULL OR buyer_agent_company = \'\') AND (buyer_agent_license IS NULL OR buyer_agent_license = \'\');',
    'Main Agent Company Missing': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE main_agent <> \'\' AND main_agent_company = \'\';',
    'Buyer Agent Company Missing': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE buyer_agent <> \'\' AND buyer_agent_company = \'\';',
    'Co-Agent Company Missing': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE co_agent <> \'\' AND co_agent_company = \'\';',
    'Missing Main Agent Photo': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE (main_agent <> \'\' OR main_agent IS NOT NULL) AND (main_agent_photo = \'null\' OR main_agent_photo = \'\' OR main_agent_photo IS NULL);',
    'Missing Buyer Agent Photo': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE (buyer_agent <> \'\' OR buyer_agent IS NOT NULL) AND (buyer_agent_photo = \'null\' OR buyer_agent_photo = \'\' OR buyer_agent_photo IS NULL);',
    'Missing Home Photo': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE home_photo = \'\' OR home_photo = \'null\';',
    'Sold Date "Not Listed for Sale"': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE sold_date ILIKE \'%NOT LISTED FOR SALE%\' AND property_history = \'\';',
    'Missing Environment Factors': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE environment_factors = \'\' OR environment_factors IS NULL;',
    'Missing Property History': f'SELECT COUNT(*) FROM {ZEBRA_TABLE} WHERE property_history = \'\' OR property_history IS NULL;',
    'Unique Agents': f'WITH agent_data AS (SELECT main_agent AS agent_name, main_agent_company AS company FROM {ZEBRA_TABLE} WHERE main_agent IS NOT NULL UNION SELECT buyer_agent, buyer_agent_company FROM {ZEBRA_TABLE} WHERE buyer_agent IS NOT NULL) SELECT COUNT(DISTINCT ROW(agent_name, company)) AS unique_agents_total FROM agent_data;',
    'Unique License Numbers': f'WITH agent_data AS (SELECT main_agent AS agent_name, main_agent_company AS company, main_agent_license AS license FROM {ZEBRA_TABLE} WHERE main_agent = \'\' UNION SELECT buyer_agent, buyer_agent_company, buyer_agent_license FROM {ZEBRA_TABLE} WHERE buyer_agent <> \'\') SELECT COUNT(DISTINCT (agent_name, company)) AS unique_agents_total, COUNT(DISTINCT license) FILTER (WHERE license <> \'null\') AS unique_licenses_total, ROUND(COUNT(DISTINCT license) FILTER (WHERE license <> \'null\') * 100.0 / NULLIF(COUNT(DISTINCT (agent_name, company)), 0), 2) AS percentage_with_license FROM agent_data;',
    'Unique Phone Numbers': f'WITH agent_data AS (SELECT main_agent AS agent_name, main_agent_company AS company, main_agent_phone AS phone FROM {ZEBRA_TABLE} WHERE main_agent IS NOT NULL UNION SELECT buyer_agent, buyer_agent_company, buyer_agent_phone FROM {ZEBRA_TABLE} WHERE buyer_agent IS NOT NULL) SELECT COUNT(DISTINCT (agent_name, company)) AS unique_agents_total, COUNT(DISTINCT phone) FILTER (WHERE phone IS NOT NULL) AS unique_phone_total, ROUND(COUNT(DISTINCT phone) FILTER (WHERE phone IS NOT NULL) * 100.0 / NULLIF(COUNT(DISTINCT (agent_name, company)), 0), 2) AS percentage_with_phone FROM agent_data;'
}

suspicious_realtor_patterns = {
    'Realtors Name Starts With "test"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE 'test%' OR r.full_name ILIKE 'test%');''',
    'Realtors Name Ends With "test"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%test' OR r.full_name ILIKE '%test');''',
    'Realtors Name Contains "not a member"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%not a member%' OR r.full_name ILIKE '%not a member%');''',
    'Realtors Name Contains "out of state"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%out of state%' OR r.full_name ILIKE '%out of state%');''',
    'Realtors Name Starts With "none"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE 'none %' OR r.full_name ILIKE 'none %');''',
    'Realtors Name Ends With "none"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '% none' OR r.full_name ILIKE '% none');''',
    'Realtors Name Ends With "non"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '% non' OR r.full_name ILIKE '% non');''',
    'Realtors Name Starts With "non"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE 'non %' OR r.full_name ILIKE 'non %');''',
    'Realtors Name Contains "nonmember"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%nonmember%' OR r.full_name ILIKE '%nonmember%');''',
    'Realtors Name Starts With "other"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE 'other %' OR r.full_name ILIKE 'other %');''',
    'Realtors Name Contains "Member"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%Member%' OR r.full_name ILIKE '%Member%');''',
    'Realtors Name Contains "Nonmls"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%Nonmls%' OR r.full_name ILIKE '%Nonmls%');''',
    'Realtors Name Contains "Outside"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%Outside%' OR r.full_name ILIKE '%Outside%');''',
    'Realtors Name Contains "null"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%null %' OR r.full_name ILIKE '%null %');''',
    'Realtors Name Contains "AGENT"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%AGENT%' OR r.full_name ILIKE '%AGENT%');''',
    'Realtors Name Contains "Unidentified"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%Unidentified%' OR r.full_name ILIKE '%Unidentified%');''',
    'Realtors Name Contains "RMLS"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%RMLS%' OR r.full_name ILIKE '%RMLS%');''',
    'Realtors Name Contains "Default"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%Default%' OR r.full_name ILIKE '%Default%');''',
    'Realtors Name Contains "Subscriber"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%Subscriber%' OR r.full_name ILIKE '%Subscriber%');''',
    'Realtors Name Contains "NON-MBR"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%NON-MBR%' OR r.full_name ILIKE '%NON-MBR%');''',
    'Realtors Name Contains "Represented"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%Represented%' OR r.full_name ILIKE '%Represented%');''',
    'Realtors Name Contains "listing"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%listing%' OR r.full_name ILIKE '%listing%');''',
    'Realtors Name Contains "Participant"': f'''SELECT COUNT(*) FROM realtors r WHERE r.duplicate = false AND r.hidden = false AND (r.name ILIKE '%Participant%' OR r.full_name ILIKE '%Participant%');''',
}

active_buyer_completeness_report = {
    'Total Active Home Shoppers': '''SELECT COUNT(*) FROM properties WHERE home_shopper_active = true;''',

    'Owner Occupied: True': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'owner_occupied')::boolean = true;
    ''',

    'Owner Occupied: False': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'owner_occupied')::boolean = false;
    ''',

    'In-State Shopper Only': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true
        AND (last_entry.elem ->> 'in_state_shopper')::boolean = true
        AND (last_entry.elem ->> 'out_of_state_shopper')::boolean = false;
    ''',

    'Out-of-State Shopper Only': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true
        AND (last_entry.elem ->> 'in_state_shopper')::boolean = false
        AND (last_entry.elem ->> 'out_of_state_shopper')::boolean = true;
    ''',

    'Both In-State and Out-of-State Shopper': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true
        AND (last_entry.elem ->> 'in_state_shopper')::boolean = true
        AND (last_entry.elem ->> 'out_of_state_shopper')::boolean = true;
    ''',

    'Visits > 1': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'unique_obs_count')::int > 1;
    ''',

    'Visits > 3': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'unique_obs_count')::int > 3;
    ''',

    'Visits > 5': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'unique_obs_count')::int > 5;
    ''',

    'Visits > 7': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'unique_obs_count')::int > 7;
    ''',

    'Visits > 9': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'unique_obs_count')::int > 9;
    ''',

    'Last Visit Within 10 Days': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '10 days') OR
          (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '10 days')
        );
    ''',

    'Last Visit Within 30 Days': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '30 days') OR
          (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '30 days')
        );
    ''',

    'Last Visit Within 45 Days': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '45 days') OR
          (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '45 days')
        );
    ''',

    'Last Visit Within 60 Days': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '60 days') OR
          (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '60 days')
        );
    ''',

    'Last Visit Within 90 Days': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '90 days') OR
          (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '90 days')
        );
    ''',

    'Last Visit Within 120 Days': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '120 days') OR
          (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '120 days')
        );
    ''',

    'Last Visit Within 200 Days': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '200 days') OR
          (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '200 days')
        );
    ''',

    'Last Visit Within 300 Days': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (
          (last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'last_observed')::timestamp >= CURRENT_DATE - INTERVAL '300 days') OR
          (NOT last_entry.elem ? 'last_observed' AND (last_entry.elem ->> 'recorded_date')::date >= CURRENT_DATE - INTERVAL '300 days')
        );
    ''',

    'LMI Property Count = 0': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'lmi_property_count')::int = 0;
    ''',

    'LMI Property Count > 0': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'lmi_property_count')::int > 0;
    ''',

    'Visited Unique Properties > 3': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true AND (last_entry.elem ->> 'unique_property_count')::int > 3;
    ''',

    'High Listing Price < Low Listing Price (Anomaly)': '''
        SELECT COUNT(*) FROM properties p,
        LATERAL (
          SELECT elem FROM jsonb_array_elements(p.home_shopper -> 'hs_history') WITH ORDINALITY AS t(elem, idx)
          ORDER BY idx DESC LIMIT 1
        ) last_entry
        WHERE p.home_shopper_active = true
        AND (last_entry.elem ->> 'listing_price_avg_high')::numeric < (last_entry.elem ->> 'listing_price_avg_low')::numeric;
    '''
}




