__all__ = ['loan_officers_query', 'sales_query', 'realtors_query', 'loans_query']

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
