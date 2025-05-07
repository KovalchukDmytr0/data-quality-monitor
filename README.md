# Query Executor Tools

A collection of Python scripts for executing database queries and generating reports for various entities (Sales, Loans, Loan Officers, and Realtors).

## Prerequisites

- Python 3.x
- Required Python packages:
  - boto3
  - psycopg2
  - python-dotenv
  - pandas

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd query_executor
```

2. Install dependencies using pip:

```bash
pip install -r requirements.txt
```

Or install packages individually:

```bash
pip install boto3 psycopg2-binary python-dotenv pandas
```

## Configuration

1. Create a `.env` file in the root directory with the following structure:

```env
# AWS Region
REGION=your-aws-region

# Development Environment
DEV_DB_HOST=your-dev-db-host
DEV_DB_NAME=your-dev-db-name
DEV_DB_USER=your-dev-db-user
DEV_AWS_ACCESS_KEY_ID=your-dev-aws-access-key
DEV_AWS_SECRET_ACCESS_KEY=your-dev-aws-secret-key
DEV_AWS_SESSION_TOKEN=your-dev-aws-session-token

# Staging Environment
STAGE_DB_HOST=your-stage-db-host
STAGE_DB_NAME=your-stage-db-name
STAGE_DB_USER=your-stage-db-user
STAGE_AWS_ACCESS_KEY_ID=your-stage-aws-access-key
STAGE_AWS_SECRET_ACCESS_KEY=your-stage-aws-secret-key
STAGE_AWS_SESSION_TOKEN=your-stage-aws-session-token

# Production Environment
PROD_DB_HOST=your-prod-db-host
PROD_DB_NAME=your-prod-db-name
PROD_DB_USER=your-prod-db-user
PROD_AWS_ACCESS_KEY_ID=your-prod-aws-access-key
PROD_AWS_SECRET_ACCESS_KEY=your-prod-aws-secret-key
PROD_AWS_SESSION_TOKEN=your-prod-aws-session-token
```

## Available Scripts

The repository contains four main query executor scripts:

1. `dev_query_executor_sales.py` - Analyzes sales data
2. `dev_query_executor_loans.py` - Analyzes loan data
3. `dev_query_executor_loan_officers.py` - Analyzes loan officer data
4. `dev_query_executor_realtors.py` - Analyzes realtor data

## Usage

Each script can be run against different environments (dev, stage, prod) by modifying the `ENV` variable in the script. By default, scripts are set to run against the development environment.

To run any of the scripts:

```bash
python3 Dev/dev_query_executor_sales.py
python3 Dev/dev_query_executor_loans.py
python3 Dev/dev_query_executor_loan_officers.py
python3 Dev/dev_query_executor_realtors.py
```

## Output

Each script generates a CSV file with the following naming convention:

- `sales_counts_{env}.csv`
- `loans_counts_{env}.csv`
- `loan_officers_counts_{env}.csv`
- `realtor_counts_{env}.csv`

The CSV files contain two columns:

1. Query Description - The name of the query/metric
2. Count - The result of the query

## Features

- **Environment-based Configuration**: Run queries against different environments (dev, stage, prod)
- **Parallel Processing**: Uses ThreadPoolExecutor for concurrent query execution
- **AWS IAM Authentication**: Secure database access using AWS IAM tokens
- **Error Handling**: Graceful error handling and reporting
- **CSV Output**: Results are saved in an easily readable CSV format

## Security Notes

- Never commit the `.env` file to version control
- Keep AWS credentials secure and rotate them regularly
- Use appropriate IAM roles and permissions
- Ensure database credentials are properly secured

## Troubleshooting

If you encounter any issues:

1. Verify your `.env` file is properly configured
2. Check AWS credentials are valid and have necessary permissions
3. Ensure database connection details are correct
4. Verify network access to the database
5. Check Python package dependencies are installed

## Contributing

When adding new queries or modifying existing ones:

1. Follow the existing code structure
2. Add appropriate comments
3. Test against all environments
4. Update this README if necessary
