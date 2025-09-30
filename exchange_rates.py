from utils import get_secret, get_session, get_rs_conn, get_logger, get_webhook, send_slack_notification
from awswrangler import redshift
import redshift_connector
import pandas as pd
import requests
import os

# Initialize variables
API_URL = os.getenv('API_URL')
API_SECRET = os.getenv('API_SECRET')
creds = {"region": "us-west-2", "cluster_id": "wbx-data", "db": "wbx_data"}
schema = creds['db']
table = 'exchange_rates'
bucket_name = 'wbx-data.redshift-unload'
prefix = 'raw/exchange_rates/'
copy_mode = 'append'

# Set up Botocore session
session = get_session(creds)

# Get logger
logger = get_logger()
logger.info("Starting ...")

# Establishing Redshift connection
logger.info("Establishing Redshift connection ...")
rs_secret = get_secret(os.environ['RS_SECRET'], session, creds['region'])
conn = get_rs_conn(rs_secret)

# Set up webhook
webhook_url = get_webhook(session, creds)
send_slack_notification(webhook_url, 'Fetching exchange rates :loading_dots:')

# Create the exchange_rates table in Redshift if it doesn't exist
def create_table_and_insert_data(conn):
    cursor = conn.cursor()
    schema = 'wbx_data'
    table = "exchange_rates"

    cursor.execute(f'Grant all on all tables in schema {schema} to group admin;')
    conn.commit()

    q = """
    select 'exchange_rates' in
    (
        select table_name
        from information_schema.tables
        where table_schema = 'wbx_data'
    );
    """
    cursor.execute(q)
    result = cursor.fetchone()

    # Check the result and convert it to a boolean
    table_exists = result[0] if result else False

    if not table_exists:
        copy_mode = 'overwrite'

        create_table_query = f"""
        CREATE TABLE {schema}.{table} (
        date DATE,
        currency VARCHAR(3),
        rate DOUBLE PRECISION
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

    cursor.execute(f'Grant all on all tables in schema {schema} to group admin;')
    conn.commit()

# Fetch the dates from Redshift starting the the day after the last date in the exchange_rates table and ending with today
def get_dates(conn):
    dates = f"""
    select full_date
    from wbx_data_dbt.dim_dates
    where full_date >= '2015-01-01'
        and full_date <= getdate()
        and full_date not in (select date from wbx_data.exchange_rates)
    order by full_date desc;
    """
    dates = pd.read_sql(dates, conn)
    return dates


# Retreive historical exchange rates using the Open Exchange Rates API
def fetch_exchange_rate_data():

    dates = get_dates(conn)
    if len(dates) > 0:
        logger.info(f"Fetching exchange rates starting from {min(dates['full_date'])}.")
        exchange_rates = pd.DataFrame()
        api_secret_response = get_secret(API_SECRET, session, creds['region'])

        # Send GET request to the Open Exchange Rates API
        for date in dates['full_date'].values:
            url = f'https://openexchangerates.org/api/historical/{date}.json'
            params = {'app_id': api_secret_response['ID']}

            response = requests.get(url, params=params)
            if response.status_code == 401:
                return 'Error retrieving data from the API'
            data = response.json().get('rates', {})
            rates = pd.DataFrame(list(data.items()), columns=['currency', 'rate'])
            rates['date'] = date
            rates['rate'] = 1 / rates['rate']
            rates = rates[['date', 'currency', 'rate']]
            exchange_rates = pd.concat([exchange_rates, rates], ignore_index=True)
            logger.info(f"{len(rates)} rows added for date {date}. Total : {len(exchange_rates)}")

        if len(exchange_rates) == 0:
            return 'Exceeded request rate limit'
        return exchange_rates
    else:
        return False

# Copy the exchange rates data to Redshift by first uploading the data to S3
def copy_to_redshift(exchange_rates):
    logger.info("Writing to Redshift ...")
    redshift.copy(
                df = exchange_rates,
                path = f's3://{bucket_name}/raw/temp.csv',
                con = conn,
                table = table,
                schema = creds['db'],
                mode = copy_mode,
                overwrite_method = "drop",
                boto3_session = session
            )

    # Grant permissions to the new table
    conn.cursor().execute(f"GRANT ALL ON ALL TABLES in SCHEMA {creds['db']} to group admin;")
    conn.commit()
    conn.close()


def main():
    create_table_and_insert_data(conn)
    exchange_rates = fetch_exchange_rate_data()
    if isinstance(exchange_rates, str):
        logger.error(exchange_rates)
        send_slack_notification(webhook_url, ":red-x-mark: " + exchange_rates)
    elif isinstance(exchange_rates, pd.DataFrame):
        copy_to_redshift(exchange_rates)
        send_slack_notification(webhook_url, 'Exchange rates updated :white_check_mark:')
    else:
        logger.info('Table was already up to date.')
        send_slack_notification(webhook_url, 'Table was already up to date :thumbsup:')
    


if __name__ == "__main__":
    main()
