pp_forms_log.py
from utils import get_secret, get_rs_conn, get_mysql_client, get_session, get_logger, get_webhook, send_slack_notification
import multiprocessing as mp
import os

# -------------------------------------- Functions --------------------------------------

# Function to initialize global logger and session for each worker process.
def init_globals():
    global logger, session
    logger = get_logger()
    session = get_session(creds)


# Task for the multiprocessing pool that will fetch the data in chunks
# from the published_version table and upload it to S3.
def task(c, creds, bucket, table_name, chunk):
    global session, logger  # Access global session and logger
    logger.info(f'Uploading chunk {c} ...')

    key = f'{table_name}_{c + 1}.csv'

    sql_q = f"""
    select
        id,
        form_id,
        case when form LIKE '%purchaseProtection%' then 1 else 0 end as pp_enabled,
        date_created
    FROM published_version
    WHERE id >= {chunk * c} AND id < {chunk * (c + 1)}
    INTO OUTFILE S3 's3://{bucket}/raw/{table_name}/{key}'
    FORMAT CSV HEADER
    OVERWRITE ON;
    """

    mydb = get_mysql_client(session, creds)
    mycursor = mydb.cursor()
    mycursor.execute(sql_q)

    mycursor.close()
    mydb.close()

    logger.info(f'Chunk {c} uploaded.')


# Function to build the pp_forms_log table if empty
def build_new_pp_forms_log_table():
    global session, logger  # Access global session and logger

    mydb = get_mysql_client(session, creds)
    mycursor = mydb.cursor()
    mycursor.execute(f"select count(*) from published_version")

    l = mycursor.fetchall()[0][0]
    logger.info(f"Total records: {l}")

    mycursor.close()
    mydb.close()

    chunk = 100000
    chunks = range(int(l / chunk) + 1)

    # Initialize a pool of processes
    pool = mp.Pool(5, initializer=init_globals)  # Set initializer to init_globals
    pool.starmap(task, [(c, creds, bucket, table_name, chunk) for c in chunks])
    pool.close()
    pool.join()  # Ensure all processes finish


# Function to copy chunks from S3 to the pp_forms_log table
def copy_chunks_to_table():
    global conn, rscursor  # Ensure conn and rscursor are accessible

    rscursor.execute(f"DROP TABLE IF EXISTS {table_name_full} CASCADE;")
    conn.commit()

    rscursor.execute(f'CREATE TABLE {table_name_full} ("id" integer, "form_id" integer, "pp_enabled" integer, "date_created" timestamp);')
    conn.commit()

    rscursor.execute(f"GRANT ALL PRIVILEGES ON TABLE wbx_data.pp_forms_log TO GROUP admin;;")
    conn.commit()


    s3_file_path = f's3://{bucket}/raw/{table_name}'

    copy_query = f"""
        COPY {table_name_full}
        FROM '{s3_file_path}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1
        DELIMITER ',';
    """

    rscursor.execute(copy_query)
    conn.commit()


# Function to add new records from published_version to pp_forms_log
def add_new_records():
    global session, logger  # Access global session and logger

    new_file = 'new_pp_form_records.csv'

    new_records = f"""
    select
        id,
        form_id,
        case when form LIKE '%purchaseProtection%' then 1 else 0 end as pp_enabled,
        date_created
    FROM published_version
    WHERE id > {max_id}
    INTO OUTFILE S3 's3://{bucket}/raw/{table_name}/{new_file}'
    FORMAT CSV HEADER
    OVERWRITE ON;
    """

    mydb = get_mysql_client(session, creds)
    mycursor = mydb.cursor()
    mycursor.execute(new_records)

    mycursor.close()
    mydb.close()

    logger.info("Adding new records to table ...")

    s3_file_path = f's3://{bucket}/raw/{table_name}/{new_file}'

    copy_query = f"""
        COPY {table_name_full}
        FROM '{s3_file_path}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1
        DELIMITER ',';
    """

    rscursor.execute(copy_query)
    conn.commit()

# -------------------------------------- Start --------------------------------------

if __name__ == "__main__":
    logger = get_logger()
    logger.info("Starting ...")

    creds = {'db': 'wbx_data', 'region': 'us-west-2', 'cluster_id': 'wbx-data'}

    # Defining variables
    iam_role = f"arn:aws:iam::{os.environ['ACCOUNT_ID']}:role/redshift-unload"
    bucket = 'wbx-data.redshift-unload'
    table_name = 'pp_forms_log'
    table_name_full = f"{creds['db']}.{table_name}"

    # Initializing Botocore client
    logger.info("Initializing Botocore client ...")
    session = get_session(creds)

    # Setting up Webhook
    webhook_url = get_webhook(session, creds)
    send_slack_notification(webhook_url, "Updating pp form logs :loading_dots:")

    # Establishing Redshift connection
    logger.info("Establishing Redshift connection ...")
    rs_secret = get_secret(os.environ['RS_SECRET'], session, creds['region'])
    conn = get_rs_conn(rs_secret)
    rscursor = conn.cursor()

    # Check if the pp_forms_log table exists and has records by fetching the max id
    try:
        max_id = rscursor.execute('select max(id) from wbx_data.pp_forms_log').fetchall()[0][0]
        logger.info(f"Max record is: {max_id}")
    except:
        max_id = 0

    # If the pp_forms_log table is empty, populate it
    if max_id == 0 or max_id is None:
        logger.info(f'{table_name} table is empty. Populating ...')
        build_new_pp_forms_log_table()
        
        logger.info(f'Copying chunks to table...')
        copy_chunks_to_table()

    # Otherwise, fetch and add new records from the published_version table   
    else:
        logger.info("Fetching new records ...")
        add_new_records()

    send_slack_notification(webhook_url, "PP form logs updated :white_check_mark:")
    logger.info("Done!")
