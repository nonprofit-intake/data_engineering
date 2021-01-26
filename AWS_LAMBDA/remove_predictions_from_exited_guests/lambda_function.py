# Each day, guests are exiting. Once a guest has exited, the predicted_exit_destination
# column should be "exited" rather than a prediction.

import os
import sys
import logging
import psycopg2

# RDS settings
rds_host = os.environ.get('RDS_HOST')
rds_username = os.environ.get('RDS_USERNAME')
rds_user_pwd = os.environ.get('RDS_USER_PWD')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Connect to database
try:
    # If able to connect
    conn = psycopg2.connect(
        host=rds_host,
        user=rds_username,
        password=rds_user_pwd)
        
except:
    # If unable to connect
    logger.error("ERROR: Could not connect to Postgres instance.")
    sys.exit()

# Log connection success
logger.info("SUCCESS: Connection to RDS Postgres instance succeeded")

def lambda_handler(event, context):
    '''Add predicted_exit_destination column to guests if not exists, 
    then updates guests to account for newly exited guests (if exited, no more need for prediction on row).'''

    add_column_query = "ALTER TABLE guests ADD IF NOT EXISTS predicted_exit_destination VARCHAR;"
    update_query = "UPDATE guests SET predicted_exit_destination=CASE WHEN exit_date IS NOT NULL THEN 'exited' END;"

    queries = [add_column_query, update_query]

    for query in queries:
       
        with conn.cursor() as cur:

            cur.execute(query)
            conn.commit()

    return 'status: 200'
