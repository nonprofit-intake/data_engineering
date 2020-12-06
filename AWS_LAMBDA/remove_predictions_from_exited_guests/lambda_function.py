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
    '''Add predicted_exit_destination column to guests_temp if not exists, 
    then updates guests_temp to account for newly exited guests (if exited, no more need for prediction on row).'''

    # Check if column exists
    if conn != None: 
        cur = conn.cursor()

        try:
            # If column exists
            query = "SELECT predicted_exit_destination FROM guests_temp;"
            cur.execute(query)
            
        except:
            conn.rollback()
            # If column doesn't exist
            query = "ALTER TABLE guests_temp ADD predicted_exit_destination VARCHAR;"
            cur.execute(query)

    conn.commit()

    # Check if column exists
    with conn.cursor() as cur:

        try:
            # Update predictions
            update_query = "UPDATE guests_temp SET predicted_exit_destination=CASE WHEN exit_date IS NOT NULL THEN 'exited' END;"
            cur.execute(update_query)

        except:
            # Unable to update predictions table
            logger.error("ERROR: Could not update predictions table.")
            sys.exit()

    conn.commit()
    conn.close()
