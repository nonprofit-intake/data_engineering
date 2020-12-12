# Downloads and opens csv of predictions from s3 bucket, 
# uses these to update the database table with predictions.

import pandas as pd
import boto3
import os
import sys
import logging
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# rds settings
rds_host = os.environ.get('RDS_HOST')
rds_username = os.environ.get('RDS_USERNAME')
rds_user_pwd = os.environ.get('RDS_USER_PWD')

# s3 settings
s3 = boto3.client('s3')
s3_bucket_origin = os.environ.get('S3_BUCKET_ORIGIN')

# predictions
predictions_file = os.environ.get('PREDICTIONS_FILE')
predictions_file_path = '/tmp/' + predictions_file


def lambda_handler(event, context):
    '''Downloads and opens csv of predictions from s3 bucket, uses these to update the 
    database table with predictions.'''

    # download predictions csv from bucket
    s3.download_file(s3_bucket_origin, predictions_file, predictions_file_path)

    # open predictions csv
    with open(predictions_file_path, 'rb') as f:
        predictions = pd.read_csv(f) 

    try:
        conn = psycopg2.connect(
            host=rds_host,
            user=rds_username,
            password=rds_user_pwd)
    except:
        logger.error("ERROR: Could not connect to Postgres instance.")
        sys.exit()

    logger.info("SUCCESS: Connection to RDS Postgres instance succeeded")


    # iterate through our results df
    for row in predictions.itertuples():
        with conn.cursor() as cur:
            # update database with our predictions
            sql_query = 'UPDATE guests_temp SET predicted_exit_destination=%s WHERE personal_id=%s AND enroll_date=%s'
            data = (row.predicted_exit_destination, row.personal_id, row.enroll_date)
            cur.execute(sql_query, data)
            conn.commit()
    conn.close()
    