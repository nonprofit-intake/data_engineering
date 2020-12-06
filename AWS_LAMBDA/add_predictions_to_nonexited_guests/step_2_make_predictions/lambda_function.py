import pandas as pd
import boto3
import os
import pickle
import numpy as np

# s3 settings
s3 = boto3.client('s3')
s3_bucket_origin = os.environ.get('S3_BUCKET_ORIGIN')
s3_bucket_destination = os.environ.get('S3_BUCKET_DESTINATION')

# model
model_name = os.environ.get('MODEL_NAME')
model_file_path = '/tmp/' + model_name

# wrangled data
wrangled_data_file = os.environ.get('WRANGLED_DATA_FILE')
wrangled_data_file_path = '/tmp/' + wrangled_data_file


def lambda_handler(event, context):
    '''Uses lightgbm model on current guests to create prediction for their exit destination.'''

    # download model file from bucket
    s3.download_file(s3_bucket_origin, model_name, model_file_path)

    # open model file
    with open(model_file_path, 'rb') as f:
        model = pickle.load(f)  

    # download wrangled data file from bucket
    s3.download_file(s3_bucket_origin, wrangled_data_file, wrangled_data_file_path)

    # open wrangled data file
    with open(wrangled_data_file_path, 'rb') as f:
        wrangled_data = pd.read_csv(f) 

    # remove enroll_date & predicted exit destination in order to make predictions (put in wrangling function)
    # (will not model correctly otherwise)
    cols_to_drop = ['enroll_date', 'personal_id', 'predicted_exit_destination']
    prediction = model.predict(wrangled_data.drop(columns=cols_to_drop))
    
    # finding max of each class
    prediction = [np.argmax(line) for line in prediction]

    # add predictions to df as new column
    wrangled_data['predicted_exit_destination'] = prediction
    wrangled_data['predicted_exit_destination'] = wrangled_data.predicted_exit_destination.replace([0,1,2,3,4], ['es', 'other', 'perm', 'temp', 'unknown'])

    # change current directory to /tmp
    os.chdir('/tmp')

    # save our csv to /tmp directory
    wrangled_data.to_csv('/tmp/csv_file.csv', index=False)

    # upload csv to bucket as wrangled_guests.csv
    s3.upload_file('/tmp/csv_file.csv', s3_bucket_destination, 'predictions.csv')
    