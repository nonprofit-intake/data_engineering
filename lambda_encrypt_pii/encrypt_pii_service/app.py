import os
import json
import pandas as pd
import numpy as np
import psycopg2
import math
from cryptography.fernet import Fernet

from chalice import Chalice, Response

app = Chalice(app_name='encrypt_pii_service')

DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PWD = os.environ['DB_PWD']

def encrypt(row_data):
  """
  Encrypts arbitrary string.
  
  row_data - str
  """
  encryption_key = str.encode(os.environ["ENCRYPTION_KEY"])
  cipher_suite = Fernet(encryption_key)
  cipher_text = cipher_suite.encrypt(str.encode(row_data)).decode("utf-8")
  return cipher_text


def wrangle(df):
  """
  Accepts dataframe and wrangles data for encryption.

  df - dataframe
  """
  df_copy = df.copy()

  # drop all but last 4 digits of ssn
  df_copy['ssn'] = df_copy['ssn'].apply(lambda row: str(row).split('-')[2])
  
  # drop any duplicate guests
  df_copy = df_copy.drop_duplicates()

  # encrypt remaining guest
  df_copy['ssn'] = df_copy['ssn'].apply(lambda row: encrypt(row))

  return df_copy


@app.route('/encrypt-pii')
def encrypt_pii_columns():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PWD
        )

        cursor = connection.cursor()
        cursor.execute("SELECT ssn, personal_id FROM guests WHERE ssn IS NOT NULL")

        df = pd.DataFrame(cursor.fetchall())
        df.columns = [x.name for x in cursor.description]

        encrypted_df = wrangle(df)

        user_data = []
        for row in encrypted_df.itertuples():
            user_data.append((row.ssn, row.personal_id))

        # Required type casting for query below (create once):
        # CREATE type t AS (a varchar(255), b integer);

        cursor.execute("""
            UPDATE guests g
            SET
                ssn = s.ssn
            FROM unnest(%s::t[]) s(ssn, personal_id)
            WHERE g.personal_id = s.personal_id;
        """, (user_data,))

        connection.commit()

        return {"Message": "PII encrypted succesfully"}
    except psycopg2.Error:
        return Response(
            body={"Error": "Service unavailable, please contact Family Promise IT manager"}, 
            headers={"Content-Type": "text/plain"},
            status_code=503
        ) 
    except:
        return {"Message": "PII already encrypted"}
    finally:
        if (connection):
            cursor.close()
            connection.close()