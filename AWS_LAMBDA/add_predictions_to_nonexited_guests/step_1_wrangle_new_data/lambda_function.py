# Connect to database, query for only rows with no predicted exit destination, 
# 'wrangle' rows to be fit for modeling, then sending to an S3 bucket

import sys
import logging
import psycopg2
import pandas as pd
import boto3
import os

# rds settings
rds_host = os.environ.get('RDS_HOST')
rds_username = os.environ.get('RDS_USERNAME')
rds_user_pwd = os.environ.get('RDS_USER_PWD')

# s3 settings
s3 = boto3.resource('s3')
s3_bucket = os.environ.get('S3_BUCKET')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = psycopg2.connect(
        host=rds_host,
        user=rds_username,
        password=rds_user_pwd)
except:
    logger.error("ERROR: Could not connect to Postgres instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS Postgres instance succeeded")

def wrangle(df):
    print(f"Original shape: {df.shape}")
    clean_df = df.copy()
    # NOT CURRENT IN GUESTS DB
    # columns = ["private_disability_income",'predicted_exit_destination']
    clean_df.columns = map(str.lower, clean_df.columns)
    clean_df.drop(columns="index", inplace=True)
    # Drop sensitive columns
    columns = ['enrollment_created_by',	'first_name',	'last_name',	
                'ssn', 'dob']
    clean_df.drop(columns=columns, inplace=True)
    # drop reviewed columns
    drop_col = ["days_enrolled_in_project","bednights_during_report_period", 
                "entire_episode_bednights", "contact_services", 
                "non-cash_count_at_exit", "info_release_status",
                "other_public", "unemployement_income", "housing_checkins"]
    clean_df.drop(columns=drop_col, inplace=True)
    # Drop questionable columns
    # REMINDER: important, missing values, these should be required
    columns = ["zip", "income_at_entry", "income_at_exit", 
            "length_of_time_homeless", "homeless_start_date",
            "vet_status", "living_situation", "length_of_stay",
            "homeless_start_date", "times_homeless_last_3years",
            "total_months_homeless", "last_perm_address", "state",
            "municipality", "housing_status", "domestic_violence",
            "currently_fleeing", "when_dv_occured", "engagement_date",
            "last_grade_completed", "school_status", "employed_status",
            "reason_not_employed", "type_of_employment", "looking_for_work",
            "soar_eligibility", "alcohol_abuse", "chronic_health_condition",
            "developmental_disability", "substance_abuse",
            "mental_health_problem", "physical_disability"]
    clean_df.drop(columns=columns, inplace=True)
    # Drop interesting most_recent columns, check in with @J, future feature engineering
    columns = ["most_recent_rrh", "most_recent_street_outreach",
                "most_recent_ce", "most_recent_es", "most_recent_trans",
                "most_recent_psh", "most_recent_prevention"]
    clean_df.drop(columns=columns, inplace=True)
    # Drop interesting datetime columns, check in with @J, future feature engineering
    columns = ["date_of_last_contact", "date_of_first_contact", 
                "date_of_last_stay", "date_of_first_stay"]
    clean_df.drop(columns=columns, inplace=True)
    # Drop unecessary columns
    # lat/lon (not useful), income_at_update (extreme low count), program_type (constant)
    # client_id (constant), chronic_homeless_status_assessment (identical exists),
    # org_name (constant), project_type (constant), fed_grant_programs (constant),
    # client_location (constant), days_enrolled_until_rrh_movein (constant),
    # days_enrolled_until_engaged (difficult), current_status (?), connected_to_mvento (?),
    # current_date (constant), cobra (constant), private_individual (constant), 
    # workers_compensation (nearly constant), personal_id (?), case_id (?)
    columns = ["latitude", "longitude", "current_status", "workers_compensation",
                "income_at_update", "program_type", "connected_to_mvento",
                "client_id", "chronic_homeless_status_assessment", "cobra",
                "project_type", "fed_grant_programs", "org_name",
                "client_location", "days_enrolled_until_rrh_movein", "case_id",
                "days_enrolled_until_engaged", "current_date", "private_individual"]
    clean_df.drop(columns=columns, inplace=True) 
    # Drop columns with 100 or more nan values
    thresh = int(clean_df.shape[0]*0.90)
    clean_df.dropna(axis='columns', thresh=thresh, inplace=True)
    # Drop columns with all 0 values
    clean_df = clean_df.loc[:, (clean_df != 0).any(axis=0)]
    # Drop rows with NaN values, should not cut down the num of rows by much..
    clean_df.dropna(inplace=True)
    # Convert project_name into two new features; uncertain, not included
    clean_df.drop(columns=["project_name"], inplace=True)
    # Convert util_track_method into new features; uncertain, not included
    clean_df.drop(columns=["util_track_method"], inplace=True)
    # Convert relationship_to_hoh into new features
    clean_df["is_hoh"] = clean_df.relationship_to_hoh.isin(["Self"]).astype(int)
    clean_df["is_son"] = clean_df.relationship_to_hoh.isin(["Son"]).astype(int)
    clean_df["is_daughter"] = clean_df.relationship_to_hoh.isin(["Daughter"]).astype(int)
    clean_df["is_significant_other"] = clean_df.relationship_to_hoh.isin(["Significant Other (Non-Married)"]).astype(int)
    clean_df["is_spouse"] = clean_df.relationship_to_hoh.isin(["Spouse"]).astype(int)
    clean_df["is_grandchild"] = clean_df.relationship_to_hoh.isin(["Grandchild"]).astype(int)
    clean_df["is_other_family"] = clean_df.relationship_to_hoh.isin(["Other Family Member"]).astype(int)
    clean_df["is_other_non_family"] = clean_df.relationship_to_hoh.isin(["Dependent Child", "Step Child", "Other Non-Family"]).astype(int)
    clean_df.drop(columns=["relationship_to_hoh"], inplace=True)
    # Convert ssn_quality, dob_quality into new features
    clean_df["ssn_available"] = clean_df.ssn_quality.isin(["Full SSN"]).astype(int)
    clean_df["dob_available"] = clean_df.dob_quality.isin(["Full DOB Reported"]).astype(int)
    clean_df.drop(columns=["ssn_quality", "dob_quality"], inplace=True)
    # Convert race into new features
    clean_df["race_refused"] = clean_df.race.isin(["Client refused"]).astype(int)
    clean_df["is_white"] = clean_df.race.isin(["White"]).astype(int)
    clean_df["is_am_or_ak_native"] = clean_df.race.isin(["American Indian or Alaska Native"]).astype(int)
    clean_df["is_black"] = clean_df.race.isin(["Black or African American"]).astype(int)
    clean_df["is_multi_racial"] = clean_df.race.isin(["Multi-Racial"]).astype(int)
    clean_df["is_pacific_islander"] = clean_df.race.isin(["Native Hawaiian or Other Pacific Islander"]).astype(int)
    clean_df["is_asian"] = clean_df.race.isin(["Asian"]).astype(int)
    clean_df.drop(columns=["race"], inplace=True)
    # Convert ethnicity into new features
    clean_df['ethnicity_refused'] = clean_df.ethnicity.isin(["Client refused"]).astype(int)
    clean_df['is_latino'] = clean_df.ethnicity.isin(["Hispanic/Latino"]).astype(int)
    clean_df.drop(columns=["ethnicity"], inplace=True)
    # Convert gender into new features
    # NOTE: can be useful in discovering gender discrimination
    clean_df['is_female'] = clean_df.gender.isin(["Female"]).astype(int)
    clean_df['is_male'] = clean_df.gender.isin(["Male"]).astype(int)
    clean_df['is_trans'] = clean_df.gender.isin(["Trans Male (FTM or Female to Male)"]).astype(int)
    clean_df.drop(columns=["gender"], inplace=True)
    # Convert disabling_cond_at_entry into new features
    clean_df["disabling_cond_at_entry_refused"] = clean_df.disabling_cond_at_entry.isin(["Client refused"]).astype(int)
    clean_df["is_disabled_at_entry"] = clean_df.disabling_cond_at_entry.isin(["Yes"]).astype(int)
    clean_df.drop(columns=["disabling_cond_at_entry"], inplace=True)
    # Convert covered_by_health_insurance into new features
    clean_df["covered_by_health_insurance_refused"] = clean_df.covered_by_health_insurance.isin(["Client refused"]).astype(int)
    clean_df["is_covered_by_health_insurance"] = clean_df.covered_by_health_insurance.isin(["Yes"]).astype(int)
    clean_df.drop(columns=["covered_by_health_insurance"], inplace=True)
    # Convert household_type into new features
    clean_df["household_has_adults_children"] = clean_df.household_type.isin(["Household with Adults and Children"]).astype(int)
    clean_df["household_has_adults_only"] = clean_df.household_type.isin(["Household without Children"]).astype(int)
    clean_df.drop(columns=["household_type"], inplace=True)
    # Convert client_record_restricted into numerical binary
    clean_df["client_record_restricted"] = clean_df.client_record_restricted.astype(int)
    # Convert all Y/N columns into 1/0 numerical binary
    clean_df.replace(['Yes', 'No'], [1, 0], inplace=True)

    return clean_df


def lambda_handler(event, context):
    '''Parses database table for only current (non-exited) guests.'''

    # query the database for only rows where null at exit date column as a df 
    query = "SELECT * FROM guests_temp WHERE exit_destination IS NULL AND exit_date IS NULL"
    null_predictions = pd.read_sql_query(query, conn)

    # wrangling df --> resulting in no prediction column
    wrangled_data = wrangle(null_predictions)
    
    # change directory to /tmp
    os.chdir('/tmp')

    # save our csv to /tmp directory
    wrangled_data.to_csv('/tmp/csv_file.csv', index=False)

    # upload csv to bucket as wrangled_guests.csv
    s3.Bucket(s3_bucket).upload_file('/tmp/csv_file.csv', 'wrangled_guests.csv')

    