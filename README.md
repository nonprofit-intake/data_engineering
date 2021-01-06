# Family Promise of Spokane Data Engineering
## Project Overview

Collection of serverless functions for guest intake system.

- add_predictions_to_non_exited_guests: updates predicted_exit_destination column of guests table using results from LightGBM classification model
  - Function 1:
  - Function 2:
  - Function 3:
- remove_predictions_from_exited_guests: checks if predicted_exit_destination column exists and creates it if necessary and updates predicted_exit_destination to account for guests that have exited


## Tech Stack
Languages: Python, SQL

Dependencies: pandas, numpy, psycopg2, pickle, boto3, 

Services: AWS, Docker, Jupyter Notebooks, Postman

Backend: AWS API Gateway, AWS Lambda, ElephantSQL

## Getting Started
### Prerequisites
- SQL
- AWS Lambda and AWS API Gateway
- PostgreSQL

### Deployment to AWS
#### Developer environment
Build Amazon Linux image with Python 3.7 and pip

```docker build -t example_image_name .```

#### Installing dependencies

All dependencies are already installed, but if for some reason you needed to delete and reinstall:

```docker run -v $(pwd):/aws -ti example_image_name```

then

```pip install bcrypt aws-psycopg2 pandas -t /aws```

Do not install if these packages already exist in the aws folder.

#### Packaging Lambda Function
```zip -r example_filename.zip *```

At this point you'll want to head over the AWS GUI for function creation at AWS Lambda. 

## Development
### Architecture
<img src="https://github.com/nonprofit-intake/family_promise_data_sharing/blob/dev/images/fampromarch.png" width="500" height="350">
Backend deployed serverlessly through AWS API Gateway and AWS Lambda.

### Endpoint - Return User Info

**URL**

https://3yk0fzdvdh.execute-api.us-east-1.amazonaws.com/default/return_user_info

**Description**

Returns the last name, first name, enroll date, exit date, income at entry, income at exit, and exit destination of a user in database.

**POST Request**
```{
    "last_name": [
    string, 
    string
    ],
    "ssn": [
    integer, 
    integer
    ],
    "pwd": string
}
```

**Response**
```{
    'complete_matches': [
        {
            'enroll_date': string,
            'exit_date': string,
            'exit_destination': string,
            'first_name': string,
            'income_at_entry': float,
            'income_at_exit': float,
            'last_name': string
        }
    ],
   'partial_matches': [
       {
           'enroll_date': string,
           'exit_date': string,
           'exit_destination': string,
           'first_name': string,
           'income_at_entry': float,
           'income_at_exit': float,
           'last_name': string
        }
    ]
}
```

#### AWS Environment Variables
- HOST = database URL
- USER = username
- PASSWORD = password
- AUTH_PWD = secret key
