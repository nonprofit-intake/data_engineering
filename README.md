# Family Promise of Spokane Data Engineering

## Project Overview
Collection of serverless functions for guest intake system.

- **add_predictions_to_non_exited_guests**: multiple functions; updates predicted_exit_destination column of guests table using results from LightGBM classification model
  - Function 1: performs query to retrieve new guest data, wrangles it for modeling, and stores wrangled data in S3
  - Function 2: retrieves wrangled data, runs it through a pickled model, and stores predicted results in S3
  - Function 3: Retrieves prediction data and uploads results to guest database
- **remove_predictions_from_exited_guests**: single function; checks if predicted_exit_destination column exists and creates it if necessary and updates predicted_exit_destination to account for guests that have exited

## Tech Stack
**Languages**: Python, SQL

**Dependencies**: Pandas, NumPy, psycopg2, pickle, Boto3, LightGBM

**Services**: Docker, AWS API Gateway, AWS Lambda, AWS S3, AWS CloudWatch, ElephantSQL, PostgreSQL

## Architecture
<img src="https://github.com/nonprofit-intake/family_promise_data_sharing/blob/dev/images/fampromarch.png" width="500" height="350">

## Getting Started
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

#### AWS Environment Variables
- HOST = database URL
- USER = username
- PASSWORD = password
- AUTH_PWD = secret key

## License
MIT
