//PREREQUISITES
--set up aws IAM role and policy
-----------------------------------

//SETTING UP SCHEMA, STORAGE INTEGRATION, EXTERNAL STAGE (INC. FILE FORMAT), AND RAW TABLE STRUCTURE
-----------------------------------
CREATE SCHEMA amplitude-schema;

CREATE OR REPLACE STORAGE INTEGRATION amplitude-s3-integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<aws-role-arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/path/to/data/');

--obtain and paste STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into AWS Role Policy
DESC INTEGRATION amplitude-s3-integration;

CREATE OR REPLACE FILE FORMAT amplitude-file-format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE STAGE amplitude-stage
  STORAGE_INTEGRATION = amplitude-s3-integration
  URL = 's3://my-bucket/path/to/data/'
  FILE_FORMAT = amplitude-file-format;

LIST @amplitude-stage;

CREATE OR REPLACE TABLE amplitude_events_raw (
  json_data VARIANT,
  filename VARCHAR
);

//LOAD DATA FROM STAGE INTO RAW TABLE
-----------------------------------
COPY INTO amplitude_events_raw
FROM
    (SELECT
        $1,
        METADATA$FILENAME
    FROM @amplitude-stage)
FILE_FORMAT = (FORMAT_NAME = amplitude-file-format);

SELECT *
FROM amplitude_events_raw;

//CHECK THE NUMBER OF FILE NAMES MATCHES THE AMOUNT SHOWN IN S3
-----------------------------------
SELECT DISTINCT FILENAME FROM amplitude_events_raw;
