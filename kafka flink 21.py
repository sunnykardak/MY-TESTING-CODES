from pyflink.table import EnvironmentSettings, TableEnvironment

def connect_via_athena():
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    # Replace these with your environment details
    region = "eu-west-2" # London
    athena_staging_s3 = "s3://your-athena-query-results-bucket/"
    data_product_role_arn = "arn:aws:iam::123456789012:role/DataProductAccessRole"
    db_name = "your_db"
    table_name = "your_table"

    # Define the JDBC URL with Role Assumption
    jdbc_url = (
        f"jdbc:awsathena://AwsRegion={region};"
        f"S3OutputLocation={athena_staging_s3};"
        f"CredentialsProvider=com.simba.athena.amazonaws.auth.STSRoleArnCredentialsProvider;"
        f"RoleArn={data_product_role_arn};"
        f"RoleSessionName=FlinkAthenaTest;"
    )

    # Register the Athena table in Flink
    # Use the generic JDBC dialect
    t_env.execute_sql(f"""
        CREATE TABLE athena_test_table (
            -- Define your columns here to match the Iceberg table
            id INT,
            data STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = '{db_name}.{table_name}',
            'driver' = 'com.simba.athena.jdbc.Driver'
        )
    """)

    # Test the connection
    try:
        t_env.execute_sql("SELECT * FROM athena_test_table LIMIT 5").print()
        print("Successfully connected via Athena JDBC!")
    except Exception as e:
        print(f"Connection failed: {e}")

connect_via_athena()


#-----------------------------------------------------------------------

import boto3
# This confirms what identity Flink is currently using
print(boto3.client('sts').get_caller_identity())

# This tests if you can actually "switch" to the data role
sts = boto3.client('sts')
response = sts.assume_role(
    RoleArn="arn:aws:iam::123456789012:role/DataProductAccessRole",
    RoleSessionName="TestingAccess"
)
print("AssumeRole Successful!")
