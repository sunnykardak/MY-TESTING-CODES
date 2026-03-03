import boto3
import json

def main():
    print("Starting FDP access test...")

    sts = boto3.client("sts")
    identity = sts.get_caller_identity()

    print("===== CURRENT IDENTITY =====")
    print(json.dumps(identity, indent=2))

    glue = boto3.client("glue", region_name="eu-west-2")

    print("===== LISTING DATABASES =====")
    response = glue.get_databases()

    for db in response.get("DatabaseList", []):
        print("Database:", db["Name"])

    print("===== CHECKING TARGET DATABASE =====")

    try:
        db_name = "fdk_uk_customer_db_iceberg"
        result = glue.get_database(Name=db_name)
        print("SUCCESS - Found database:", result["Database"]["Name"])
    except Exception as e:
        print("FAILED to access target database")
        print(str(e))

if __name__ == "__main__":
    main()
