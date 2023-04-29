import json
import os
import pymssql
import boto3
import time

PORT = 1433  # int(os.environ['DB_PORT'])
USER = "admin"  # os.environ['DB_USER']
PASSWORD = "19xdnbdaZDsJPrGSaNyt"  # os.environ['DB_PASSWORD']
DB_SNAP_SHOT_IDENTIFIER = "backuptestsnapshot"
BACKUP_TARGET = "db-clone-restore-database-temporal"
DATABASE_TO_RESTORE = os.environ["DATABASE_TO_RESTORE"]
S3_BUCKET_BACKUP = os.environ["S3_BUCKET_BACKUP"]
DB_PRODUCTION_RDS_URL = os.environ["DB_PRODUCTION_RDS_URL"]
SQS_QUEUE_URL_TRIGGER = os.environ["SQS_QUEUE_URL_TRIGGER"]

RDSClient = boto3.client("rds")


def lambda_handler(event, context):
    print("-> isAvaileble :: ",isAvaileble("db-poc"))

    connection = pymssql.connect(
        server=DB_PRODUCTION_RDS_URL,
        port=PORT,
        user=USER,
        password=PASSWORD,
        autocommit=True,
    )

    isTaskInProgress(connection)


def isTaskInProgress(conn):
    cursor = conn.cursor()
    cursor.execute("EXEC msdb.dbo.rds_task_status @task_id=0")
    result = cursor.fetchone()
    print("isTaskInProgress", result[5])
    return result[5]


def isAvaileble(rdsInstanceName):
    response = RDSClient.describe_db_instances(DBInstanceIdentifier=rdsInstanceName)

    dbInstances = response["DBInstances"]
    if len(dbInstances) < 1:
        return {"error": "Instances not found", "status": "error", "available": False}

    rdsInstanceStatus = dbInstances[0]["DBInstanceStatus"]

    print(rdsInstanceStatus)
    return {
        "error": "None",
        "status": rdsInstanceStatus,
        "available": rdsInstanceStatus == "available",
    }
