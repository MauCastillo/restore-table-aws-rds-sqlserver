import json
import os
import pymssql
import boto3
from datetime import datetime

# os.environ['DB_HOST']

PORT = 1433  # int(os.environ['DB_PORT'])
USER = "admin"  # os.environ['DB_USER']
PASSWORD = "19xdnbdaZDsJPrGSaNyt"  # os.environ['DB_PASSWORD']
DB_INSTANCE_IDENTIFIER = "database-poc-test"
DB_SNAP_SHOT_IDENTIFIER = "database-testing-poc-snapshot"
BACKUP_TARGET_CLONE = os.environ["BACKUP_TARGET_CLONE"]

BACKUP_TARGET = "db-clone-restore-database-temporal"

SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
SQS_DELAY = int(os.environ["SQS_DELAY"])
DATABASE_TO_RESTORE = os.environ["DATABASE_TO_RESTORE"]
S3_BUCKET_BACKUP = os.environ["S3_BUCKET_BACKUP"]

RDSClient = boto3.client("rds")
SQSClient = boto3.client("sqs")
QuerySaveBackup = "exec msdb.dbo.rds_backup_database @source_db_name='%s', @s3_arn_to_backup_to='arn:aws:s3:::%s/%s', @overwrite_S3_backup_file=1;"


def lambda_handler(event, context):
    print(" SQS Event ", event)
    response = RDSClient.describe_db_instances(DBInstanceIdentifier=BACKUP_TARGET)
    dbInstances = response["DBInstances"]
    if len(dbInstances) < 1:
        return {"error": "Instances not found"}

    rdsInstanceURL = dbInstances[0]["Endpoint"]["Address"]
    sqsBody = {}

    records = event["Records"]
    if len(records) > 0:
        sqsBody = json.loads(records[0]["body"])

    try:
        # Configuración de la conexión
        conn = pymssql.connect(
            server=rdsInstanceURL,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=sqsBody["database_restore"],
        )
        cursor = conn.cursor()

        localDate = datetime.now()
        nameBackup = "backup_%s_%s.bak" % (
            sqsBody["database_restore"],
            localDate.strftime("%m_%d_%Y"),
        )

        sqlServerExecute = QuerySaveBackup % (
            sqsBody["database_restore"],
            S3_BUCKET_BACKUP,
            nameBackup,
        )

        print(">>> sqlServerExecute <<<", sqlServerExecute)
        cursor.execute(sqlServerExecute)
        result = cursor.fetchall()

        conn.close()
        print("SQL Server Execution: ", sqlServerExecute)
        print("Result: ", result)
        return result

    except Exception as e:
        # sendSQSMessage()
        print(str(e))
        raise e


def sendSQSMessage():
    messageSQS = {
        "backup-target": BACKUP_TARGET_CLONE,
        "database-restore": DATABASE_TO_RESTORE,
        "db-instance-identifier": DB_INSTANCE_IDENTIFIER,
        "db-snap-shot-identifier": DB_SNAP_SHOT_IDENTIFIER,
    }

    response = SQSClient.send_message(
        QueueUrl=SQS_QUEUE_URL,
        DelaySeconds=SQS_DELAY,
        MessageBody=(json.dumps(messageSQS)),
    )

    print(response["MessageId"])


if __name__ == "__main__":
    event = {
        {
            "Records": [
                {
                    "messageId": "514014ec-9a6c-45ed-adb0-990645d0a0a2",
                    "receiptHandle": "AQEBE8etnBcM+hl2MPe+rnR2OxwdF6uFVZGFmF5IuIaemFFd0AKqk3ZGnrYbwDnFMkfcbNEQJb8o/S0Xy1295lYFY0Z3PvbkmgMqnTrVxiTY5Ar7YQufQQpRVxg9PAHy1VjboqaZiu+UzLzX5KvC9x3oy4VhN/SbCQtW6AWe7xdy2B9SNGUK6R1/Dx3wtPIY4s8T14sTULgG9siKLAskoT4GswVeSbMhO9CFIOSEnsph1sNBbBkQnka48F6SyL/REIibfITo1ZqtDNxtIhqbDjjk3/EACPML/365VzgaAZfjNY7USp1tb92oNe441ZRupLY1X+6nFctZjibWfWNyihpUeXvCuw2d/sLG1Ar4o36YTnBVgQD3919yxjCzdJBjqedGhSnj6UWxCkK8JbiKvq+lSA==",
                    "body": "test backupweek of 12/11/2016.",
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "AWSTraceHeader": "Root=1-6446f995-396320c7187ef97201dbefcb;Parent=512388d51807ff9e;Sampled=0;Lineage=ed3a6528:0",
                        "SentTimestamp": "1682373016226",
                        "SenderId": "AROA5YNG2WHY2SQ2TUS5N:backup_restore-rds-database",
                        "ApproximateFirstReceiveTimestamp": "1682373036226",
                    },
                    "messageAttributes": {
                        "backup-target": {
                            "stringValue": "db-clone-restore-database-temporal",
                            "stringListValues": [],
                            "binaryListValues": [],
                            "dataType": "String",
                        },
                        "db-instance-identifier": {
                            "stringValue": "database-poc-test",
                            "stringListValues": [],
                            "binaryListValues": [],
                            "dataType": "String",
                        },
                        "database-restore": {
                            "stringValue": "testing_database_restore",
                            "stringListValues": [],
                            "binaryListValues": [],
                            "dataType": "String",
                        },
                        "db-snap-shot-identifier": {
                            "stringValue": "recovery-test",
                            "stringListValues": [],
                            "binaryListValues": [],
                            "dataType": "String",
                        },
                    },
                    "md5OfBody": "929d4a2b2504c89517d815dd975d93fd",
                    "md5OfMessageAttributes": "6cd0a92926cd38179cffcb9466a1f507",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:945779552753:restore-backup-database",
                    "awsRegion": "us-east-1",
                }
            ]
        }
    }
    context = {}
    result = lambda_handler(event, context)
    print("lambda_handler: ", result)
