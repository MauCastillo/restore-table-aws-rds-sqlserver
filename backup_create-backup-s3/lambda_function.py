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
    print(">>>>>>>>>>>>>>>>>>>>>>> SQS Event <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", event)
    response = RDSClient.describe_db_instances(DBInstanceIdentifier=BACKUP_TARGET)
    dbInstances = response["DBInstances"]
    if len(dbInstances) < 1:
        return {"error": "Instances not found"}

    rdsInstanceURL = dbInstances[0]["Endpoint"]["Address"]

    records = event["Records"]
    print("even mock::::::", records)
    for record in records:
        content = record["body"]
        print(content)

    try:
        # Configuración de la conexión
        conn = pymssql.connect(
            server=rdsInstanceURL,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE_TO_RESTORE,
        )
        cursor = conn.cursor()

        localDate = datetime.now()
        nameBackup = "backup_%s_%s.bak" % (
            DATABASE_TO_RESTORE,
            localDate.strftime("%m_%d_%Y"),
        )

        sqlServerExecute = QuerySaveBackup % (
            DATABASE_TO_RESTORE,
            S3_BUCKET_BACKUP,
            nameBackup,
        )

        print(">>> sqlServerExecute <<<", sqlServerExecute)
        cursor.execute(sqlServerExecute)
        result = cursor.fetchall()

        conn.close()
        print("SQL Server Execution: ", sqlServerExecute)
        print("SQS content: ", json.dumps(content))
        print("Result: ", result)
        return result

    except Exception as e:
        sendSQSMessage()
        print(str(e))
        raise e


def sendSQSMessage():

    response = SQSClient.send_message(
        QueueUrl=SQS_QUEUE_URL,
        DelaySeconds=SQS_DELAY,
        MessageAttributes={
            "backup-target": {"DataType": "String", "StringValue": BACKUP_TARGET_CLONE},
            "database-restore": {
                "DataType": "String",
                "StringValue": DATABASE_TO_RESTORE,
            },
            "db-instance-identifier": {
                "DataType": "String",
                "StringValue": DB_INSTANCE_IDENTIFIER,
            },
            "db-snap-shot-identifier": {
                "DataType": "String",
                "StringValue": DB_SNAP_SHOT_IDENTIFIER,
            },
        },
        MessageBody=("test backup" "week of 12/11/2016."),
    )

    print(response["MessageId"])


if __name__ == "__main__":
    event = {
        "Records": [
            {
                "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
                "receiptHandle": "MessageReceiptHandle",
                "body": "Hello from SQS!",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1523232000000",
                    "SenderId": "123456789012",
                    "ApproximateFirstReceiveTimestamp": "1523232000001",
                },
                "messageAttributes": {},
                "md5OfBody": "{{{md5_of_body}}}",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
                "awsRegion": "us-east-1",
            }
        ]
    }
    context = {}
    result = lambda_handler(event, context)
    print("lambda_handler: ", result)