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
QueryListTask = "exec msdb.dbo.rds_task_status @task_id=%s;"


def lambda_handler(event, context):
    response = RDSClient.describe_db_instances(DBInstanceIdentifier=BACKUP_TARGET)
    records = event["Records"]
    if len(records) <= 0:
        return {"error":"SQS Message not found"}

    sqsBody = json.loads(records[0]["body"])
    conn = pymssql.connect(
        server=sqsBody["url_rds_instances"],
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=sqsBody["database_restore"],
    )
    cursor = conn.cursor()
    query = QueryListTask % sqsBody["task_id"]
    print("query jaja",query)

    cursor.execute(query)
    result = cursor.fetchone()
    if len(result) <= 4:
        return {"error":"SQS Message not found"}
    
    print("lifecycle",str(result[5]))
    taskID = str(result[5])
    conn.close()

    return result


if __name__ == "__main__":
    event = {
        "Records": [
            {
                "messageId": "8a9ecb04-f7c2-4c72-ac99-6d33655a0f55",
                "receiptHandle": "AQEBIupDkoFE/jd4wS0Y9bkXdJaW4Z4BTbP6X2xRAVb98dV8Ppx4FBUTkAefu6ROZ2tgCn970GprZKfuW9znhOe6ao2WeOIpqnJhXqdaOtAK1nTW27UI5UR3Dqr0e/EMO53OwixzcfImv5P6jndoQYbXZGwKsfG7iDO9PUHvrl70NAt1Niz0RGAQEZZZhPJUyrhRsOGh24tQigbq3T1G7MNJ0LWVMN7mLyNjz1NjYPfPKTAqxEYe97mKpjZrVuHx2vffvCi5VLBasfUiiKrxnGpa7Dn6hWIxDshOJMOVkuBj4J+J35ivZrWiDd6aSYCON7e63Khil4kktyZXiQGHLi68AnadDu8UsiuKeRHH8tuONUigaThDGUiwBAZdGDg8OifI9+klItJIVfMxOY4Uswkg2g==",
                "body": '{"task_id":"11", "url_rds_instances": "db-clone-restore-database-temporal.ckkscxdfuhqg.us-east-1.rds.amazonaws.com", "backup_target_clone": "db-clone-restore-database-temporal", "database_restore": "testing_database_restore", "db_instance_identifier": "database-poc-test", "db_snapshot_identifier": "recovery-test"}',
                "attributes": {
                    "ApproximateReceiveCount": "2",
                    "AWSTraceHeader": "Root=1-6447018c-62cb3d2d583ece3120a3f9f5;Parent=0296019439621060;Sampled=0;Lineage=ed3a6528:0",
                    "SentTimestamp": "1682375056364",
                    "SenderId": "AROA5YNG2WHY2SQ2TUS5N:backup_restore-rds-database",
                    "ApproximateFirstReceiveTimestamp": "1682375076364",
                },
                "messageAttributes": {},
                "md5OfBody": "935103e11e8b7a8cfeb5b1203185d020",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:945779552753:restore-backup-database",
                "awsRegion": "us-east-1",
            }
        ]
    }

    context = {}
    result = lambda_handler(event, context)
    print("lambda_handler: ", result)
