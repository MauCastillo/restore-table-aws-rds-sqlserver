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

SQS_DELAY = int(os.environ["SQS_DELAY"])
DATABASE_TO_RESTORE = os.environ["DATABASE_TO_RESTORE"]
S3_BUCKET_BACKUP = os.environ["S3_BUCKET_BACKUP"]
DB_PRODUCTION_RDS_URL = os.environ["DB_PRODUCTION_RDS_URL"]
SQS_QUEUE_URL_TRIGGER = os.environ["SQS_QUEUE_URL_TRIGGER"]

RDSClient = boto3.client("rds")
SQSClient = boto3.client("sqs")
S3Client = boto3.resource("s3")

QueryRemoveDatabase = "DROP DATABASE %s;"
QueryRestore = "EXEC msdb.dbo.rds_restore_database @restore_db_name='%s', @s3_arn_to_restore_from='arn:aws:s3:::%s/%s';"


def lambda_handler(event, context):
    sqsBody = {}
    records = event["Records"]
    for message in records:
        try:
            sqsBody = json.loads(message["body"])
            S3Client.Object(S3_BUCKET_BACKUP, sqsBody["backup_name"]).load()

            backup_file = "/tmp/backup.bak"
            database_restore = sqsBody["database_restore"]

            conn = pymssql.connect(
                server=DB_PRODUCTION_RDS_URL,
                port=PORT,
                user=USER,
                password=PASSWORD,
            )

            end_transaction(conn)

            cursor = conn.cursor()
            restore_sql = QueryRestore % (
                sqsBody["database_restore"],
                S3_BUCKET_BACKUP,
                sqsBody["backup_name"],
            )

            print(restore_sql)
            cursor.execute(restore_sql)
            conn.commit()
            conn.close()

        except Exception as e:
            print("Error al procesar el mensaje: {}".format(e))
            time.sleep(10 * 60)
            continue

        # Delete the message from the queue
        receipt_handle = message['receiptHandle']
        response = SQSClient.delete_message(
            QueueUrl=SQS_QUEUE_URL_TRIGGER,
            ReceiptHandle=receipt_handle
        )

# End transaction before executing the stored procedure
def end_transaction(conn):
    cursor = conn.cursor()
    cursor.execute('COMMIT')

if __name__ == "__main__":
    event = {
        "Records": [
            {
                "messageId": "8a9ecb04-f7c2-4c72-ac99-6d33655a0f55",
                "receiptHandle": "AQEBIupDkoFE/jd4wS0Y9bkXdJaW4Z4BTbP6X2xRAVb98dV8Ppx4FBUTkAefu6ROZ2tgCn970GprZKfuW9znhOe6ao2WeOIpqnJhXqdaOtAK1nTW27UI5UR3Dqr0e/EMO53OwixzcfImv5P6jndoQYbXZGwKsfG7iDO9PUHvrl70NAt1Niz0RGAQEZZZhPJUyrhRsOGh24tQigbq3T1G7MNJ0LWVMN7mLyNjz1NjYPfPKTAqxEYe97mKpjZrVuHx2vffvCi5VLBasfUiiKrxnGpa7Dn6hWIxDshOJMOVkuBj4J+J35ivZrWiDd6aSYCON7e63Khil4kktyZXiQGHLi68AnadDu8UsiuKeRHH8tuONUigaThDGUiwBAZdGDg8OifI9+klItJIVfMxOY4Uswkg2g==",
                "body": '{"backup_name":"backup_testing_database_04_24_2023.bak" ,"task_id":"11", "url_rds_instances": "db-clone-restore-database-temporal.ckkscxdfuhqg.us-east-1.rds.amazonaws.com", "backup_target_clone": "db-clone-restore-database-temporal", "database_restore": "testing_database", "db_instance_identifier": "db-poc", "db_snapshot_identifier": "backuptestsnapshot"}',
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
