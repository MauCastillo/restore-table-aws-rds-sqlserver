import json
import os
import time
import pymssql
import boto3
import random
import string

from datetime import datetime

# os.environ['DB_HOST']

PORT = 1433  # int(os.environ['DB_PORT'])
USER = "admin"  # os.environ['DB_USER']
PASSWORD = "19xdnbdaZDsJPrGSaNyt"  # os.environ['DB_PASSWORD']
DB_INSTANCE_IDENTIFIER = "db-poc"
DB_SNAP_SHOT_IDENTIFIER = "backuptestsnapshot"
BACKUP_TARGET_CLONE = os.environ["BACKUP_TARGET_CLONE"]

BACKUP_TARGET = "db-clone-restore-database-temporal"

SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
SQS_QUEUE_URL_TRIGGER = os.environ["SQS_QUEUE_URL_TRIGGER"]
SQS_DELAY = int(os.environ["SQS_DELAY"])
DATABASE_TO_RESTORE = os.environ["DATABASE_TO_RESTORE"]
S3_BUCKET_BACKUP = os.environ["S3_BUCKET_BACKUP"]

RDSClient = boto3.client("rds")
SQSClient = boto3.client("sqs")

QuerySaveBackup = "exec msdb.dbo.rds_backup_database @source_db_name='%s', @s3_arn_to_backup_to='arn:aws:s3:::%s/%s', @overwrite_S3_backup_file=1;"
tokenSize = 5


def lambda_handler(event, context):
    nameBackup = ""
    rdsInstanceCloneURL = ""

    taskStatus = ""
    sns_message = {}

    for record in event["Records"]:
        sns_message = json.loads(record["Sns"]["Message"])

    if sns_message["Source ID"] != "db-clone-restore-database-temporal":
        return {"status": "error", "message": "instance not recovery"}
    try:
        if isAvaileble(BACKUP_TARGET)["available"] == False:
            raise Exception("rds instance not available, %s" % BACKUP_TARGET)

        rdsInstanceCloneURL = getURLinstance(BACKUP_TARGET)

        # Receive messages from the SQS queue
        sqsRespose = SQSClient.receive_message(
            QueueUrl=SQS_QUEUE_URL_TRIGGER, MaxNumberOfMessages=1
        )
        messages = sqsRespose.get("Messages", [])

        # Process the received messages
        for message in messages:
            sqsBody = json.loads(message["Body"])
            connection = pymssql.connect(
                server=rdsInstanceCloneURL,
                port=PORT,
                user=USER,
                password=PASSWORD,
                database=sqsBody["database_restore"],
            )
            cursor = connection.cursor()

            localDate = datetime.now()
            nameBackup = "backup_%s_%s_%s.bak" % (
                sqsBody["database_restore"],
                localDate.strftime("%m_%d_%Y"),
                token(tokenSize),
            )

            sqlServerExecute = QuerySaveBackup % (
                sqsBody["database_restore"],
                S3_BUCKET_BACKUP,
                nameBackup,
            )


            cursor.execute(sqlServerExecute)
            connection.commit()

            taskStatus = isTaskInProgress(connection)
            # Delete the message from the queue
            receipt_handle = message["ReceiptHandle"]
            response = SQSClient.delete_message(
                QueueUrl=SQS_QUEUE_URL_TRIGGER, ReceiptHandle=receipt_handle
            )

            connection.close()

        sendSQSMessage(rdsInstanceCloneURL, nameBackup)
        print(
            {"status": "success", "message": "In Progress", "task_status": taskStatus}
        )

    except Exception as e:
        print({"status": "error", "message": "{}".format(e)})
        raise Exception(e)

def getURLinstance(instanceIdententifier):
        response = RDSClient.describe_db_instances(DBInstanceIdentifier=instanceIdententifier)
        dbInstances = response["DBInstances"]
        if len(dbInstances) < 1:
            return ""

        return dbInstances[0]["Endpoint"]["Address"]

def isAvaileble(rdsInstanceName):
    time.sleep(240)
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


def token(lenght):
    letters = string.ascii_lowercase + string.digits
    return "".join(random.choice(letters) for i in range(lenght))


def isTaskInProgress(conn):
    cursor = conn.cursor()
    cursor.execute("EXEC msdb.dbo.rds_task_status @task_id=0")
    result = cursor.fetchone()
    print("isTaskInProgress", result[5])
    return result[5]


def sendSQSMessage(urlRdsInstance, backupName):
    messageSQS = {
        "backup_target": BACKUP_TARGET_CLONE,
        "database_restore": DATABASE_TO_RESTORE,
        "db_instance_identifier": DB_INSTANCE_IDENTIFIER,
        "db_snap_shot_identifier": DB_SNAP_SHOT_IDENTIFIER,
        "url_rds_instances": urlRdsInstance,
        "backup_name": backupName,
    }

    response = SQSClient.send_message(
        QueueUrl=SQS_QUEUE_URL,
        DelaySeconds=SQS_DELAY,
        MessageBody=(json.dumps(messageSQS)),
    )

    print(response["MessageId"])


if __name__ == "__main__":
    event = {
        "Records": [
            {
                "messageId": "8a9ecb04-f7c2-4c72-ac99-6d33655a0f55",
                "receiptHandle": "AQEBIupDkoFE/jd4wS0Y9bkXdJaW4Z4BTbP6X2xRAVb98dV8Ppx4FBUTkAefu6ROZ2tgCn970GprZKfuW9znhOe6ao2WeOIpqnJhXqdaOtAK1nTW27UI5UR3Dqr0e/EMO53OwixzcfImv5P6jndoQYbXZGwKsfG7iDO9PUHvrl70NAt1Niz0RGAQEZZZhPJUyrhRsOGh24tQigbq3T1G7MNJ0LWVMN7mLyNjz1NjYPfPKTAqxEYe97mKpjZrVuHx2vffvCi5VLBasfUiiKrxnGpa7Dn6hWIxDshOJMOVkuBj4J+J35ivZrWiDd6aSYCON7e63Khil4kktyZXiQGHLi68AnadDu8UsiuKeRHH8tuONUigaThDGUiwBAZdGDg8OifI9+klItJIVfMxOY4Uswkg2g==",
                "body": '{"backup_name": "backup_testing_database_04_26_2023.bak","backup_target_clone": "db-clone-restore-database-temporal", "database_restore": "testing_database", "db_instance_identifier": "db-poc", "db_snapshot_identifier": "backuptestsnapshot"}',
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
