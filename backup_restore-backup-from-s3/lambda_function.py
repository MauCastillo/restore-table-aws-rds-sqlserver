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
SQSClient = boto3.client("sqs")
S3Client = boto3.resource("s3")

QUERY_DELETE_DATABASE = (
    "IF EXISTS (SELECT name FROM sys.databases WHERE name = '%s') DROP DATABASE %s; "
)
QueryRestore = "EXEC msdb.dbo.rds_restore_database @restore_db_name='%s', @s3_arn_to_restore_from='arn:aws:s3:::%s/%s';"


def lambda_handler(event, context):
    # Receive messages from the SQS queue
    print(" >>> La cola <<< ", SQS_QUEUE_URL_TRIGGER)
    response = SQSClient.receive_message(
        QueueUrl=SQS_QUEUE_URL_TRIGGER, MaxNumberOfMessages=1
    )

    print(" >>> NO siguio <<< ")

    # Process the received messages
    messages = response.get("Messages", [])
    print(" >>> message <<< ", len(messages))

    for message in messages:
        if isAvaileble(BACKUP_TARGET)["available"] == False:
            raise Exception("rds instance not available, %s" % BACKUP_TARGET)

        sqsBody = json.loads(message["Body"])
        print("____________________________")
        print(sqsBody)
        print("____________________________")
        if isAvaileble(sqsBody["db-instance-identifier"])["available"] == False:
            Exception(
                "rds instance not available, %s" % sqsBody["db-instance-identifier"]
            )

        connection = pymssql.connect(
            server=DB_PRODUCTION_RDS_URL,
            port=PORT,
            user=USER,
            password=PASSWORD,
            autocommit=True,
        )

        cursor = connection.cursor()
        restore_sql = QueryRestore % (
            sqsBody["database_restore"],
            S3_BUCKET_BACKUP,
            sqsBody["backup_name"],
        )

        print(">>> Cerrando las conexiones a la base de datos")
        # Disconnect all users and applications connected to the database
        cursor.execute(
            "USE "
            + sqsBody["database_restore"]
            + "; ALTER DATABASE "
            + sqsBody["database_restore"]
            + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
        )

        # Drop the database
        print(">>> Eliminando Base de datos")
        deleteQuery = QUERY_DELETE_DATABASE % (
            sqsBody["database_restore"],
            sqsBody["database_restore"],
        )

        cursor.execute(deleteQuery)
        time.sleep(30)

        print(restore_sql)
        print("===> ejecutando La restauracion :-)")
        cursor.execute(restore_sql)
        connection.commit()
        print(">>> La restauracion a iniciado...")
        connection.close()
        print("____________________________")
        print(message)
        print("____________________________")
        # Delete the message from the queue
        receipt_handle = message["ReceiptHandle"]

        SQSClient.delete_message(
            QueueUrl=SQS_QUEUE_URL_TRIGGER, ReceiptHandle=receipt_handle
        )

    print({"status": "success", "message": "In Progress"})


def isAvaileble(rdsInstanceName):
    print(">> isAvaileble(rdsInstanceName) <<", rdsInstanceName)
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


if __name__ == "__main__":
    print("cambio")
    result = lambda_handler({}, {})
    print("lambda_handler: ", result)
