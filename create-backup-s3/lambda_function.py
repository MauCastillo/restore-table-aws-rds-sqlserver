import json
import os
import pymssql
import boto3
from datetime import datetime

# os.environ['DB_HOST']
S3_BUCKET_BACKUP = "sql-backup-2023"
SERVER = "db-clone-restore-database-temporal.ckkscxdfuhqg.us-east-1.rds.amazonaws.com"
PORT = 1433  # int(os.environ['DB_PORT'])
# SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
USER = "admin"  # os.environ['DB_USER']
PASSWORD = "mina5412"  # os.environ['DB_PASSWORD']
DATABASE_RESTORE = "test-database-restore"  # os.environ['DATABASE_RESTORE']
DB_INSTANCE_IDENTIFIER = "database-poc-test"
DB_SNAP_SHOT_IDENTIFIER = "database-testing-poc-snapshot"
QUERY_SAVE_BACKUP = "exec msdb.dbo.rds_backup_database @source_db_name='%s', @s3_arn_to_backup_to='arn:aws:s3:::%s/%s', @overwrite_S3_backup_file=1;"
BACKUP_TARGET = "db-clone-restore-database-temporal"
PREFIX_CLONE = "CLONE"


def lambda_handler(event, context):
    rdsClient = boto3.client("rds")
    response = rdsClient.describe_db_instances(DBInstanceIdentifier=BACKUP_TARGET)
    dbInstances = response["DBInstances"]
    if len(dbInstances) < 1:
        return {"error": "Instances not found"}

    rdsInstanceURL = dbInstances[0]["Endpoint"]["Address"]

    records = event["Records"]
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
            database=DATABASE_RESTORE,
        )
        cursor = conn.cursor()

        localDate = datetime.now()
        nameBackup = "backup_%s_%s.bak" % (
            DATABASE_RESTORE,
            localDate.strftime("%m_%d_%Y"),
        )
        sqlServerExecute = QUERY_SAVE_BACKUP % (
            DATABASE_RESTORE,
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
        print(str(e))
        raise e


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
