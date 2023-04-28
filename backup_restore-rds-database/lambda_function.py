import boto3
import json
import os


DB_INSTANCE_IDENTIFIER = "db-poc"
DB_SNAP_SHOT_IDENTIFIER = os.environ["DB_SNAP_SHOT_IDENTIFIER"]
OPTION_GROUP_NAME = os.environ["OPTION_GROUP_NAME"]
DATABASE_TO_RESTORE = os.environ["DATABASE_TO_RESTORE"]
BACKUP_TARGET_CLONE = os.environ["BACKUP_TARGET_CLONE"]
VPC_SECURITY_GROUP_ID = os.environ["VPC_SECURITY_GROUP_ID"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
SQS_DELAY = int(os.environ["SQS_DELAY"])

RDSClient = boto3.client("rds")
SQSClient = boto3.client("sqs")


def lambda_handler(event, context):
    try:
        result = resotoreBackup()
        return result

    except Exception as e:
        print(str(e))
        raise e


def resotoreBackup():
    response = RDSClient.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=BACKUP_TARGET_CLONE,
        DBSnapshotIdentifier=DB_SNAP_SHOT_IDENTIFIER,
        PubliclyAccessible=True,
        VpcSecurityGroupIds=[
            VPC_SECURITY_GROUP_ID,
        ],
        Tags=[
            {"Key": "Serivice", "Value": "restoro temporal backup"},
        ],
    )

    sendSQSMessage()
    return response


def sendSQSMessage():

    messageSQS = {
        "backup_target_clone": BACKUP_TARGET_CLONE,
        "database_restore": DATABASE_TO_RESTORE,
        "db_instance_identifier": DB_INSTANCE_IDENTIFIER,
        "db_snapshot_identifier": DB_SNAP_SHOT_IDENTIFIER,
    }

    response = SQSClient.send_message(
        QueueUrl=SQS_QUEUE_URL,
        DelaySeconds=SQS_DELAY,
        MessageBody=json.dumps(messageSQS),
    )
    
    return response


if __name__ == "__main__":
    event = {}
    context = {}
    result = lambda_handler(event, context)
    print("lambda_handler: ", result)