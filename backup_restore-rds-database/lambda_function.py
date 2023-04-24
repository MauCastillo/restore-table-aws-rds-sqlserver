import boto3
import os


DB_INSTANCE_IDENTIFIER = "database-poc-test"
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
        sendSQSMessage()
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
            "sg-04343444ece6df193",
            VPC_SECURITY_GROUP_ID,
        ],
        Tags=[
            {"Key": "Serivice", "Value": "restoro temporal backup"},
        ],
    )

    return response


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
