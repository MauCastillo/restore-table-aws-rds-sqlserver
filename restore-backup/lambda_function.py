
import boto3
import os


DB_INSTANCE_IDENTIFIER = 'database-poc-test'
DB_SNAP_SHOT_IDENTIFIER = 'database-testing-poc-snapshot'
DATABASE_RESTORE = "testing_database"  # os.environ['DATABASE_RESTORE']
BACKUP_TARGET = 'db-clone-restore-database-temporal'
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']

def lambda_handler(event, context):
    try:
        result = resotoreBackup()
        return result

    except Exception as e:
        print(str(e))
        raise e


def resotoreBackup():
    rdsClient = boto3.client('rds')
    response = rdsClient.restore_db_instance_from_db_snapshot(DBInstanceIdentifier=BACKUP_TARGET,
                                                           DBSnapshotIdentifier=DB_SNAP_SHOT_IDENTIFIER,
                                                           PubliclyAccessible=True,
                                                           Tags=[
                                                               {
                                                                   'Key': 'Serivice',
                                                                   'Value': 'restoro temporal backup'
                                                               },
                                                           ],)

    return response


def sendSQSMessage():
    # Create SQS client
    sqs = boto3.client('sqs')

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        DelaySeconds=10000,
        MessageAttributes={
            'backup-target': {
                'DataType': 'String',
                'StringValue': BACKUP_TARGET
            },
            'database-restore': {
                'DataType': 'String',
                'StringValue': DATABASE_RESTORE
            },
            'db-instance-identifier': {
                'DataType': 'String',
                'StringValue': DB_INSTANCE_IDENTIFIER
            },
            'db-snap-shot-identifier': {
                'DataType': 'String',
                'StringValue': DB_SNAP_SHOT_IDENTIFIER
            },
        },
        MessageBody=(
            'test backup'
            'week of 12/11/2016.'
        )
    )

    print(response['MessageId'])
