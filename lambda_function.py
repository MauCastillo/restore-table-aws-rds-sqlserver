import json
import os
import pymssql
import boto3
import datetime

# os.environ['DB_HOST']
S3_BUCKET_BACKUP = "sql-backup-2023"
SERVER = "database-poc-test.ckkscxdfuhqg.us-east-1.rds.amazonaws.com"
PORT = 1433  # int(os.environ['DB_PORT'])
USER = "admin"  # os.environ['DB_USER']
PASSWORD = "19xdnbdaZDsJPrGSaNyt"  # os.environ['DB_PASSWORD']
DATABASE = "testing_database"  # os.environ['DB_NAME']
DB_INSTANCE_IDENTIFIER = 'database-poc-test'
DB_SNAP_SHOT_IDENTIFIER = 'database-testing-poc-snapshot'
QUERY_SAVE_BACKUP = "exec msdb.dbo.rds_backup_database @source_db_name='%s', @s3_arn_to_backup_to='arn:aws:s3:::%s/%s', @overwrite_S3_backup_file=1;"
BACKUP_TARGET = 'db-clone-restore-databaser-temporal'
PREFIX_CLONE = 'CLONE'


def lambda_handler(event, context):
    try:
        # Configuración de la conexión
        conn = pymssql.connect(
            server=SERVER,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        cursor = conn.cursor()
        
        localDate = datetime.now()
        date = localDate.strftime('%m-%d-%Y')
        nameBackup = "backup_%s_%s.bak" % (DATABASE,date)
        queryExecute= QUERY_SAVE_BACKUP % (DATABASE,S3_BUCKET_BACKUP )
        cursor.execute(QUERY_SAVE_BACKUP)
        result = cursor.fetchall()

        conn.close()
        return result

    except Exception as e:
        print(str(e))
        raise e


def resotoreBackup():
    rdsClient = boto3.client('rds')
    response = rdsClient.restore_db_instance_from_db_snapshot(DBInstanceIdentifier="temporaRestore",
                                                           DBSnapshotIdentifier=DB_SNAP_SHOT_IDENTIFIER,
                                                           PubliclyAccessible=True,
                                                           Tags=[
                                                               {
                                                                   'Key': 'Serivice',
                                                                   'Value': 'restoro temporal backup'
                                                               },
                                                           ],)

    return response


if __name__ == '__main__':
    event = {}
    context = {}
    # print(resotoreBackup())
    result = lambda_handler(event, context)
    print("lambda_handler: ", result)
