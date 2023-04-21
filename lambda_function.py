import json
import os
import pymssql
import boto3
import datetime

# os.environ['DB_HOST']
S3_BUCKET_BACKUP = "sql-backup-2023"
SERVER = "database-poc-test.ckkscxdfuhqg.us-east-1.rds.amazonaws.com"
PORT = 1433  # int(os.environ['DB_PORT'])
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
USER = "admin"  # os.environ['DB_USER']
PASSWORD = "19xdnbdaZDsJPrGSaNyt"  # os.environ['DB_PASSWORD']
DATABASE_RESTORE = "testing_database"  # os.environ['DATABASE_RESTORE']
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
            database=DATABASE_RESTORE
        )
        cursor = conn.cursor()
        
        localDate = datetime.now()
        nameBackup = "backup_%s_%s.bak" % (DATABASE_RESTORE,localDate.strftime('%m_%d_%Y'))
        queryExecute= QUERY_SAVE_BACKUP % (DATABASE_RESTORE,S3_BUCKET_BACKUP )
        cursor.execute(QUERY_SAVE_BACKUP)
        result = cursor.fetchall()

        conn.close()
        return result

    except Exception as e:
        print(str(e))
        raise e




if __name__ == '__main__':
    event = {}
    context = {}
    result = lambda_handler(event, context)
    print("lambda_handler: ", result)
