SHELL := /bin/bash
lambda_function := backup_create-backup-s3

export S3_BUCKET_BACKUP=backup-sql-server-unique-database
export DATABASE_TO_RESTORE=testing_database_restore
export SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/945779552753/start-restore-backup-rds
export BACKUP_TARGET_CLONE=db-clone-restore-database-temporal
export VPC_SECURITY_GROUP_ID=sg-04343444ece6df193
export SQS_DELAY=30


deploy:
	cp ../dependencies/deployment-package.zip $(lambda_function).zip
	zip -g $(lambda_function).zip lambda_function.py
	aws lambda update-function-code --function-name $(lambda_function) --zip-file fileb://$(lambda_function).zip

run:
	python3 lambda_function.py 