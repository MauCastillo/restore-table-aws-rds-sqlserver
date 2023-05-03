SHELL := /bin/bash
lambda_function := backup_restore-backup-from-s3

export S3_BUCKET_BACKUP=backup-sql-server-unique-database
export SQS_QUEUE_URL_TRIGGER=https://sqs.us-east-1.amazonaws.com/945779552753/start-restore-backup-rds
deploy:
	cp ../dependencies/deployment-package.zip $(lambda_function).zip
	zip -g $(lambda_function).zip lambda_function.py
	aws lambda update-function-code --function-name $(lambda_function) --zip-file fileb://$(lambda_function).zip

run:
	python3 lambda_function.py 