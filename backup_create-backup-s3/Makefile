lambda_function := backup_create-backup-s3

deploy:
	cp ../dependencies/deployment-package.zip $(lambda_function).zip
	zip -g $(lambda_function).zip lambda_function.py
	aws lambda update-function-code --function-name $(lambda_function) --zip-file fileb://$(lambda_function).zip