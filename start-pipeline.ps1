# This script automates the startup of the entire data pipeline.

Write-Host "--- Step 1: Starting all Docker services... ---" -ForegroundColor Green
docker-compose up -d

Write-Host "Waiting 45 seconds for services (especially Postgres) to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 45

Write-Host "--- Step 2: Creating Kafka topic 'wikipedia-edits'... ---" -ForegroundColor Green

docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic wikipedia-edits --partitions 1 --replication-factor 1

Write-Host "--- Step 3: Initializing Superset (this can take a few minutes)... ---" -ForegroundColor Green
docker-compose exec superset superset db upgrade
docker-compose exec superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin
docker-compose exec superset superset init

Write-Host "--- Step 4: Starting Python data producer in a new window... ---" -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python producer/producer.py"

Write-Host "Waiting 10 seconds for the producer to start sending data..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host "--- Step 5: Submitting the Spark streaming job... ---" -ForegroundColor Green

docker-compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 /opt/spark/apps/stream_processor.py

Write-Host "--- Pipeline startup complete! ---" -ForegroundColor Cyan