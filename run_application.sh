#!/bin/bash

docker pull postgres:16.3-bullseye
docker image build -t nytingestortest .
docker compose run -e NYT_API_KEY=$1 -e POSTGRES_USER=$2 -e POSTGRES_PASSWORD=$3 ingestor
