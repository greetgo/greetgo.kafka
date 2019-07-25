#!/bin/sh

cd $(dirname $0)

docker-compose down

sudo rm -rf volumes/

docker-compose up -d
