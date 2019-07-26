#!/bin/sh

cd "$(dirname "$0")" || exit 100

../gradlew jar
EXIT_CODE=$?
if [ "$EXIT_CODE" -ne "0" ]; then
  exit $?
fi

name=kafka-massive-tests
version=0.0.1

docker build -t $name:$version .

#docker save $name:$version | ssh dock1 docker load
#docker save $name:$version | ssh dock2 docker load
#docker save $name:$version | ssh dock3 docker load
