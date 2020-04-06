# Kafka-java-client-example

This repo is kafka java client example code and set of basic unit and integration tests.

## Run unit tests

### Using maven
```shell script
mvn -T 1C clean test 
```

## To spin up the kafka cluster in docker containers
```shell script
cd tools
./start-script.sh
```

### Run integration tests
```shell script
mvn -T 1C clean verify -Dskip.unit.tests=true 
```