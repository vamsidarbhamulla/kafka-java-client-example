#!/usr/bin/env bash
## script usage: source start-service.sh
echo "******************************************************************************"
echo "*                          Start kafka service                               *"
echo "******************************************************************************"

echo "* Spin up the kafka service *"

export DOCKERHOST=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1)

docker-compose up --build --force-recreate
docker-compose down --remove-orphans

echo "******************************************************************************"
echo "*                         Ending kafka service                               *"
echo "******************************************************************************"