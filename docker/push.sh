#!/bin/bash

# Pushes the created docker image to the docker hub.

RELATIVE_SCRIPT_DIR=$( dirname -- ${BASH_SOURCE[0]} )
SCRIPT_DIR=$( cd -- ${RELATIVE_SCRIPT_DIR} &> /dev/null && pwd )

cd ${SCRIPT_DIR}
source ./env.txt

docker push ${TARGET_CONTAINER_NAME}