#!/bin/bash

# Builds the docker container. Configurations should be made in the env.txt file.

RELATIVE_SCRIPT_DIR=$( dirname -- ${BASH_SOURCE[0]} )
SCRIPT_DIR=$( cd -- ${RELATIVE_SCRIPT_DIR} &> /dev/null && pwd )

cd ${SCRIPT_DIR}
source ./env.txt

GIT_ROOT_DIR=$( git rev-parse --show-toplevel )

LATEST_JAR=$( ls -v ${GIT_ROOT_DIR}/target/"${SOURCE_JAR_BASENAME}"*.jar | tail -n 1 )
echo ${LATEST_JAR}

cp ${LATEST_JAR} "./${SOURCE_JAR_BASENAME}.jar"

docker build -t ${TARGET_CONTAINER_NAME} --build-arg srcProgram="./${SOURCE_JAR_BASENAME}.jar" --build-arg srcXml="./${SOURCE_XML}" .

#rm "./${SOURCE_JAR_BASENAME}.jar"
