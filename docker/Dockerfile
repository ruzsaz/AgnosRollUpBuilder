FROM openjdk:17-slim
ARG srcProgram
ARG srcXml
WORKDIR /
COPY ${srcProgram} AgnosRollUpBuilder.jar
COPY ${srcXml} cube.xml
# ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD java -jar -Dspring.profiles.active=prod ./AgnosRollUpBuilder.jar --xml=./cube.xml --i=final --o=rolled
