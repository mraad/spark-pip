#!/usr/bin/env bash
#
${SPARK_HOME}/bin/spark-submit\
 --master yarn-client\
 --driver-memory 1g\
 --executor-memory 2g\
 --executor-cores 1\
 --driver-java-options "-server -Xms1g -Xmx16g"\
 --jars target/libs/jts-1.13.jar\
 target/spark-pip-0.1.jar
