#!/usr/bin/env bash
#
#  --jars target/libs/jts-1.13.jar\
# --driver-java-options "-server -Xms1g -Xmx16g"\
#
# ${SPARK_HOME}/bin/spark-submit\
hdfs dfs -rm -r -skipTrash /output
spark-submit\
 --master yarn-client\
 --driver-memory 512m\
 --executor-memory 1g\
 --executor-cores 2\
 target/spark-pip-0.3.jar
