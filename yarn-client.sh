#!/usr/bin/env bash
#
hdfs dfs -rm -r -skipTrash /output
time spark-submit\
 --master yarn-client\
 --driver-memory 512m\
 --num-executors 2\
 --executor-cores 1\
 --executor-memory 2g\
 target/spark-pip-0.3.jar
