#!/usr/bin/env bash
${SPARK_HOME}/bin/spark-submit\
 --driver-java-options "-server -Xms1g -Xmx16g"\
 target/spark-pip-0.1.jar\
 local.properties
