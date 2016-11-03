#!/usr/bin/env bash
rm -rf /tmp/output
${SPARK_HOME}/bin/spark-submit\
 target/spark-pip-0.3.jar\
 local.properties
