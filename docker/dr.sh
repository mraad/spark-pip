#!/usr/bin/env bash
docker run\
  -it\
  --rm=true\
  -h boot2docker\
  -p 8088:8088\
  -p 9000:9000\
  -p 50010:50010\
  -p 50070:50070\
  -p 50075:50075\
  mraad/hdfs\
  /etc/bootstrap.sh
