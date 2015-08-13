# Apache YARN/HDFS 2.7.1, Spark 2.4.1 Docker Image

Docker image to bootstrap HDFS and YARN as a pseudo cluster to enable the execution of a Spark Job in a yarn client
mode where the driver runs in the client process, and the application master is only used for requesting resources from YARN.

## Reference Images

This project derives *heavily* from the following project:
 
* [docker-spark](https://github.com/sequenceiq/docker-spark) 
* [hadoop-docker](https://github.com/sequenceiq/hadoop-docker) 

The small deference is that [WebHDFS](https://hadoop.apache.org/docs/r1.0.4/webhdfs.html) is enabled, and
all the IP address in `core-site.xml`, `hdfs-site.xml` and `yarn-site.xml` are using the container hostname
that is defined at run time using the `-h` command line switch.

## Build the Image

```
docker build -t mraad/hdfs .
```

## Start a Container

**Make sure that SELinux is disabled on the host.
If you are using [boot2docker](http://boot2docker.io/) you don't need to do anything, but make sure that you have a minimum of 4GB of RAM.**

Update the host `/etc/hosts` file with an entry named `boot2docker` whose value is the result of the `boot2docker ip` command.

For example:

```
127.0.0.1       localhost
192.168.59.103  boot2docker
```

```
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
  /etc/bootstrap.sh -bash
```

## Viewing Web UIs

In the host web browser, open

* [http://boot2docker:50070](http://boot2docker:50070) to view the HDFS web UI
* [http://boot2docker:8088](http://boot2docker:8088) to view the YARN web UI

## Run the Spark Shell

```
spark-shell\
 --master yarn-client\
 --driver-memory 1g\
 --executor-memory 1g\
 --executor-cores 1
```

Execute the following command which should return 1000:

```
scala> sc.parallelize(1 to 1000).count()
```
