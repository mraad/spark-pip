#
# Creates pseudo distributed hadoop 2.7.1
#

FROM sequenceiq/pam:centos-6.5

MAINTAINER "Mansour Raad" mraad@esri.com

USER root

# install dev tools
RUN yum clean all; \
    rpm --rebuilddb; \
    yum install -y curl which tar sudo openssh-server openssh-clients rsync
# update libselinux. see https://github.com/sequenceiq/hadoop-docker/issues/14
RUN yum update -y libselinux

# passwordless ssh
RUN ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key
RUN ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

# java
RUN curl -LO 'http://download.oracle.com/otn-pub/java/jdk/7u79-b15/jdk-7u79-linux-x64.rpm' -H 'Cookie: oraclelicense=accept-securebackup-cookie'
RUN rpm -i jdk-7u79-linux-x64.rpm
RUN rm jdk-7u79-linux-x64.rpm

ENV JAVA_HOME /usr/java/default
ENV PATH $PATH:$JAVA_HOME/bin

# download native support
RUN mkdir -p /tmp/native
RUN curl -Ls http://dl.bintray.com/sequenceiq/sequenceiq-bin/hadoop-native-64-2.7.0.tar | tar -x -C /tmp/native

# hadoop
RUN curl -s http://www.us.apache.org/dist/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz | tar -xz -C /usr/local
RUN cd /usr/local && ln -s hadoop-2.7.1 hadoop

# spark
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.4.1-bin-hadoop2.6.tgz | tar -xz -C /usr/local
RUN cd /usr/local && ln -s spark-1.4.1-bin-hadoop2.6 spark

ENV SPARK_HOME /usr/local/spark

ENV HADOOP_PREFIX /usr/local/hadoop
ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR /usr/local/hadoop/etc/hadoop

ENV PATH $PATH:$SPARK_HOME/bin:$HADOOP_PREFIX/bin

RUN sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/java/default\nexport HADOOP_PREFIX=/usr/local/hadoop\nexport HADOOP_HOME=/usr/local/hadoop\n:' /usr/local/hadoop/etc/hadoop/hadoop-env.sh
RUN sed -i '/^export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop/:' /usr/local/hadoop/etc/hadoop/hadoop-env.sh

# pseudo distributed
ADD core-site-template.xml /usr/local/hadoop/etc/hadoop/core-site-template.xml
ADD hdfs-site-template.xml /usr/local/hadoop/etc/hadoop/hdfs-site-template.xml
ADD yarn-site-template.xml /usr/local/hadoop/etc/hadoop/yarn-site-template.xml

RUN sed s/HOSTNAME/localhost/ /usr/local/hadoop/etc/hadoop/core-site-template.xml > /usr/local/hadoop/etc/hadoop/core-site.xml
RUN sed s/HOSTNAME/localhost/ /usr/local/hadoop/etc/hadoop/hdfs-site-template.xml > /usr/local/hadoop/etc/hadoop/hdfs-site.xml

RUN /usr/local/hadoop/bin/hdfs namenode -format

# fixing the libhadoop.so like a boss
RUN rm -rf /usr/local/hadoop/lib/native
RUN mv /tmp/native /usr/local/hadoop/lib

ADD ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config
RUN chown root:root /root/.ssh/config

ADD bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENV BOOTSTRAP /etc/bootstrap.sh

# working around docker.io build error
RUN ls -la /usr/local/hadoop/etc/hadoop/*-env.sh
RUN chmod +x /usr/local/hadoop/etc/hadoop/*-env.sh
RUN ls -la /usr/local/hadoop/etc/hadoop/*-env.sh

# fix the 254 error code
RUN sed -i "/^[^#]*UsePAM/ s/.*/#&/" /etc/ssh/sshd_config
RUN echo "UsePAM no" >> /etc/ssh/sshd_config
RUN echo "Port 2122" >> /etc/ssh/sshd_config

RUN service sshd start &&\
    /usr/local/hadoop/etc/hadoop/hadoop-env.sh &&\
    /usr/local/hadoop/sbin/start-dfs.sh &&\
    /usr/local/hadoop/bin/hdfs dfs -mkdir -p /user/root &&\
    /usr/local/hadoop/bin/hdfs dfs -chmod a+rwx /user/root &&\
    /usr/local/hadoop/bin/hdfs dfs -put /usr/local/spark/lib /spark

CMD ["/etc/bootstrap.sh", "-d"]

# HDFS ports
EXPOSE 9000 50010 50020 50070 50075 50090
# YARN ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088
# Other ports
EXPOSE 2122 7077 8080 8081
