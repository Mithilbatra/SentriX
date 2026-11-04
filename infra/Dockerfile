# One image for every node; entrypoint.sh starts the daemons for $ROLE.
FROM eclipse-temurin:11-jre-jammy

ARG APACHE=https://archive.apache.org/dist
ARG MAVEN=https://repo1.maven.org/maven2

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip procps curl \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir numpy==1.26.4 happybase==1.3.0 kafka-python==2.3.3 \
      fastapi==0.162.0 uvicorn==0.54.0 pytest==9.2.0 httpx==0.28.1 \
 && pip3 install --no-cache-dir --no-deps graphframes-py==0.9.3

RUN curl -fsSL $APACHE/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
    | tar xz -C /opt --exclude='*/share/doc' --exclude='*/share/hadoop/tools' \
 && mv /opt/hadoop-3.3.6 /opt/hadoop
RUN curl -fsSL $APACHE/hbase/2.6.7/hbase-2.6.7-hadoop3-bin.tar.gz \
    | tar xz -C /opt --exclude='*/docs' \
 && mv /opt/hbase-2.6.7-hadoop3 /opt/hbase
RUN curl -fsSL $APACHE/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz | tar xz -C /opt \
 && mv /opt/spark-3.5.9-bin-hadoop3 /opt/spark
RUN curl -fsSL $APACHE/kafka/3.9.2/kafka_2.13-3.9.2.tgz | tar xz -C /opt \
 && mv /opt/kafka_2.13-3.9.2 /opt/kafka

# Kafka source/sink and GraphFrames, baked in so jobs need no --packages download.
RUN cd /opt/spark/jars && for j in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      io/graphframes/graphframes-spark3_2.12/0.9.3/graphframes-spark3_2.12-0.9.3.jar; \
    do curl -fsSLO $MAVEN/$j; done

ENV HADOOP_HOME=/opt/hadoop HBASE_HOME=/opt/hbase SPARK_HOME=/opt/spark KAFKA_HOME=/opt/kafka
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop \
    PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/hbase/bin:/opt/spark/bin:/opt/spark/sbin:/opt/kafka/bin \
    PYTHONPATH=/app PYSPARK_PYTHON=python3 \
    HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true \
    HADOOP_HEAPSIZE_MAX=512m HBASE_HEAPSIZE=768m SPARK_DAEMON_MEMORY=512m \
    SPARK_WORKER_MEMORY=2g SPARK_WORKER_CORES=2 KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"

COPY conf/core-site.xml conf/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
COPY conf/hbase-site.xml conf/hdfs-site.xml $HBASE_HOME/conf/
COPY conf/spark-defaults.conf conf/log4j2.properties $SPARK_HOME/conf/
COPY conf/kafka.properties $KAFKA_HOME/config/sentrix.properties
COPY entrypoint.sh /entrypoint.sh

WORKDIR /app
ENTRYPOINT ["/entrypoint.sh"]
