# One image for every node; entrypoint.sh starts the daemons for $ROLE.
FROM eclipse-temurin:11-jre-jammy

ARG APACHE=https://archive.apache.org/dist
ARG MAVEN=https://repo1.maven.org/maven2
# SHA-512 sums that Apache publishes next to each release; the build fails on a mismatch.
ARG HADOOP_SHA512=de3eaca2e0517e4b569a88b63c89fae19cb8ac6c01ff990f1ff8f0cc0f3128c8e8a23db01577ca562a0e0bb1b4a3889f8c74384e609cd55e537aada3dcaa9f8a
ARG HBASE_SHA512=2e0835a3ed8e5a9114f5d878deafb2f7a6b1c8386ba422d1a307fcc5551d452dc7da468729a43acf75dcfe0b20424cb48f72ed424dfd15f81b1b3624454ed82d
ARG SPARK_SHA512=66097faf48b12f8673dc13256a44d731c27c86a5831cd8ca930c6fd9dd9bdf31d40593095e959461351d55da1a8e3ad5ed95e0b4b51bea87ca3989ab002ea8ce
ARG KAFKA_SHA512=01186a5086b5ae753811ee71808a797baec1303d4e0d4b1c0acf42c489654a7b8988136f8469d0b66f0be439f8e3e9b816171ea0b0d7a36462b07aa2d3ef0023

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip procps curl \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir numpy==1.26.4 happybase==1.3.0 kafka-python==2.3.3 \
      fastapi==0.162.0 uvicorn==0.54.0 pytest==9.2.0 httpx2==2.13.1 \
 && pip3 install --no-cache-dir --no-deps graphframes-py==0.9.3

RUN curl -fsSLo /tmp/hadoop.tgz $APACHE/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
 && echo "$HADOOP_SHA512  /tmp/hadoop.tgz" | sha512sum -c - \
 && tar xzf /tmp/hadoop.tgz -C /opt --exclude='*/share/doc' --exclude='*/share/hadoop/tools' \
 && rm /tmp/hadoop.tgz && mv /opt/hadoop-3.3.6 /opt/hadoop
RUN curl -fsSLo /tmp/hbase.tgz $APACHE/hbase/2.6.7/hbase-2.6.7-hadoop3-bin.tar.gz \
 && echo "$HBASE_SHA512  /tmp/hbase.tgz" | sha512sum -c - \
 && tar xzf /tmp/hbase.tgz -C /opt --exclude='*/docs' \
 && rm /tmp/hbase.tgz && mv /opt/hbase-2.6.7-hadoop3 /opt/hbase
RUN curl -fsSLo /tmp/spark.tgz $APACHE/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz \
 && echo "$SPARK_SHA512  /tmp/spark.tgz" | sha512sum -c - \
 && tar xzf /tmp/spark.tgz -C /opt \
 && rm /tmp/spark.tgz && mv /opt/spark-3.5.9-bin-hadoop3 /opt/spark
RUN curl -fsSLo /tmp/kafka.tgz $APACHE/kafka/3.9.2/kafka_2.13-3.9.2.tgz \
 && echo "$KAFKA_SHA512  /tmp/kafka.tgz" | sha512sum -c - \
 && tar xzf /tmp/kafka.tgz -C /opt \
 && rm /tmp/kafka.tgz && mv /opt/kafka_2.13-3.9.2 /opt/kafka

# Kafka source/sink and GraphFrames, baked in so jobs need no --packages download.
# Each jar's SHA-256 (cross-checked against Maven Central's published SHA-1); any failure stops the build.
RUN cd /opt/spark/jars && for j in \
      fed393dcc3ef70611fe3d4f35ae78a8ea7c55301fd4cdd66c5dc97fcfaa5a00f:org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar \
      e82612e6070438c5ea48b08c74f367a6d214a390bdf6dcf3ff316dca592ddda5:org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar \
      9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6:org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83:org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      3ec893c194b0abf716e8c350cafd7ba152026ac613d1365a8da0c8f9bfdce4b8:io/graphframes/graphframes-spark3_2.12/0.9.3/graphframes-spark3_2.12-0.9.3.jar; \
    do curl -fsSLO "$MAVEN/${j#*:}" && echo "${j%%:*}  ${j##*/}" | sha256sum -c - || exit 1; done

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
