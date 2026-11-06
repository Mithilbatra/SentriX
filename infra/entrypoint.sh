#!/usr/bin/env bash
# Starts the daemons for $ROLE (master | worker | dashboard), then follows their logs.
# Without ROLE it runs the given command (used by `make test`).
set -euo pipefail

wait_port() { until (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; do sleep 2; done; }
rm -f /tmp/*.pid  # stale after a container restart; the start scripts would refuse to run

case "${ROLE:-}" in
  master)
    [ -d /data/nn/current ] || hdfs namenode -format -nonInteractive
    hdfs --daemon start namenode
    start-master.sh
    [ -f /data/kafka/meta.properties ] ||
      kafka-storage.sh format -t "$(kafka-storage.sh random-uuid)" -c "$KAFKA_HOME/config/sentrix.properties"
    kafka-server-start.sh -daemon "$KAFKA_HOME/config/sentrix.properties"
    hbase-daemon.sh start zookeeper
    wait_port master 9000
    hdfs dfsadmin -safemode wait
    # HBase writes to HDFS on start, so it needs at least one live DataNode.
    until hdfs dfsadmin -report 2>/dev/null | grep -q '^Live datanodes ([1-9]'; do sleep 2; done
    hbase-daemon.sh start master
    hbase-daemon.sh start thrift
    wait_port master 9092
    for topic in txns scored; do
      kafka-topics.sh --bootstrap-server master:9092 --create --if-not-exists --topic "$topic" --partitions 4
    done
    ;;
  worker)
    wait_port master 9000 && hdfs --daemon start datanode
    wait_port master 7077 && start-worker.sh spark://master:7077
    wait_port master 2181 && hbase-daemon.sh start regionserver
    ;;
  dashboard)
    exec uvicorn dashboard.app:app --host 0.0.0.0 --port 8000
    ;;
  *)
    exec "$@"
    ;;
esac
logs=(/opt/hadoop/logs/*.log /opt/hbase/logs/*.log /opt/spark/logs/*.out)
pids=$(cat /tmp/*.pid)  # read now: HBase deletes its pid file when the process exits
if [ "$ROLE" = master ]; then
  logs+=(/opt/kafka/logs/server.log)
  pids+=" $(pgrep -f kafka.Kafka)"  # started with -daemon, which writes no pid file
fi
tail -F "${logs[@]}" &
# This script is the container's main process, so exit when any daemon dies:
# the restart policy then restarts the whole node.
while sleep 10; do
  for pid in $pids; do
    kill -0 "$pid" 2>/dev/null || { echo "daemon $pid died, restarting this node"; exit 1; }
  done
done
