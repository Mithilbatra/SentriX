EXEC   = docker compose exec master
SUBMIT = $(EXEC) spark-submit

.PHONY: up status history train stream live rings test down clean

up:       ## build and start the cluster and dashboard
	docker compose up -d --build

status:   ## live HDFS DataNodes, Spark workers and HBase RegionServers
	$(EXEC) sh -c "hdfs dfsadmin -report | grep 'Live datanodes'; \
	  curl -s localhost:8080/json/ | grep -o '\"aliveworkers\" : [0-9]*'; \
	  echo status | hbase shell -n 2>/dev/null | grep servers"

history:  ## write 24h of labelled transactions to HDFS
	$(EXEC) sh -c "hdfs dfs -mkdir -p /sentrix && python3 -m sentrix.gen history | hdfs dfs -put -f - /sentrix/history.jsonl"

train:    ## train the model, seed account profiles in HBase
	$(SUBMIT) jobs/train.py

stream:   ## real-time scorer (keep it running)
	$(SUBMIT) jobs/stream.py

live:     ## send live transactions to Kafka (keep it running)
	$(EXEC) python3 -m sentrix.gen live

rings:    ## fraud-ring and mule detection
	$(SUBMIT) jobs/rings.py

test:
	docker run --rm -v "$(CURDIR):/app" -e PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
	  sentrix python3 -m pytest -q

down:
	docker compose down

clean:    ## also delete HDFS, HBase and Kafka data
	docker compose down -v
