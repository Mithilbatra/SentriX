"""Score live transactions from Kafka topic `txns` in real time.

Each micro-batch: executors compute features from each payer's profile in HBase,
the RandomForest scores them, then every scored txn goes to Kafka `scored` (dashboard feed)
and the HDFS archive (fraud-ring job), and flagged ones to the HBase `alerts` table.

    spark-submit jobs/stream.py
"""
import json

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from sentrix import HDFS, KAFKA, store
from sentrix.features import FEATURES, new_state, step

THRESHOLD = 0.5
TXN = "txn_id string, ts long, payer string, payee string, amount double, device string"
EXTRA = [k for k in FEATURES if k != "amount"]  # amount is already a txn field
FEATURED = TXN + "".join(f", {k} double" for k in EXTRA)
# P(fraud) from the probability vector; pyspark.ml.functions.vector_to_array would need pandas
p_fraud = F.udf(lambda v: round(float(v[1]), 4), DoubleType())


def featurize(rows):
    """Runs on an executor. All txns of a payer are in this partition (repartitioned by payer)."""
    rows = sorted(rows, key=lambda r: r.ts)
    if not rows:
        return []
    conn = store.connect()
    accounts = conn.table("accounts")
    payers = list({r.payer for r in rows})
    states = {k.decode(): json.loads(v[b"p:state"]) for k, v in accounts.rows(payers, columns=[b"p:state"])}
    out, last = [], {}
    for r in rows:
        last[r.payer] = f = step(states.setdefault(r.payer, new_state()), r.asDict())
        out.append((*r, *(f[k] for k in EXTRA)))
    with accounts.batch() as b:
        for p in payers:
            b.put(p, {"p:state": json.dumps(states[p]), "f:last": json.dumps(last[p])})
    conn.close()
    return out


def save_alerts(rows):
    rows = list(rows)
    if not rows:
        return
    conn = store.connect()
    with conn.table("alerts").batch() as b:
        for r in rows:
            b.put(f"{10**13 - r.ts:013d}#{r.txn_id}", {"a:json": json.dumps(r.asDict())})  # newest first
    conn.close()


def process(batch, _batch_id):
    if batch.isEmpty():
        return
    # localCheckpoint runs featurize exactly once, so profile updates are not re-applied by later actions.
    # shortcut: a replayed micro-batch (driver crash) updates profiles twice; fine for a demo.
    feats = spark.createDataFrame(batch.repartition("payer").rdd.mapPartitions(featurize), FEATURED).localCheckpoint()
    scored = (model.transform(feats)
              .withColumn("risk", p_fraud("probability"))
              .withColumn("flag", F.col("risk") >= THRESHOLD)
              .drop("features", "rawPrediction", "probability", "prediction")
              .cache())
    scored.filter("flag").foreachPartition(save_alerts)
    (scored.select(F.to_json(F.struct("*")).alias("value")).write.format("kafka")
     .option("kafka.bootstrap.servers", KAFKA).option("topic", "scored").save())
    # shortcut: one small file per micro-batch; compact the archive if the demo runs for days.
    scored.coalesce(1).write.mode("append").parquet(f"{HDFS}/archive")
    scored.unpersist()


spark = SparkSession.builder.appName("sentrix-stream").getOrCreate()
model = PipelineModel.load(f"{HDFS}/model")
conn = store.connect()
store.ensure_tables(conn)
conn.close()

(spark.readStream.format("kafka")
 .option("kafka.bootstrap.servers", KAFKA)
 .option("subscribe", "txns")
 .option("startingOffsets", "latest")
 .option("maxOffsetsPerTrigger", 5000)
 .load()
 .select(F.from_json(F.col("value").cast("string"), TXN).alias("t")).select("t.*")
 .dropna().filter("amount > 0")  # drop malformed messages
 .filter("ts <= unix_millis(current_timestamp()) + 300000")  # a future ts would never leave the 10-minute window
 .writeStream.foreachBatch(process)
 .option("checkpointLocation", f"{HDFS}/checkpoint/stream")
 .trigger(processingTime="2 seconds")
 .start()
 .awaitTermination())
