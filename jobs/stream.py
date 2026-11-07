"""Score live transactions from Kafka topic `txns` in real time.

Each micro-batch: executors compute features from each payer's profile in HBase,
the RandomForest scores them, then every scored txn goes to Kafka `scored` (dashboard feed)
and flagged ones to the HBase `alerts` table. A second query copies `scored` into the HDFS
archive (input for the fraud-ring job) once a minute.

    spark-submit jobs/stream.py
"""
import json
from contextlib import closing

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from sentrix import HDFS, KAFKA, store
from sentrix.features import FEATURES, new_state, step

THRESHOLD = 0.5
TXN = "txn_id string, ts long, payer string, payee string, amount double, device string"
EXTRA = [k for k in FEATURES if k != "amount"]  # amount is already a txn field
FEATURED = TXN + "".join(f", {k} double" for k in EXTRA)
SCORED = FEATURED + ", risk double, flag boolean"
# P(fraud) from the probability vector; pyspark.ml.functions.vector_to_array would need pandas
p_fraud = F.udf(lambda v: round(float(v[1]), 4), DoubleType())


def featurize(rows):
    """Runs on an executor. All txns of a payer are in this partition (repartitioned by payer)."""
    rows = sorted(rows, key=lambda r: r.ts)
    if not rows:
        return []
    payers = list({r.payer for r in rows})
    with closing(store.connect()) as conn:
        accounts = conn.table("accounts")
        states = {k.decode(): json.loads(v[b"p:state"]) for k, v in accounts.rows(payers, columns=[b"p:state"])}
        out, last = [], {}
        for r in rows:
            last[r.payer] = f = step(states.setdefault(r.payer, new_state()), r.asDict())
            out.append((*r, *(f[k] for k in EXTRA)))
        with accounts.batch() as b:
            for p in payers:
                b.put(p, {"p:state": json.dumps(states[p]), "f:last": json.dumps(last[p])})
    return out


def save_alerts(rows):
    rows = list(rows)
    if not rows:
        return
    with closing(store.connect()) as conn, conn.table("alerts").batch() as b:
        for r in rows:
            b.put(store.alert_key(r.ts, r.txn_id), {"a:json": json.dumps(r.asDict())})


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
 .start())

# Archive for the ring job: one Parquet file a minute instead of one per 2-second micro-batch.
# The file sink commits each file through its own checkpoint, so readers never see partial or
# repeated files.
(spark.readStream.format("kafka")
 .option("kafka.bootstrap.servers", KAFKA)
 .option("subscribe", "scored")
 .option("startingOffsets", "earliest")
 .load()
 .select(F.from_json(F.col("value").cast("string"), SCORED).alias("t")).select("t.*")
 .coalesce(1)
 .writeStream.format("parquet")
 .option("path", f"{HDFS}/archive")
 .option("checkpointLocation", f"{HDFS}/checkpoint/archive")
 .trigger(processingTime="1 minute")
 .start())
spark.streams.awaitAnyTermination()
