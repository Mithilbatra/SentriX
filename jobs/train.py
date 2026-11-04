"""Train the fraud model on labelled history in HDFS; seed account profiles in HBase.

    spark-submit jobs/train.py
"""
import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

from sentrix import HDFS, store
from sentrix.features import FEATURES, new_state, step


def replay(txns):
    """One payer's history in time order -> (feature rows, final profile)."""
    state, rows = new_state(), []
    for t in sorted(txns, key=lambda t: t["ts"]):
        f = step(state, t)
        rows.append((t["ts"], float(t["is_fraud"]), *(f[k] for k in FEATURES)))
    return rows, state


def save_profiles(profiles):
    conn = store.connect()
    with conn.table("accounts").batch(batch_size=500) as b:
        for payer, state in profiles:
            b.put(payer, {"p:state": json.dumps(state)})
    conn.close()


spark = SparkSession.builder.appName("sentrix-train").getOrCreate()
history = spark.read.json(f"{HDFS}/history.jsonl")
by_payer = history.rdd.map(lambda r: (r.payer, r.asDict())).groupByKey().mapValues(replay).cache()
schema = "ts long, label double, " + ", ".join(f"{k} double" for k in FEATURES)
data = spark.createDataFrame(by_payer.flatMap(lambda kv: kv[1][0]), schema)

# Time split: train on the first 80% of the period, test on the rest.
cutoff = data.approxQuantile("ts", [0.8], 0.001)[0]
train, test = data.filter(data.ts < cutoff).cache(), data.filter(data.ts >= cutoff).cache()
model = Pipeline(stages=[
    VectorAssembler(inputCols=FEATURES, outputCol="features"),
    RandomForestClassifier(numTrees=50, maxDepth=8, seed=42),
]).fit(train)

pred = model.transform(test).cache()
roc = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(pred)
pr = BinaryClassificationEvaluator(metricName="areaUnderPR").evaluate(pred)
tp = pred.filter("label = 1 AND prediction = 1").count()
fp = pred.filter("label = 0 AND prediction = 1").count()
fn = pred.filter("label = 1 AND prediction = 0").count()
print(f"test txns {pred.count()}  fraud {tp + fn}  AUC-ROC {roc:.3f}  AUC-PR {pr:.3f}  "
      f"precision {tp / max(tp + fp, 1):.3f}  recall {tp / max(tp + fn, 1):.3f}")

model.write().overwrite().save(f"{HDFS}/model")
conn = store.connect()
store.ensure_tables(conn)
conn.close()
by_payer.map(lambda kv: (kv[0], kv[1][1])).foreachPartition(save_profiles)
print(f"model saved to {HDFS}/model, {by_payer.count()} account profiles written to HBase")
