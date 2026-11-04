"""Find fraud rings and mule accounts in the transaction graph with GraphFrames.

Graph: accounts are vertices; a payer -> payee edge aggregates all their transfers (n, total).
Rings: strongly connected components (3+ accounts) of the repeated high-value subgraph,
       i.e. money that keeps coming back to where it started.
Mules: accounts paid once each by many different payers that pass most of it on.
Results replace the HBase `rings` table; precision/recall against the history labels is printed.

    spark-submit jobs/rings.py
"""
import json

from graphframes import GraphFrame
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

from sentrix import HDFS, store

WINDOW_MS = 24 * 3600 * 1000  # analyse the latest 24 hours
RING_EDGE_TOTAL = 50_000  # Rs moved along one payer -> payee edge
MULE_PAYERS = 20          # distinct one-off payers
MULE_PASS = (0.5, 1.5)    # outflow / inflow


def report(name, found, truth):
    tp = len(found & truth)
    print(f"{name}: {len(found)} accounts flagged, {len(truth)} in ground truth, "
          f"precision {tp / max(len(found), 1):.2f}, recall {tp / max(len(truth), 1):.2f}")


spark = SparkSession.builder.appName("sentrix-rings").getOrCreate()
spark.sparkContext.setCheckpointDir(f"{HDFS}/checkpoint/graph")

cols = ["payer", "payee", "amount", "ts"]
history = spark.read.json(f"{HDFS}/history.jsonl")
txns = history.select(cols)
try:
    txns = txns.unionByName(spark.read.parquet(f"{HDFS}/archive").select(cols))
except AnalysisException:  # the live scorer has not archived anything yet
    pass
# The thresholds are counts and totals, so keep the window fixed as the archive grows.
txns = txns.filter(F.col("ts") > txns.agg(F.max("ts")).first()[0] - WINDOW_MS)

edges = (txns.groupBy(F.col("payer").alias("src"), F.col("payee").alias("dst"))
         .agg(F.count("*").alias("n"), F.round(F.sum("amount"), 2).alias("total"))
         .cache())
vertices = (edges.groupBy(F.col("src").alias("id")).agg(F.sum("total").alias("out_amt"))
            .join(edges.groupBy(F.col("dst").alias("id")).agg(F.sum("total").alias("in_amt")), "id", "full")
            .na.fill(0.0)
            .cache())

# Rings
heavy = edges.filter((F.col("n") >= 2) & (F.col("total") >= RING_EDGE_TOTAL))
scc = GraphFrame(vertices, heavy).stronglyConnectedComponents(maxIter=10).select("id", "component")
members = (scc.groupBy("component").agg(F.sort_array(F.collect_list("id")).alias("members"))
           .filter(F.size("members") >= 3))
inside = (heavy.join(scc.withColumnRenamed("id", "src"), "src")
          .join(scc.select(F.col("id").alias("dst"), F.col("component").alias("dst_component")), "dst")
          .filter("component = dst_component"))
rings = (inside.groupBy("component")
         .agg(F.collect_list(F.struct("src", "dst", "n", "total")).alias("edges"), F.sum("total").alias("score"))
         .join(members, "component"))
clusters = [{"id": f"ring-{r.members[0]}", "type": "ring", "members": r.members,
             "edges": [e.asDict() for e in r.edges], "score": round(r.score, 2)} for r in rings.collect()]

# Mules
one_off_payers = GraphFrame(vertices, edges.filter("n = 1")).inDegrees
mules = (vertices.join(one_off_payers, "id")
         .filter((F.col("inDegree") >= MULE_PAYERS)
                 & F.col("out_amt").between(MULE_PASS[0] * F.col("in_amt"), MULE_PASS[1] * F.col("in_amt"))))
for m in mules.collect():
    victims = edges.filter((F.col("dst") == m.id) & (F.col("n") == 1)).orderBy(F.desc("total")).limit(20).collect()
    cashouts = edges.filter((F.col("src") == m.id) & (F.col("total") >= 0.1 * m.out_amt)).collect()
    clusters.append({"id": f"mule-{m.id}", "type": "mule",
                     "members": [m.id] + [e.dst for e in cashouts] + [e.src for e in victims],
                     "edges": [e.asDict() for e in cashouts + victims], "score": round(m.in_amt, 2)})

conn = store.connect()
if b"rings" in conn.tables():
    conn.delete_table("rings", disable=True)
store.ensure_tables(conn)
with conn.table("rings").batch() as b:
    for c in clusters:
        b.put(c["id"], {"r:json": json.dumps(c)})
conn.close()

labelled = history.filter("fraud_type IN ('ring', 'mule')").select("payer", "payee", "fraud_type").collect()
report("rings", {a for c in clusters if c["type"] == "ring" for a in c["members"]},
       {a for r in labelled if r.fraud_type == "ring" for a in (r.payer, r.payee)})
report("mules", {c["members"][0] for c in clusters if c["type"] == "mule"},
       {r.payee for r in labelled if r.fraud_type == "mule"} & {r.payer for r in labelled if r.fraud_type == "mule"})
print(f"{len(clusters)} clusters written to HBase table 'rings'")
