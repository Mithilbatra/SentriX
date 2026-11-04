"""Dashboard: live feed (SSE from Kafka `scored`), fraud alerts and rings (HBase).

    uvicorn dashboard.app:app --port 8000
"""
import json
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse
from kafka import KafkaConsumer

from sentrix import KAFKA, store

app = FastAPI(title="SentriX")
COLUMN = {"alerts": b"a:json", "rings": b"r:json"}


def rows(table, limit=None):
    conn = store.connect()
    try:
        store.ensure_tables(conn)
        return [json.loads(data[COLUMN[table]]) for _, data in conn.table(table).scan(limit=limit)]
    finally:
        conn.close()


@app.get("/")
def index():
    return FileResponse(Path(__file__).with_name("index.html"))


@app.get("/api/alerts")
def alerts(limit: int = Query(50, ge=1, le=500)):
    return rows("alerts", limit)  # row keys are inverted timestamps: newest first


@app.get("/api/rings")
def rings():
    return sorted(rows("rings"), key=lambda c: -c["score"])


@app.get("/events")
def events():
    def stream():
        consumer = KafkaConsumer("scored", bootstrap_servers=KAFKA, auto_offset_reset="latest")
        try:
            while True:
                batch = consumer.poll(timeout_ms=1000)
                messages = [m.value.decode() for part in batch.values() for m in part]
                yield "".join(f"data: {m}\n\n" for m in messages) or ": ping\n\n"  # ping detects closed tabs
        finally:
            consumer.close()

    return StreamingResponse(stream(), media_type="text/event-stream")
