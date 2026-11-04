import json

from fastapi.testclient import TestClient

from dashboard import app as dash
from sentrix import store


class FakeTable:
    def __init__(self, data):
        self.data = data

    def scan(self, limit=None):
        return sorted(self.data.items())[:limit]


class FakeConn:
    def __init__(self, tables):
        self._tables = tables

    def tables(self):
        return [name.encode() for name in self._tables]

    def create_table(self, name, families):
        self._tables[name] = {}

    def table(self, name):
        return FakeTable(self._tables[name])

    def close(self):
        pass


def client(monkeypatch, tables):
    monkeypatch.setattr(store, "connect", lambda: FakeConn(tables))
    return TestClient(dash.app)


def test_alerts_newest_first_and_limit_validated(monkeypatch):
    alerts = {f"{10**13 - ts:013d}#t{ts}": {b"a:json": json.dumps({"ts": ts}).encode()} for ts in (1, 3, 2)}
    c = client(monkeypatch, {"alerts": alerts})
    assert [a["ts"] for a in c.get("/api/alerts?limit=2").json()] == [3, 2]
    assert c.get("/api/alerts?limit=0").status_code == 422


def test_rings_sorted_by_score_and_missing_tables_created(monkeypatch):
    tables = {"rings": {f"r{s}": {b"r:json": json.dumps({"score": s}).encode()} for s in (5, 9, 1)}}
    c = client(monkeypatch, tables)
    assert [r["score"] for r in c.get("/api/rings").json()] == [9, 5, 1]
    assert {"accounts", "alerts"} <= set(tables)
    assert c.get("/").status_code == 200
