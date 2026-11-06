import pytest
from pyspark.sql import SparkSession  # needs Spark + GraphFrames, as in `make test` and CI

from jobs.rings import find_clusters, save

SCHEMA = "payer string, payee string, amount double, ts long"
DAY_MS = 24 * 3600 * 1000


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").getOrCreate()
    yield session
    session.stop()


def payments():
    """One ring, one mule and one busy shop, a second apart."""
    out = []

    def pay(payer, payee, amount):
        out.append((payer, payee, float(amount), 1000 * len(out)))

    for _ in range(2):  # the ring passes money round twice
        pay("r1", "r2", 30_000), pay("r2", "r3", 29_000), pay("r3", "r1", 28_000)
    for i in range(25):  # 25 one-off payers pay the mule, which passes ~90% on
        pay(f"v{i}", "mule", 5_000)
    pay("mule", "cash", 60_000), pay("mule", "cash", 52_000)
    for i in range(30):  # a busy shop: many one-off payers, pays nobody
        pay(f"c{i}", "shop", 500)
    return out


def found(spark, rows):
    return {(c["type"], c["members"][0]) for c in find_clusters(spark.createDataFrame(rows, SCHEMA))}


def test_finds_the_ring_and_the_mule_but_not_the_shop(spark):
    clusters = find_clusters(spark.createDataFrame(payments(), SCHEMA))
    assert {(c["type"], c["members"][0]) for c in clusters} == {("ring", "r1"), ("mule", "mule")}
    mule = next(c for c in clusters if c["type"] == "mule")
    assert mule["members"][:2] == ["mule", "cash"] and len(mule["members"]) == 22  # + the top 20 payers


def test_a_pause_in_activity_does_not_hide_the_ring(spark):
    later = [("x1", "x2", 100.0, 2 * DAY_MS + i) for i in range(3)]  # a trickle two days later
    assert ("ring", "r1") in found(spark, payments() + later)


class FakeTable(dict):
    """The parts of a happybase Table that save() uses; row keys are bytes, as HBase returns them."""

    def batch(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def put(self, key, data):
        self[key.encode()] = data

    def delete(self, key):
        del self[key]

    def scan(self):
        return list(self.items())


def test_save_replaces_the_result_but_keeps_it_when_nothing_is_found():
    table = FakeTable({b"ring-old": {}, b"ring-a": {}})
    save(table, [{"id": "ring-a"}, {"id": "mule-b"}])
    assert set(table) == {b"ring-a", b"mule-b"}
    save(table, [])
    assert set(table) == {b"ring-a", b"mule-b"}
