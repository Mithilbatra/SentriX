"""HBase access through the Thrift server."""
import happybase

from . import HBASE

# accounts: p:state = feature profile, f:last = latest features
# alerts:   row = alert_key(ts, txn_id), a:json = scored txn
# rings:    row = cluster id, r:json = members, edges, score
TABLES = {"accounts": ["p", "f"], "alerts": ["a"], "rings": ["r"]}


def alert_key(ts, txn_id):
    """Row key for the alerts table: inverted timestamp first, so a scan returns the newest alerts first."""
    return f"{10**13 - ts:013d}#{txn_id}"


def connect():
    return happybase.Connection(HBASE, timeout=60000)


def ensure_tables(conn):
    for name, families in TABLES.items():
        if name.encode() not in conn.tables():
            try:
                conn.create_table(name, {f: {} for f in families})
            except Exception:
                if name.encode() not in conn.tables():  # anything but losing a creation race
                    raise
