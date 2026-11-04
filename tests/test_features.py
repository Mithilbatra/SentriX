import json

from sentrix.features import WINDOW_MS, new_state, step


def txn(ts, amount=100.0, payee="m001@upi", device="d0001"):
    return {"ts": ts, "amount": amount, "payee": payee, "device": device}


def test_first_txn_has_no_history():
    f = step(new_state(), txn(0))
    assert f == {"amount": 100.0, "cnt_10m": 0.0, "sum_10m": 0.0, "amt_ratio": 1.0,
                 "new_payee": 1.0, "new_device": 1.0}


def test_window_ratio_and_known_counterparties():
    s = new_state()
    step(s, txn(0, 100.0))
    step(s, txn(60_000, 300.0))
    f = step(s, txn(120_000, 800.0))
    assert (f["cnt_10m"], f["sum_10m"], f["amt_ratio"]) == (2.0, 400.0, 4.0)
    assert (f["new_payee"], f["new_device"]) == (0.0, 0.0)

    late = step(s, txn(120_000 + WINDOW_MS + 1, payee="u0002@upi", device="x1"))
    assert (late["cnt_10m"], late["new_payee"], late["new_device"]) == (0.0, 1.0, 1.0)
    assert len(s["recent"]) == 1  # old entries pruned, state stays small


def test_state_survives_json_round_trip():
    """The live scorer keeps state as JSON in HBase; features must not change because of it."""
    a, b = new_state(), new_state()
    for i, t in enumerate([txn(0), txn(5_000, 250.0, "u0009@upi"), txn(9_000, 40.0)]):
        b = json.loads(json.dumps(b))
        assert step(a, t) == step(b, t), i
