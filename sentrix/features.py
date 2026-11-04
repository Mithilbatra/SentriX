"""Per-account behavioural features.

One implementation serves both training (replayed over history) and the live scorer
(state kept in HBase), so the model is scored on exactly the features it was trained on.
"""

WINDOW_MS = 10 * 60 * 1000
FEATURES = ["amount", "cnt_10m", "sum_10m", "amt_ratio", "new_payee", "new_device"]


def new_state():
    """Profile of an account with no history (JSON-serialisable, stored in HBase)."""
    return {"recent": [], "payees": [], "devices": [], "amt_sum": 0.0, "amt_cnt": 0}


def step(state, txn):
    """Features of `txn` from the payer's earlier activity, then fold `txn` into `state`."""
    ts, amount = txn["ts"], txn["amount"]
    recent = [r for r in state["recent"] if ts - r[0] <= WINDOW_MS]
    avg = state["amt_sum"] / state["amt_cnt"] if state["amt_cnt"] else amount
    features = {
        "amount": amount,
        "cnt_10m": float(len(recent)),
        "sum_10m": float(sum(a for _, a in recent)),
        "amt_ratio": amount / avg if avg else 1.0,
        "new_payee": float(txn["payee"] not in state["payees"]),
        "new_device": float(txn["device"] not in state["devices"]),
    }
    state["recent"] = recent + [[ts, amount]]
    for key, value in (("payees", txn["payee"]), ("devices", txn["device"])):
        if value not in state[key]:
            state[key].append(value)
    state["amt_sum"] += amount
    state["amt_cnt"] += 1
    return features
