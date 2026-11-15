import itertools
import json
import sys

import pytest

from sentrix.gen import World, main


def sample(n, seed=1):
    return list(itertools.islice(World(seed).stream(0, 2.0), n))


@pytest.mark.parametrize("mode", ["history", "live"])
@pytest.mark.parametrize("option", ["hours", "tps"])
@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "-inf"])
def test_invalid_generator_arguments_fail_before_generating(mode, option, value, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["gen", mode, f"--{option}={value}"])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    output = capsys.readouterr()
    assert not output.out
    assert f"--{option} must be a positive finite number" in output.err


def test_history_accepts_positive_fractional_arguments(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["gen", "history", "--hours=0.01", "--tps=0.5"])
    main()
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert rows and all(row["amount"] > 0 for row in rows)


def test_history_is_reproducible_ordered_and_sane():
    txns = sample(60_000)
    assert txns[:500] == sample(500)
    assert all(a["ts"] <= b["ts"] for a, b in zip(txns, txns[1:]))
    assert all(t["payer"] != t["payee"] and t["amount"] >= 1 for t in txns)
    assert all(t["is_fraud"] == bool(t["fraud_type"]) for t in txns)
    kinds = {t["fraud_type"] for t in txns}
    assert kinds == {"", "takeover", "spike", "ring", "mule"}
    assert 0.01 < sum(t["is_fraud"] for t in txns) / len(txns) < 0.1


def test_fraud_patterns_use_the_fixed_actors():
    w, txns = World(), sample(60_000)
    for t in txns:
        if t["fraud_type"] == "ring":
            assert any(t["payer"] in r and t["payee"] in r for r in w.rings)
        if t["fraud_type"] == "mule":
            assert t["payee"] in w.mules or (t["payer"] in w.mules and t["payee"] in w.cashouts)
