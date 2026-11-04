"""Synthetic UPI transactions with injected fraud.

    python3 -m sentrix.gen history [--hours 24]   labelled JSON lines on stdout (training data)
    python3 -m sentrix.gen live                    unlabelled, in real time, to Kafka topic `txns`

Accounts, rings and mules come from a fixed seed, so history and live share one world.
Use the same --tps for both: velocity features depend on how often accounts transact.
"""
import argparse
import heapq
import itertools
import json
import random
import sys
import time

from . import KAFKA

N_USERS, N_MERCHANTS = 2000, 200
P_TAKEOVER, P_SPIKE, P_RING, P_MULE = 0.003, 0.005, 0.0002, 0.0001  # chance per normal txn


class World:
    def __init__(self, seed=None):
        rng = random.Random(42)  # the world itself is fixed; `seed` only drives events
        self.users = [f"u{i:04d}@upi" for i in range(N_USERS)]
        self.everyone = self.users + [f"m{i:03d}@upi" for i in range(N_MERCHANTS)]
        self.profile = {
            u: {
                "device": f"d{i:04d}",
                "scale": rng.lognormvariate(6.2, 0.8),  # typical amount, median ~Rs 500
                "friends": rng.sample(self.users[:i] + self.users[i + 1:], rng.randint(3, 6)),
                "shops": rng.sample(self.everyone[N_USERS:], rng.randint(3, 8)),
            }
            for i, u in enumerate(self.users)
        }
        actors = rng.sample(self.users, 23)
        self.rings = [actors[0:3], actors[3:7], actors[7:12], actors[12:18]]
        self.mules, self.cashouts = actors[18:21], actors[21:23]
        self.honest = sorted(set(self.users) - set(actors))
        self.rng = random.Random(seed)

    def txn(self, ts, payer, payee, amount, device=None, kind=""):
        return {
            "txn_id": f"{self.rng.getrandbits(64):016x}",
            "ts": int(ts),
            "payer": payer,
            "payee": payee,
            "amount": round(max(amount, 1.0), 2),
            "device": device or self.profile[payer]["device"],
            "is_fraud": int(bool(kind)),
            "fraud_type": kind,
        }

    def stranger(self, payer):
        while (other := self.rng.choice(self.everyone)) == payer:
            pass
        return other

    def normal(self, payer, ts):
        p, r = self.profile[payer], self.rng.random()
        payee = (self.rng.choice(p["shops"]) if r < 0.7
                 else self.rng.choice(p["friends"]) if r < 0.95
                 else self.stranger(payer))
        device = p["device"] if self.rng.random() < 0.99 else p["device"] + "b"  # second phone
        return [self.txn(ts, payer, payee, p["scale"] * self.rng.lognormvariate(0, 0.5), device)]

    def takeover(self, payer, ts):
        """Stolen credentials: a burst to strangers from a new device."""
        device, scale, out = f"x{self.rng.getrandbits(32):08x}", self.profile[payer]["scale"], []
        for _ in range(self.rng.randint(4, 8)):
            ts += self.rng.uniform(15, 60) * 1000
            out.append(self.txn(ts, payer, self.stranger(payer), scale * self.rng.uniform(3, 10), device, "takeover"))
        return out

    def spike(self, payer, ts):
        """One payment far above the account's usual amount, to a stranger."""
        amount = self.profile[payer]["scale"] * self.rng.uniform(15, 40)
        return [self.txn(ts + 1000, payer, self.stranger(payer), amount, kind="spike")]

    def ring(self, payer, ts):
        """Money cycled around a fixed group of accounts, a small cut at each hop."""
        ring, amount, out = self.rng.choice(self.rings), self.rng.uniform(20000, 50000), []
        for _ in range(self.rng.randint(2, 4)):
            for i, member in enumerate(ring):
                ts += self.rng.uniform(30, 180) * 1000
                amount *= self.rng.uniform(0.97, 1.0)
                out.append(self.txn(ts, member, ring[(i + 1) % len(ring)], amount, kind="ring"))
        return out

    def mule(self, payer, ts):
        """Scam victims pay a mule, which forwards ~90% to a cash-out account."""
        mule, total, out = self.rng.choice(self.mules), 0.0, []
        for victim in self.rng.sample(self.honest, self.rng.randint(10, 25)):
            ts += self.rng.uniform(30, 120) * 1000
            amount = self.rng.uniform(2000, 15000)
            total += amount
            out.append(self.txn(ts, victim, mule, amount, kind="mule"))
        hops = self.rng.randint(2, 3)
        for _ in range(hops):
            ts += self.rng.uniform(60, 300) * 1000
            out.append(self.txn(ts, mule, self.rng.choice(self.cashouts), total * 0.9 / hops, kind="mule"))
        return out

    def stream(self, start_ms, tps):
        """Transactions in time order from start_ms, forever."""
        events = [(P_TAKEOVER, self.takeover), (P_SPIKE, self.spike), (P_RING, self.ring), (P_MULE, self.mule)]
        heap, seq, t = [], itertools.count(), float(start_ms)
        while True:
            t += self.rng.expovariate(tps) * 1000
            payer = self.rng.choice(self.users)
            txns, r = self.normal(payer, t), self.rng.random()
            for p, event in events:
                if r < p:
                    txns += event(payer, t)
                    break
                r -= p
            for x in txns:
                heapq.heappush(heap, (x["ts"], next(seq), x))
            while heap and heap[0][0] <= t:  # later txns are always after t
                yield heapq.heappop(heap)[2]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["history", "live"])
    ap.add_argument("--hours", type=float, default=24, help="history length (default 24)")
    ap.add_argument("--tps", type=float, default=2, help="transactions per second (default 2)")
    args = ap.parse_args()
    now = time.time() * 1000

    if args.mode == "history":
        for x in World(seed=1).stream(now - args.hours * 3600 * 1000, args.tps):
            if x["ts"] >= now:
                break
            sys.stdout.write(json.dumps(x) + "\n")
        return

    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode())
    for n, x in enumerate(World().stream(now, args.tps), 1):
        time.sleep(max(0.0, x["ts"] / 1000 - time.time()))
        del x["is_fraud"], x["fraud_type"]  # labels never leave the generator
        producer.send("txns", key=x["payer"].encode(), value=x)
        if n % 100 == 0:
            print(f"sent {n} transactions", file=sys.stderr)


if __name__ == "__main__":
    main()
