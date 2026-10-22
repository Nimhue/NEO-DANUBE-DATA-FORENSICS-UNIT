import argparse

from .breach import DIFFICULTY, generate

p = argparse.ArgumentParser(description="Generate a Neo-Danube breach scenario.")
p.add_argument("--seed", type=int, default=2077)
p.add_argument("--difficulty", choices=list(DIFFICULTY), default="corpo")
p.add_argument("--out", default="./landing")
a = p.parse_args()
m = generate(a.out, a.seed, a.difficulty)["manifest"]
print(f"> BREACH SCENARIO GENERATED  seed={m['seed']} difficulty={m['difficulty']}")
print(f"  {m['identity_records']} identity records · {m['ledger_v0_rows']} ledger rows · "
      f"{m['commits']} commits · {m['shard_records']} shard records in {m['shard_files']} fragments")
print(f"  written to {a.out}")
