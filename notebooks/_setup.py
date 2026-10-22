# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙ DFU · shared setup
# MAGIC Every case notebook starts with `%run ../_setup`. It defines the widgets, the Unity Catalog
# MAGIC names, and makes the `src/` package importable so notebooks and unit tests share one codebase.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "dfu", "Unity Catalog")
dbutils.widgets.text("seed", "2077", "Scenario seed")
dbutils.widgets.dropdown("difficulty", "corpo", ["street_kid", "corpo", "nomad", "legend"], "Difficulty")

CATALOG = dbutils.widgets.get("catalog")
SEED = int(dbutils.widgets.get("seed"))
DIFFICULTY = dbutils.widgets.get("difficulty")

RAW, PROC, FIND, QUAR, OPS = (f"{CATALOG}.{s}" for s in
                              ("evidence_raw", "evidence_processed", "case_findings", "quarantine", "ops"))
LANDING = f"/Volumes/{CATALOG}/evidence_raw/landing"
CHECKPOINTS = f"/Volumes/{CATALOG}/evidence_raw/checkpoints"
LEDGER = f"{PROC}.ledger"

# make ../../src importable (works for Repos/Git folders and bundle deployments)
_here = os.getcwd()
for cand in [os.path.join(_here, "..", "src"), os.path.join(_here, "..", "..", "src")]:
    if os.path.isdir(os.path.join(cand, "dfu")) and os.path.abspath(cand) not in sys.path:
        sys.path.insert(0, os.path.abspath(cand))


def banner(title: str, lines: list[str] | None = None, color: str = "#FCEE0A"):
    """Small neon status panel for notebook output."""
    body = "".join(f"<div style='color:#c8cbe6'>&gt; {l}</div>" for l in (lines or []))
    displayHTML(f"""
      <div style="background:#0A0A1F;border:1px solid {color};border-left:6px solid {color};
                  padding:14px 18px;font-family:'JetBrains Mono',monospace;border-radius:2px">
        <div style="color:{color};font-weight:700;letter-spacing:.12em">{title}</div>{body}
      </div>""")


print(f"catalog={CATALOG}  seed={SEED}  difficulty={DIFFICULTY}  landing={LANDING}")
