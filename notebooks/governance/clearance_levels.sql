-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ◆ GOVERNANCE · Clearance levels & chain of custody
-- MAGIC
-- MAGIC Evidence is only useful if you can prove nobody tampered with it. In this unit that proof comes
-- MAGIC from Unity Catalog itself:
-- MAGIC
-- MAGIC | Lore | Unity Catalog feature |
-- MAGIC |---|---|
-- MAGIC | Chain of custody | lineage (automatic) + tags on every evidence table |
-- MAGIC | Clearance levels | column masks on PII, row filters on suspects |
-- MAGIC | Internal affairs | `system.access.audit`: who looked at what |
-- MAGIC
-- MAGIC Uses two account groups: **`dfu_detectives`** (full clearance) and everyone else (rookies).
-- MAGIC Create `dfu_detectives` in the account console and add yourself, or rookies is all you'll see.
-- MAGIC Set the `catalog` widget to the catalog you used for the cases.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "dfu", "Unity Catalog")

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

-- MAGIC %md ## 1 · Tag the evidence (chain of custody)

-- COMMAND ----------

ALTER TABLE evidence_processed.shard_recovered SET TAGS ('case_id' = 'HAL-2077-01', 'evidence_class' = 'recovered', 'custody' = 'dfu');
ALTER TABLE evidence_processed.ledger          SET TAGS ('case_id' = 'HAL-2077-02', 'evidence_class' = 'crime_scene', 'custody' = 'dfu');
ALTER TABLE evidence_processed.identity_records SET TAGS ('case_id' = 'HAL-2077-03', 'evidence_class' = 'pii', 'custody' = 'dfu');
ALTER TABLE case_findings.suspects             SET TAGS ('case_id' = 'HAL-2077-04', 'evidence_class' = 'finding', 'custody' = 'dfu');

ALTER TABLE evidence_processed.identity_records ALTER COLUMN citizen_id SET TAGS ('pii' = 'government_id');
ALTER TABLE evidence_processed.identity_records ALTER COLUMN dob        SET TAGS ('pii' = 'date_of_birth');

COMMENT ON TABLE case_findings.suspects IS 'Named suspects in the Halcyon ledger breach. Need-to-know.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 · Column masks: rookies see `[REDACTED]`
-- MAGIC A mask is a SQL function attached to a column. UC calls it on every read, for every user,
-- MAGIC from every tool: notebooks, SQL warehouses, Power BI, all of them.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ops.mask_pii(value STRING)
  RETURNS STRING
  COMMENT 'Full value for dfu_detectives, redacted for everyone else'
  RETURN CASE WHEN is_account_group_member('dfu_detectives') THEN value ELSE '[REDACTED]' END;

CREATE OR REPLACE FUNCTION ops.mask_dob(value STRING)
  RETURNS STRING
  RETURN CASE WHEN is_account_group_member('dfu_detectives') THEN value ELSE concat(left(value, 4), '-••-••') END;

ALTER TABLE evidence_processed.identity_records ALTER COLUMN citizen_id SET MASK ops.mask_pii;
ALTER TABLE evidence_processed.identity_records ALTER COLUMN dob        SET MASK ops.mask_dob;

-- COMMAND ----------

SELECT record_id, source_system, full_name, dob, citizen_id FROM evidence_processed.identity_records LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 · Row filter: the prime suspect is need-to-know
-- MAGIC Rookies can see accomplices but not who the prime suspect is.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ops.need_to_know(role STRING)
  RETURNS BOOLEAN
  RETURN is_account_group_member('dfu_detectives') OR role <> 'PRIME SUSPECT';

ALTER TABLE case_findings.suspects SET ROW FILTER ops.need_to_know ON (role);

-- COMMAND ----------

SELECT role, canonical_name, tamper_edits, eddies_received FROM case_findings.suspects;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 · Grants (uncomment once the groups exist)

-- COMMAND ----------

-- GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO `account users`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA case_findings TO `account users`;
-- GRANT ALL PRIVILEGES ON SCHEMA evidence_processed TO `dfu_detectives`;
-- REVOKE SELECT ON SCHEMA quarantine FROM `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 · Internal affairs: who touched the evidence?
-- MAGIC Needs access to system tables (`system.access.audit`).

-- COMMAND ----------

SELECT event_time, user_identity.email AS who, action_name,
       request_params.full_name_arg AS object
  FROM system.access.audit
 WHERE service_name = 'unityCatalog'
   AND request_params.full_name_arg LIKE concat(:catalog, '.%')
   AND event_date >= current_date() - INTERVAL 7 DAYS
 ORDER BY event_time DESC
 LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6 · Lineage = chain of custody
-- MAGIC Open **Catalog Explorer → case_findings.suspects → Lineage**. You'll see the full path back to
-- MAGIC the raw shard files in the landing volume, captured automatically by Unity Catalog.
