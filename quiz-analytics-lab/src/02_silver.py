# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 · Silver — クレンジング / 型付け / 重複除去
# MAGIC **試験領域**: Transformation（bronze→silver、型変換、null 除去、dedup）

# COMMAND ----------

dbutils.widgets.text("catalog", "quiz_dev"); dbutils.widgets.text("schema", "quiz")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}"); spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# users: user_id で重複除去、文字列を trim
spark.sql("""
CREATE OR REPLACE TABLE silver_users AS
SELECT DISTINCT
  CAST(user_id AS INT) AS user_id,
  trim(display_name)   AS display_name,
  lower(trim(email))   AS email,
  trim(region)         AS region,
  trim(department)     AS department,
  CAST(is_owner AS BOOLEAN) AS is_owner
FROM bronze_users
WHERE user_id IS NOT NULL
""")

# questions
spark.sql("""
CREATE OR REPLACE TABLE silver_questions AS
SELECT DISTINCT CAST(question_id AS INT) AS question_id, lower(trim(domain)) AS domain
FROM bronze_questions
WHERE question_id IS NOT NULL
""")

# attempts: is_correct→boolean、answered_at→timestamp、キー null 除去、attempt_id で dedup
spark.sql("""
CREATE OR REPLACE TABLE silver_attempts AS
SELECT
  CAST(attempt_id AS BIGINT)                 AS attempt_id,
  CAST(user_id AS INT)                       AS user_id,
  session_id, mode,
  CAST(question_id AS INT)                   AS question_id,
  CAST(is_correct AS BOOLEAN)                AS is_correct,
  CAST(answered_at AS TIMESTAMP)             AS answered_at
FROM bronze_attempts
WHERE attempt_id IS NOT NULL AND user_id IS NOT NULL AND question_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY attempt_id ORDER BY answered_at) = 1
""")

# access_control（マスク/行フィルタ用のルックアップ）
spark.sql("""
CREATE OR REPLACE TABLE silver_access_control AS
SELECT DISTINCT lower(trim(principal_email)) AS principal_email,
       trim(allowed_region) AS allowed_region,
       CAST(pii_access AS BOOLEAN) AS pii_access
FROM bronze_access_control WHERE principal_email IS NOT NULL
""")

for t in ["silver_users","silver_questions","silver_attempts","silver_access_control"]:
    print(t, spark.table(t).count())