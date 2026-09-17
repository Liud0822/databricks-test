# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 · Gold — ディメンション / ファクト / 集計（Liquid Clustering）
# MAGIC **試験領域**: Transformation（Gold モデリング、結合、集計）, 最適化（Liquid Clustering）

# COMMAND ----------

dbutils.widgets.text("catalog", "quiz_dev"); dbutils.widgets.text("schema", "quiz")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}"); spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# ディメンション
spark.sql("CREATE OR REPLACE TABLE dim_user     AS SELECT * FROM silver_users")
spark.sql("CREATE OR REPLACE TABLE dim_question AS SELECT * FROM silver_questions")

# ファクト（domain を結合で付与）。Liquid Clustering をアクセスパターンに合わせて設定。
spark.sql("""
CREATE OR REPLACE TABLE fact_attempt
CLUSTER BY (domain, user_id)
AS
SELECT a.attempt_id, a.user_id, a.question_id, q.domain,
       a.is_correct, a.mode, a.session_id, a.answered_at
FROM silver_attempts a
JOIN silver_questions q USING (question_id)
""")

# COMMAND ----------

# 集計: ユーザー×ドメインの正答率（分析システムの中核指標）
spark.sql("""
CREATE OR REPLACE TABLE agg_domain_accuracy AS
SELECT user_id, domain,
       count(*)                                        AS attempts,
       sum(CAST(is_correct AS INT))                    AS correct,
       round(100.0*sum(CAST(is_correct AS INT))/count(*), 1) AS accuracy_pct
FROM fact_attempt
GROUP BY user_id, domain
""")

# Genie/BI 用の結合ビュー（dim_user のマスク/行フィルタが自動的に効く）
spark.sql("""
CREATE OR REPLACE VIEW v_attempt_enriched AS
SELECT f.attempt_id, f.user_id, u.display_name, u.email, u.region, u.department,
       f.question_id, f.domain, f.is_correct, f.mode, f.answered_at
FROM fact_attempt f
JOIN dim_user u USING (user_id)
""")

display(spark.sql("SELECT domain, sum(attempts) attempts, round(100.0*sum(correct)/sum(attempts),1) acc FROM agg_domain_accuracy GROUP BY domain ORDER BY acc"))