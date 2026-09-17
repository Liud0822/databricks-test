# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Genie 準備 — 分析用ビューと質問例
# MAGIC **試験領域**: Platform（BI/分析、Databricks SQL）
# MAGIC
# MAGIC Genie は自然言語で表・ビューに質問できる。ここでは Genie に読ませる Gold ビューを整え、
# MAGIC 質問例を用意する。実際の Genie Space 作成は UI（Genie → New）で行う（ガイド参照）。

# COMMAND ----------
dbutils.widgets.text("catalog", "quiz_dev"); dbutils.widgets.text("schema", "quiz")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}"); spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------
# 分析ビュー1: ドメイン別の全体正答率（弱点ドメインの把握）
spark.sql("""
CREATE OR REPLACE VIEW v_domain_summary AS
SELECT domain,
       sum(attempts) AS attempts,
       sum(correct)  AS correct,
       round(100.0*sum(correct)/sum(attempts), 1) AS accuracy_pct
FROM agg_domain_accuracy
GROUP BY domain
""")

# 分析ビュー2: 地域別の正答率（行フィルタ/PIIの文脈で地域軸も使う）
spark.sql("""
CREATE OR REPLACE VIEW v_region_summary AS
SELECT u.region, count(*) AS attempts,
       round(100.0*avg(CAST(f.is_correct AS INT)), 1) AS accuracy_pct
FROM fact_attempt f JOIN dim_user u USING (user_id)
GROUP BY u.region
""")

# 分析ビュー3: 苦手問題ランキング（正答率の低い設問）
spark.sql("""
CREATE OR REPLACE VIEW v_hard_questions AS
SELECT question_id, domain, count(*) attempts,
       round(100.0*avg(CAST(is_correct AS INT)),1) accuracy_pct
FROM fact_attempt
GROUP BY question_id, domain
HAVING count(*) >= 3
ORDER BY accuracy_pct ASC
""")

display(spark.table("v_domain_summary").orderBy("accuracy_pct"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Genie Space に登録するテーブル/ビュー
# MAGIC - `v_attempt_enriched`（明細）, `v_domain_summary`, `v_region_summary`, `v_hard_questions`, `agg_domain_accuracy`
# MAGIC
# MAGIC ## Genie への質問例（日本語でOK）
# MAGIC - 「ドメイン別の正答率を低い順に見せて」
# MAGIC - 「私が一番苦手なドメインは？」
# MAGIC - 「地域ごとの正答率を比較して」
# MAGIC - 「正答率が50%未満の問題を挙げて」
# MAGIC - 「governance ドメインの正答率の推移を週ごとに」
# MAGIC
# MAGIC ※ dim_user 由来の email 列はマスク/行フィルタが効くため、Genie の結果にもガバナンスが反映される。
