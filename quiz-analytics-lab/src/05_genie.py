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

# 統合ビュー: seed の履歴(fact_attempt) ∪ アプリの解答(app_attempts)
#  → ここが「ループを閉じる」核心。以降の集計はこの v_all_attempts を見る。
spark.sql("""
CREATE OR REPLACE VIEW v_all_attempts AS
SELECT 'seed' AS source, CAST(user_id AS STRING) AS actor,
       question_id, domain, is_correct, mode, answered_at
FROM fact_attempt
UNION ALL
SELECT 'app' AS source, principal AS actor,
       question_id, domain, CAST(is_correct AS BOOLEAN) AS is_correct, mode, answered_at
FROM app_attempts
""")

# 分析ビュー1: ドメイン別の全体正答率（seed＋アプリ）
spark.sql("""
CREATE OR REPLACE VIEW v_domain_summary AS
SELECT domain,
       count(*)                                        AS attempts,
       sum(CAST(is_correct AS INT))                    AS correct,
       round(100.0*avg(CAST(is_correct AS INT)), 1)    AS accuracy_pct
FROM v_all_attempts
GROUP BY domain
""")

# 分析ビュー2: 地域別の正答率（seed の履歴ベース。行フィルタ/PIIの文脈で地域軸を使う）
spark.sql("""
CREATE OR REPLACE VIEW v_region_summary AS
SELECT u.region, count(*) AS attempts,
       round(100.0*avg(CAST(f.is_correct AS INT)), 1) AS accuracy_pct
FROM fact_attempt f JOIN dim_user u USING (user_id)
GROUP BY u.region
""")

# 分析ビュー3: 苦手問題ランキング（seed＋アプリ、質問全文つき）
spark.sql("""
CREATE OR REPLACE VIEW v_hard_questions AS
SELECT a.question_id, a.domain, q.question,
       count(*) AS attempts,
       round(100.0*avg(CAST(a.is_correct AS INT)),1) AS accuracy_pct
FROM v_all_attempts a
JOIN dim_question q USING (question_id)
GROUP BY a.question_id, a.domain, q.question
HAVING count(*) >= 3
ORDER BY accuracy_pct ASC
""")

# ソース別の件数（seed / app が合流していることを確認）
display(spark.sql("SELECT source, count(*) AS attempts FROM v_all_attempts GROUP BY source"))
display(spark.table("v_domain_summary").orderBy("accuracy_pct"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space に登録するテーブル/ビュー
# MAGIC - **`v_all_attempts`**（seed＋アプリの統合明細・分析の主役）, `v_domain_summary`, `v_region_summary`, `v_hard_questions`
# MAGIC - `v_attempt_enriched`（seed明細・地域/PII用）, `app_attempts`（アプリ生ログ）
# MAGIC
# MAGIC ## Genie への質問例（日本語でOK）
# MAGIC - 「ドメイン別の正答率を低い順に見せて」
# MAGIC - 「アプリ（source=app）と履歴（source=seed）で正答率を比較して」
# MAGIC - 「私が一番苦手なドメインは？」
# MAGIC - 「地域ごとの正答率を比較して」
# MAGIC - 「正答率が50%未満の問題を、質問文つきで挙げて」
# MAGIC
# MAGIC ※ dim_user 由来の email 列はマスク/行フィルタが効くため、Genie の結果にもガバナンスが反映される。
# MAGIC ※ アプリで解くたびに `app_attempts` が増え、`v_all_attempts` 経由で集計に反映される（ループが閉じる）。