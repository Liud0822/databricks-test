# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · LDP メトリクス — 宣言的パイプライン（ライブ集計＋品質ゲート）
# MAGIC **試験領域**: Transformation & Modeling（Lakeflow Declarative Pipelines / ストリーミングテーブル / マテリアライズドビュー / expectations）
# MAGIC
# MAGIC アプリが `app_attempts` に追記するたびに、メトリクスを**増分で自動更新**する。
# MAGIC `03_gold`/`05_genie` の手動再実行を、この宣言的パイプラインが置き換える。
# MAGIC
# MAGIC - **ストリーミングテーブル** `ldp_app_attempts` … `app_attempts` を増分取り込み＋**expectations（品質ゲート）**
# MAGIC - **マテリアライズドビュー** `mv_domain_accuracy` … seed履歴＋アプリ解答のドメイン別正答率（自動更新）
# MAGIC
# MAGIC ※ これは Lakeflow パイプラインとして実行する（通常のノートブック実行ではない）。
# MAGIC   `resources/quiz_metrics.pipeline.yml` でデプロイ、`quiz_metrics.job.yml` の table update トリガーで起動。

# COMMAND ----------
import dlt
from pyspark.sql.functions import col, count, avg, round as _round, sum as _sum

# パイプライン configuration から受け取る（bundle が注入）
catalog = spark.conf.get("quiz.catalog")
schema  = spark.conf.get("quiz.schema")

# COMMAND ----------
# ストリーミングテーブル: app_attempts を増分取り込み。expectations で品質ゲート。
@dlt.table(
    name="ldp_app_attempts",
    comment="アプリ解答の増分取り込み（品質チェック付き）"
)
@dlt.expect_or_drop("valid_is_correct", "is_correct IN (0,1)")
@dlt.expect_or_drop("valid_question_id", "question_id IS NOT NULL")
@dlt.expect_or_drop("valid_answered_at", "answered_at IS NOT NULL")
def ldp_app_attempts():
    return spark.readStream.table(f"{catalog}.{schema}.app_attempts")

# COMMAND ----------
# マテリアライズドビュー: seed履歴(fact_attempt) ＋ アプリ解答 のドメイン別正答率。
# パイプライン実行のたびに増分/再計算され、常に最新になる。
@dlt.table(
    name="mv_domain_accuracy",
    comment="ドメイン別正答率（seed履歴＋アプリ解答・自動更新）"
)
def mv_domain_accuracy():
    seed = (spark.read.table(f"{catalog}.{schema}.fact_attempt")
            .select("domain", col("is_correct").cast("int").alias("is_correct")))
    app = (dlt.read("ldp_app_attempts")
           .select("domain", col("is_correct").cast("int").alias("is_correct")))
    allrows = seed.unionByName(app)
    return (allrows.groupBy("domain")
            .agg(count("*").alias("attempts"),
                 _sum("is_correct").alias("correct"),
                 _round(100.0 * avg("is_correct"), 1).alias("accuracy_pct")))
