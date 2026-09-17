# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · App 用テーブル — questions_app / app_attempts
# MAGIC **試験領域**: Ingestion（読み込み）, Governance（App SP への GRANT）
# MAGIC
# MAGIC 出題アプリ（Databricks App）が読む問題表と、解答を書き込む表を用意する。
# MAGIC `seed/questions_full.csv` を `raw` ボリュームにアップロードしてから実行。

# COMMAND ----------
dbutils.widgets.text("catalog", "quiz_dev"); dbutils.widgets.text("schema", "quiz")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}"); spark.sql(f"USE SCHEMA {schema}")
vol = f"/Volumes/{catalog}/{schema}/raw"

# COMMAND ----------
# 問題表（全文）: options_json は JSON 文字列。複雑なテキストなので spark.read で堅牢に読む。
df = (spark.read
      .option("header", "true").option("multiLine", "true").option("escape", '"')
      .csv(f"{vol}/questions_full.csv"))
df = (df.withColumn("question_id", df.question_id.cast("int"))
        .withColumn("answer", df.answer.cast("int")))
df.write.mode("overwrite").saveAsTable("questions_app")
print("questions_app:", spark.table("questions_app").count(), "rows")

# COMMAND ----------
# 解答保存表（アプリが INSERT する）。domain を持たせて fact と形を揃える → v_all_attempts で合流。
spark.sql("""
CREATE TABLE IF NOT EXISTS app_attempts (
  attempt_id STRING, principal STRING, session_id STRING, mode STRING,
  question_id INT, domain STRING, is_correct INT, answered_at TIMESTAMP
)""")
# 既存表（旧スキーマ）が残っていても domain 列を追加
spark.sql("ALTER TABLE app_attempts ADD COLUMN IF NOT EXISTS domain STRING")
print("app_attempts ready")

# COMMAND ----------
# MAGIC %md
# MAGIC ## App サービスプリンシパルへの権限付与（App 作成後に実行）
# MAGIC App を作るとサービスプリンシパル（例: `app-xxxx`）が発行される。App 詳細画面で名前を確認し、
# MAGIC 下の `<APP_SP>` を置き換えて実行する。**読み取り＝questions_app、書き込み＝app_attempts**。

# COMMAND ----------
APP_SP = "<APP_SP>"  # ← App のサービスプリンシパル名/ID に置き換える
if APP_SP != "<APP_SP>":
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{APP_SP}`")
    spark.sql(f"GRANT USE SCHEMA  ON SCHEMA  {catalog}.{schema} TO `{APP_SP}`")
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.questions_app TO `{APP_SP}`")
    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog}.{schema}.app_attempts TO `{APP_SP}`")
    print("granted to", APP_SP)
else:
    print("APP_SP を置き換えてから再実行してください")

# COMMAND ----------
# MAGIC %md
# MAGIC ## （任意）アプリの解答を分析ファクトに合流
# MAGIC 分析側（03_gold）の `fact_attempt` に app_attempts も含めたい場合は、03 の fact 生成を
# MAGIC 「silver_attempts ∪ app_attempts」に拡張する。まずは別表のままでも Genie で app_attempts を直接分析できる。
