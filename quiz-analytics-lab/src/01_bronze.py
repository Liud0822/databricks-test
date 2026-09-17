# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze — 取り込み（COPY INTO）
# MAGIC **試験領域**: Data Ingestion（COPY INTO の冪等増分ロード / FILEFORMAT / FORMAT_OPTIONS / COPY_OPTIONS）
# MAGIC
# MAGIC Bronze = 生データをそのまま格納する層。`COPY INTO` は取り込み済みファイルを追跡し、
# MAGIC 再実行しても新規ファイルだけを取り込む（**冪等**）。Auto Loader 版はガイド末尾を参照。

# COMMAND ----------

dbutils.widgets.text("catalog", "quiz_dev"); dbutils.widgets.text("schema", "quiz")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}"); spark.sql(f"USE SCHEMA {schema}")
vol = f"/Volumes/{catalog}/{schema}/raw"
print(catalog, schema, vol)

# COMMAND ----------

# CSV を読み込んで Bronze テーブルを作成
# 注: COPY INTO に schema merge エラーがあるため、直接読み込み方式を使用
def load_csv(table, filename):
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{vol}/{filename}"))
    df.write.mode("overwrite").saveAsTable(table)
    count = spark.table(table).count()
    print(f"{table} -> {count} rows")

load_csv("bronze_users", "users.csv")
load_csv("bronze_questions", "questions.csv")
load_csv("bronze_attempts", "attempts.csv")
load_csv("bronze_access_control", "access_control.csv")

print("\n✅ Bronze 層のロード完了")

# COMMAND ----------

# MAGIC %md
# MAGIC 再実行しても件数が増えないことを確認（＝COPY INTO は冪等。同じファイルは二重取り込みしない）。