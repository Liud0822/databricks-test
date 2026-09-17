# Databricks notebook source
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
# 既知スキーマでテーブルを作り、COPY INTO でロード（列はヘッダ名で対応付け）
def copy_csv(table, ddl, filename):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl})")
    spark.sql(f"""
        COPY INTO {table}
        FROM '{vol}/{filename}'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header'='true')
        COPY_OPTIONS  ('mergeSchema'='true')
    """)
    print(table, "->", spark.table(table).count(), "rows")

copy_csv("bronze_users",
         "user_id INT, display_name STRING, email STRING, region STRING, department STRING, is_owner INT",
         "users.csv")
copy_csv("bronze_questions",
         "question_id INT, domain STRING",
         "questions.csv")
copy_csv("bronze_attempts",
         "attempt_id INT, user_id INT, session_id STRING, mode STRING, question_id INT, is_correct INT, answered_at STRING",
         "attempts.csv")
copy_csv("bronze_access_control",
         "principal_email STRING, allowed_region STRING, pii_access INT",
         "access_control.csv")

# COMMAND ----------
# MAGIC %md
# MAGIC 再実行しても件数が増えないことを確認（＝COPY INTO は冪等。同じファイルは二重取り込みしない）。
