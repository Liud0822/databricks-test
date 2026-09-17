# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — カタログ / スキーマ / ボリューム
# MAGIC **試験領域**: Databricks Intelligence Platform（UC 階層 / サーバーレス）, Governance（オブジェクト作成）
# MAGIC
# MAGIC Unity Catalog 階層: **メタストア → カタログ → スキーマ → テーブル/ボリューム**。
# MAGIC ここでは dev/prod を **カタログ**で分ける（`quiz_dev` / `quiz_prod`）。

# COMMAND ----------
dbutils.widgets.text("catalog", "quiz_dev")
dbutils.widgets.text("schema", "quiz")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
print("catalog =", catalog, "| schema =", schema)

# COMMAND ----------
# カタログ / スキーマ / 取り込み用ボリュームを作成
# ※ Free Edition で CREATE CATALOG が拒否される場合は、ジョブ/ウィジェットで
#    catalog="workspace"、schema="quiz_dev"（prodは"quiz_prod"）に変えるだけでOK。
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {catalog}.{schema}.raw")   # マネージドボリューム
print("OK")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 次の手順
# MAGIC 下に表示されるパスへ、`seed/` の CSV（users, questions, attempts, access_control）を
# MAGIC アップロードしてください（Catalog Explorer → Volume → Upload、または CLI `databricks fs cp`）。

# COMMAND ----------
print(f"取り込み元パス: /Volumes/{catalog}/{schema}/raw/")
# アップロード後、ここでファイル一覧を確認できる:
# display(spark.sql(f"LIST '/Volumes/{catalog}/{schema}/raw/'"))
