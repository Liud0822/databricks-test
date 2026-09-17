# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — 取り込み（COPY INTO）
# MAGIC **試験領域**: Data Ingestion（COPY INTO の冪等増分ロード / FILEFORMAT / FORMAT_OPTIONS / COPY_OPTIONS）
# MAGIC
# MAGIC Bronze = 生データをそのまま格納する層。`COPY INTO` は取り込み済みファイルを追跡し、
# MAGIC 再実行しても新規ファイルだけを取り込む（**冪等**）。Auto Loader 版はガイド末尾を参照。
# MAGIC
# MAGIC 問題データは `questions_full.csv`（実際の45問・全文）を単一ソースとして取り込む。

# COMMAND ----------

dbutils.widgets.text("catalog", "quiz_dev"); dbutils.widgets.text("schema", "quiz")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}"); spark.sql(f"USE SCHEMA {schema}")
vol = f"/Volumes/{catalog}/{schema}/raw"
print(catalog, schema, vol)

# COMMAND ----------

# DBTITLE 1,COPY INTO で Bronze 層をロード
# COPY INTO で CSV を Bronze テーブルに取り込む（冪等な増分ロード）
# 1) 初回のみ read_files で空テーブル(スキーマ)を作成、2) COPY INTO でロード
#    multiLine / escape を有効化してあるので、引用符・改行を含む複雑なCSV(questions_full)も安全。

def copy_into_bronze(table, filename):
    if not spark.catalog.tableExists(table):
        print(f"📦 {table} を新規作成中...")
        spark.sql(f"""
        CREATE TABLE {table}
        USING DELTA
        AS SELECT * FROM read_files(
            '{vol}/{filename}',
            format => 'csv',
            header => true,
            inferSchema => true,
            multiLine => true,
            escape => '"'
        )
        WHERE 1=0
        """)
    else:
        print(f"✓ {table} は既に存在（増分ロードモード）")

    result = spark.sql(f"""
    COPY INTO {table}
    FROM '{vol}/{filename}'
    FILEFORMAT = CSV
    FORMAT_OPTIONS (
        'header' = 'true',
        'inferSchema' = 'true',
        'multiLine' = 'true',
        'escape' = '"'
    )
    COPY_OPTIONS ('mergeSchema' = 'true')
    """)
    affected = result.first()["num_affected_rows"]
    count = spark.table(table).count()
    print(f"  → {affected} 行追加, 合計 {count} 行\n")

copy_into_bronze("bronze_users", "users.csv")
copy_into_bronze("bronze_questions", "questions_full.csv")   # ← 実際の45問(全文)を単一ソースに
copy_into_bronze("bronze_attempts", "attempts.csv")
copy_into_bronze("bronze_access_control", "access_control.csv")

print("\n✅ Bronze 層のロード完了（COPY INTO 使用）")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 冪等性の確認（重要な試験ポイント）
# MAGIC **上のセルを再実行**すると、COPY INTO は「既に取り込み済み」と判断し **num_affected_rows = 0**
# MAGIC を返す（同じファイルを二重取り込みしない = **冪等**）。

# COMMAND ----------

# DBTITLE 1,冪等性テスト
print("=== 冪等性テスト: 同じファイルを再取り込み ===\n")
result = spark.sql(f"""
COPY INTO bronze_users
FROM '{vol}/users.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
""")
display(result)
print("\n✅ num_affected_rows = 0 なら成功！COPY INTO は取り込み済みを追跡し二重取り込みを防ぐ（冪等）")

# COMMAND ----------

# DBTITLE 1,COPY INTO のメリット
# MAGIC %md
# MAGIC ### 📚 COPY INTO vs spark.read + overwrite
# MAGIC | 機能 | COPY INTO | spark.read + overwrite |
# MAGIC |------|-----------|------------------------|
# MAGIC | **冪等性** | ✅ 同じファイルはスキップ | ❌ 毎回全上書き |
# MAGIC | **増分ロード** | ✅ 新規ファイルだけ追加 | ❌ 全再読み込み |
# MAGIC | **ファイル追跡** | ✅ `_delta_log` に記録 | ❌ 追跡しない |
# MAGIC | **スキーマ進化** | ✅ `mergeSchema` 対応 | △ 手動 |
# MAGIC
# MAGIC #### 🎯 試験ポイント
# MAGIC 1. COPY INTO は自動で冪等 → 再実行しても安全
# MAGIC 2. **FORMAT_OPTIONS**: 読み取り設定 (header, inferSchema, multiLine, escape)
# MAGIC 3. **COPY_OPTIONS**: コピー動作設定 (mergeSchema, force)
# MAGIC 4. 大量ファイルの継続取り込みは **Auto Loader**（Structured Streaming）が上位互換
