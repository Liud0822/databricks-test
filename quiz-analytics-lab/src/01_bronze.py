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

# DBTITLE 1,COPY INTO で Bronze 層をロード
# COPY INTO で CSV を Bronze テーブルに取り込む（増分ロード）
# 重要: 初回のみテーブル作成、2回目以降は COPY INTO が新規ファイルだけを追加

def copy_into_bronze(table, filename):
    # 1. テーブルが存在しない場合のみ、空の Delta テーブルを作成
    table_exists = spark.catalog.tableExists(table)
    
    if not table_exists:
        print(f"📦 {table} を新規作成中...")
        spark.sql(f"""
        CREATE TABLE {table}
        USING DELTA
        AS SELECT * FROM read_files(
            '{vol}/{filename}',
            format => 'csv',
            header => true,
            inferSchema => true
        )
        WHERE 1=0
        """)
    else:
        print(f"✓ {table} は既に存在（増分ロードモード）")
    
    # 2. COPY INTO でデータをロード（冪等的な増分ロード）
    #    新規ファイルだけを取り込む。既存ファイルは自動スキップ。
    result = spark.sql(f"""
    COPY INTO {table}
    FROM '{vol}/{filename}'
    FILEFORMAT = CSV
    FORMAT_OPTIONS (
        'header' = 'true',
        'inferSchema' = 'true'
    )
    COPY_OPTIONS (
        'mergeSchema' = 'true'
    )
    """)
    
    # 取り込み結果を表示
    affected_rows = result.first()["num_affected_rows"]
    count = spark.table(table).count()
    print(f"  → {affected_rows} 行追加, 合計 {count} 行\n")

copy_into_bronze("bronze_users", "users.csv")
copy_into_bronze("bronze_questions", "questions.csv")
copy_into_bronze("bronze_attempts", "attempts.csv")
copy_into_bronze("bronze_access_control", "access_control.csv")

print("\n✅ Bronze 層のロード完了（COPY INTO 使用）")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 冪等性の確認（重要な試験ポイント）
# MAGIC
# MAGIC **このセルを再実行**すると、COPY INTO は「既に取り込み済み」と判断し、
# MAGIC **0 rows affected** を返す（同じファイルを二重取り込みしない = **冪等**）。
# MAGIC
# MAGIC 試してみよう：👆 上のセルを再実行 → ログに `0 rows affected` と表示される。

# COMMAND ----------

# DBTITLE 1,冪等性テスト
# 冪等性テスト: 同じ COPY INTO を再実行
print("=== 冪等性テスト: 同じファイルを再取り込み ===\n")

# users.csv を再度 COPY INTO
result = spark.sql(f"""
COPY INTO bronze_users
FROM '{vol}/users.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
""")

print("\nCOPY INTO の結果:")
display(result)

print("\n✅ num_affected_rows = 0 なら成功！")
print("💡 COPY INTO は取り込み済みファイルを追跡し、二重取り込みを防ぐ（= 冪等）")

# COMMAND ----------

# DBTITLE 1,COPY INTO のメリット
# MAGIC %md
# MAGIC ### 📚 COPY INTO vs spark.read + overwrite
# MAGIC
# MAGIC | 機能 | COPY INTO | spark.read + overwrite |
# MAGIC |------|-----------|------------------------|
# MAGIC | **冪等性** | ✅ 同じファイルはスキップ | ❌ 毎回全データ上書き |
# MAGIC | **増分ロード** | ✅ 新規ファイルだけ追加 | ❌ 全ファイル再読み込み |
# MAGIC | **ファイル追跡** | ✅ `_delta_log` に記録 | ❌ 追跡しない |
# MAGIC | **パフォーマンス** | ✅ 差分のみ処理 | ❌ 全データスキャン |
# MAGIC | **スキーマ進化** | ✅ `mergeSchema` 対応 | △ 手動対応 |
# MAGIC
# MAGIC #### 🎯 試験ポイント
# MAGIC
# MAGIC 1. **COPY INTO は自動で冪等** → 再実行しても安全
# MAGIC 2. **FORMAT_OPTIONS**: CSV の読み取り設定 (header, inferSchema, delimiter)
# MAGIC 3. **COPY_OPTIONS**: コピー動作設定 (mergeSchema, force)
# MAGIC 4. **増分ロード**: 新規ファイルだけ取り込むことで大量データでも効率的
# MAGIC
# MAGIC #### 💡 実務パターン
# MAGIC
# MAGIC ```sql
# MAGIC -- 新規ファイルが増えた場合、再実行するだけ：
# MAGIC COPY INTO bronze_table
# MAGIC FROM '/Volumes/catalog/schema/raw/'
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'data_*.csv'  -- パターンマッチ
# MAGIC FORMAT_OPTIONS ('header' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC ```
# MAGIC
# MAGIC ※ Auto Loader （Structured Streaming）はさらに高機能だが、COPY INTO の方がシンプルで試験頑出も高い。