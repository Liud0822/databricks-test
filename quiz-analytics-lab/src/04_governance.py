# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Governance — PII / タグ / 列マスク / 行フィルタ / GRANT
# MAGIC **試験領域**: Governance & Security（列マスク, 行フィルタ, タグ, GRANT/REVOKE, ABAC）
# MAGIC
# MAGIC 単一ユーザーでも効果を確認できるよう、グループではなく **access_policy テーブル**で
# MAGIC 「現在のユーザーの権限」を管理する（自分の行を書き換えるとマスク/フィルタの挙動が変わる）。

# COMMAND ----------
dbutils.widgets.text("catalog", "quiz_dev"); dbutils.widgets.text("schema", "quiz")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}"); spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1) 権限ルックアップ表を用意し、現在のユーザーの行を登録
# MAGIC `pii_access` = PII（email）を素で見られるか / `allowed_region` = 見える地域（'ALL'なら全部）

# COMMAND ----------
spark.sql("""
CREATE TABLE IF NOT EXISTS access_policy (
  principal STRING, pii_access BOOLEAN, allowed_region STRING
)""")
me = spark.sql("SELECT current_user()").collect()[0][0]
spark.sql(f"DELETE FROM access_policy WHERE principal = '{me}'")
# 既定は full access（まず素のデータが見える状態にしておく）
spark.sql(f"INSERT INTO access_policy VALUES ('{me}', true, 'ALL')")
print("current_user =", me)
display(spark.table("access_policy"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2) 列マスク（Column Mask）— email を pii_access に応じてマスク
# MAGIC マスク関数（SQL UDF）はルックアップ表を参照する。`ALTER TABLE ... SET MASK` で列に適用。

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURN CASE
  WHEN (SELECT COALESCE(max(pii_access), false) FROM access_policy WHERE principal = current_user())
    THEN email
  ELSE concat('***@', split(email, '@')[1])
END
""")
try:
    spark.sql("ALTER TABLE dim_user ALTER COLUMN email SET MASK mask_email")
    print("column mask applied on dim_user.email")
except Exception as e:
    print("mask 既に適用済みか再適用不可（再実行時は正常）:", str(e)[:120])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3) 行フィルタ（Row Filter）— region を allowed_region で制限
# MAGIC 真偽を返す UDF を作り、`ALTER TABLE ... SET ROW FILTER ... ON (列)` で適用。

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE FUNCTION rf_region(region STRING)
RETURN EXISTS (
  SELECT 1 FROM access_policy
  WHERE principal = current_user()
    AND (allowed_region = 'ALL' OR allowed_region = region)
)
""")
try:
    spark.sql("ALTER TABLE dim_user SET ROW FILTER rf_region ON (region)")
    print("row filter applied on dim_user(region)")
except Exception as e:
    print("row filter 既に適用済みか再適用不可（再実行時は正常）:", str(e)[:120])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4) 動作確認（ここが実験の山場）
# MAGIC まず今は full access なので email は素・全地域が見える。
# MAGIC 次のセルで自分の権限を「pii_access=false, allowed_region='Kanto'」に変えて再クエリすると、
# MAGIC **email がマスクされ、Kanto の行だけ**になる。

# COMMAND ----------
print("=== full access のとき ===")
display(spark.sql("SELECT user_id, display_name, email, region FROM dim_user ORDER BY user_id"))

# COMMAND ----------
# 権限を絞る（PII不可・関東のみ）→ 再クエリでマスク＆行制限を体感
spark.sql(f"UPDATE access_policy SET pii_access = false, allowed_region = 'Kanto' WHERE principal = '{me}'")
print("=== pii_access=false, allowed_region=Kanto のとき ===")
display(spark.sql("SELECT user_id, display_name, email, region FROM dim_user ORDER BY user_id"))
# 元に戻す: spark.sql(f"UPDATE access_policy SET pii_access=true, allowed_region='ALL' WHERE principal='{me}'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5) タグ付け（データ分類）と GRANT
# MAGIC 列・テーブルにタグを付けて「どこに PII があるか」を明示。GRANT はグループがあれば
# MAGIC グループへ、無ければ構文の確認まで（Free の単一ユーザーでは対象グループが無い場合がある）。

# COMMAND ----------
spark.sql("ALTER TABLE dim_user ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_type' = 'email')")
spark.sql("ALTER TABLE dim_user SET TAGS ('classification' = 'confidential')")
print("tags applied")

# GRANT の例（試験構文）。`analysts` グループが存在すれば実行、無ければコメントのままでOK。
grant_sql = f"""
-- GRANT USE CATALOG ON CATALOG {catalog} TO `analysts`;
-- GRANT USE SCHEMA  ON SCHEMA  {catalog}.{schema} TO `analysts`;
-- 読み取り専用（集計テーブルのみ）を付与:
-- GRANT SELECT ON TABLE {catalog}.{schema}.agg_domain_accuracy TO `analysts`;
"""
print(grant_sql)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6) （発展）ABAC — タグ＋ポリシーで横断適用
# MAGIC ガバナンスタグ＋ABACポリシーが使える環境なら、`pii` タグの付いた列を **1つのポリシー**で
# MAGIC 一括マスクできる（個別 SET MASK 不要）。UI（Catalog Explorer → Governance）または
# MAGIC ポリシー用SQLで作成する。詳細と可否はガイドの「発展編」を参照。
# MAGIC ※ 上の 2)〜3) だけで試験のマスク/行フィルタ観点は満たせる。ABAC は上位の一元管理。
