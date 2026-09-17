# Databricks notebook source
# DBTITLE 1,读取环境变量
# 从 Bundle 配置中读取 catalog 和 schema 变量
catalog = spark.conf.get("bundle.catalog")
schema = spark.conf.get("bundle.schema")

print(f"当前环境使用:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")

# COMMAND ----------

# DBTITLE 1,创建示例表
# 使用环境变量创建表
table_name = f"{catalog}.{schema}.sample_data"

# 创建示例 DataFrame
data = [
    (1, "Alice", "dev"),
    (2, "Bob", "prod"),
    (3, "Charlie", "staging")
]
df = spark.createDataFrame(data, ["id", "name", "environment"])

# 保存到 Unity Catalog
df.write.mode("overwrite").saveAsTable(table_name)

print(f"表已创建: {table_name}")

# COMMAND ----------

# DBTITLE 1,读取数据
# 从环境特定的表读取数据
result_df = spark.read.table(f"{catalog}.{schema}.sample_data")

display(result_df)

# COMMAND ----------

# DBTITLE 1,SQL 示例
# MAGIC %sql
# MAGIC -- 注意：在 SQL 中需要通过 Job parameters 传递变量
# MAGIC -- 在 databricks.yml 的 job 定义中配置：
# MAGIC --   parameters:
# MAGIC --     - name: catalog
# MAGIC --       default: ${var.catalog}
# MAGIC --     - name: schema  
# MAGIC --       default: ${var.schema}
# MAGIC
# MAGIC -- 然后在 SQL 中使用：
# MAGIC USE CATALOG ${catalog};
# MAGIC USE SCHEMA ${schema};
# MAGIC
# MAGIC SELECT * FROM sample_data;