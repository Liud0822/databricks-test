# quiz-analytics-lab

DEA 試験範囲を1つのプロジェクトで実践するハンズオン（→ 全体像はリポジトリ直下の [README](../README.md)）。

- **分析基盤**の手順 → [LAB_GUIDE.md](./LAB_GUIDE.md)
- **出題アプリ（Databricks App）**の手順 → [APP_GUIDE.md](./APP_GUIDE.md)

```
databricks.yml                    Asset Bundle（dev=quiz_dev / prod=quiz_prod）
resources/quiz_analytics.job.yml  Lakeflow ジョブ（00→06 の DAG）
src/00_setup … 06_load_app_tables 各ステップのノートブック
app/                              出題アプリ（Streamlit・解答を Delta へ書込）
seed/                             合成データ（6ユーザー/4地域・約500受験・45問・権限表）
```

## 実行順（重要）
`00_setup` → seed を `raw` ボリュームへアップロード → `01`→`02`→`03`→`04`→`05`。
アプリを使うなら `06_load_app_tables` → APP_GUIDE。

> `03_gold` は `CREATE OR REPLACE TABLE dim_user` でテーブルを作り直す（＝マスク/タグ/行フィルタが消える）。
> **必ず 03 を流してから 04（ガバナンス）** を実行すること。

## 重点
- **ガバナンス**：列マスク・行フィルタ・ガバナンスタグ・**ABACポリシー**（`class.email_address` タグに自動適用）・GRANT。
  単一ユーザーでも `access_policy` 表を書き換えて効果を確認できる設計。
- **dev/prod**：Bundle の targets/variables でカタログを切替（`validate → deploy → run`）。
- **分析**：Genie で自然言語質問／出題アプリの解答（`app_attempts`）も同じ基盤で分析。

非公式・学習用。データはすべて合成。
