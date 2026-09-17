# quiz-analytics-lab

Databricks（Free Edition）で DEA 試験範囲を串刺し実践するハンズオン。
**まず [LAB_GUIDE.md](./LAB_GUIDE.md) を読む**（手順・試験領域の対応・Free Edition の注意すべて記載）。

```
LAB_GUIDE.md                      手順書（プレイブック）
databricks.yml                    Automation Bundle（dev=quiz_dev / prod=quiz_prod）
resources/quiz_analytics.job.yml  Lakeflow ジョブ（00→05 の DAG）
src/00_setup.py … 05_genie.py     各ステップのノートブック
seed/                             合成データ（users/questions/attempts/access_control）
```

重点：**ガバナンス（PII / タグ / 列マスク / 行フィルタ / GRANT）** と **dev/prod（Bundle）**。
分析は **Genie**。単一ユーザーでも列マスク/行フィルタの効果を確認できる設計（`access_policy` 表方式）。
