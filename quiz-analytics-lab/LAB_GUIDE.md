# 模擬題分析システム — ハンズオン・プレイブック

Databricks（**Free Edition**）で、DEA 試験の実践可能な範囲を **1本のプロジェクトに串刺し**で体験するラボです。
データは「模擬題アプリの受験履歴」＋「合成ユーザー次元」。重点は **ガバナンス（PII / タグ / 列マスク / 行フィルタ）** と **dev/prod（Automation Bundle）**。

---

## 0. 全体像

```
seed CSV ──(COPY INTO)──▶ Bronze ──(clean/型/dedup)──▶ Silver ──(結合/集計/Liquid Clustering)──▶ Gold
                                                                                    │
   dev/prod をカタログで分離（Automation Bundle の variables）                        ├─ 列マスク / 行フィルタ / タグ / GRANT
   Lakeflow ジョブで 00→05 を DAG 実行                                                └─ Genie で自然言語分析
```

### 試験領域カバー表

| フェーズ | 主な試験領域 | 実践する内容 |
|---|---|---|
| 1 セットアップ | Platform（UC階層・サーバーレス） | カタログ/スキーマ/ボリューム作成 |
| 2 Bronze | Data Ingestion | **COPY INTO**（冪等増分）＋（発展）Auto Loader |
| 3 Silver/Gold | Transformation・最適化 | 型変換/dedup/結合/集計/**Liquid Clustering** |
| 4 ガバナンス | **Governance & Security** | **列マスク・行フィルタ・タグ・GRANT**・（発展）ABAC |
| 5 dev/prod | **CI/CD** | **Automation Bundle** の targets/variables で環境昇格 |
| 6 ジョブ化 | Lakeflow Jobs | マルチタスク DAG・パラメータ・トリガー |
| 7 分析 | Platform（BI/SQL） | **Genie** で自然言語分析 |

### 前提
- Databricks **Free Edition**（サーバーレス、ワークスペース/メタストア各1、アカウントコンソール無し）。
- **既にやった** git リポジトリ作成・Databricks への clone・`bundle init` は**スキップOK**。ここでは用意済みのファイルを使う。
- ターミナルで `databricks` CLI が使える（bundle を試すなら）。CLI を使わず**全部ノートブック手動**でも一通り実践できる。

> Free Edition で `CREATE CATALOG` が拒否される場合の回避策は各所に併記。基本は「カタログを `workspace` にして、スキーマを `quiz_dev` / `quiz_prod` にする」だけで全部動く（ノートブックは catalog/schema の2変数で完全パラメータ化済み）。

---

## 1. リポジトリにファイルを置く

`quiz-analytics-lab/` の中身を、あなたの Git リポジトリ（`databricks-test`）に入れます。方法はどちらでも：

- **ローカルで**：`databricks-test` のローカルクローンに `quiz-analytics-lab/` をコピー → `git add . && git commit && git push`。Databricks 側の Git フォルダで **Pull**。
- **Databricks 上で**：Workspace の Git フォルダ内に同じ構成でファイルを作成（Assistant に「このファイルを作って」と貼り付け）。

> **試験ポイント（CI/CD / Repos）**：Git フォルダはブランチ作成・コミット・プッシュ・PR ができる外部Git連携。開発は dev ブランチ、本番反映は Bundle deploy、という流れを意識。

---

## 2. セットアップ（`src/00_setup.py`）

ノートブックを開き、上部ウィジェットに `catalog=quiz_dev`, `schema=quiz` を入れて実行。

やること：`CREATE CATALOG quiz_dev` → `CREATE SCHEMA quiz` → `CREATE VOLUME raw`。

**seed の CSV をアップロード**：Catalog Explorer → `quiz_dev` → `quiz` → Volumes → `raw` → Upload で、`seed/` の
`users.csv` / `questions_full.csv` / `attempts.csv` / `access_control.csv` を置く（パスは `/Volumes/quiz_dev/quiz/raw/`）。
※ 問題は `questions_full.csv`（実際の45問・全文）を単一ソースにしたので、薄い `questions.csv` は不要（削除可）。

> **試験ポイント（Platform）**：Unity Catalog は メタストア→カタログ→スキーマ→テーブル/ボリューム の階層。ボリュームは「ファイルを置く UC 管理の場所」で、取り込み元として使う。

---

## 3. Bronze — 取り込み（`src/01_bronze.py`）

`COPY INTO` で 4 表をロード。**もう一度実行しても件数が増えない**ことを確認（＝冪等）。

手動でも試せる SQL（SQL Editor 用、`<cat>` は quiz_dev）：
```sql
COPY INTO <cat>.quiz.bronze_users
FROM '/Volumes/<cat>/quiz/raw/users.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true')
COPY_OPTIONS  ('mergeSchema'='true');
```

> **試験ポイント（Ingestion）**：`COPY INTO <table> FROM '<location>' FILEFORMAT=<fmt> [FORMAT_OPTIONS(...)] [COPY_OPTIONS(...)]`。取り込み済みファイルを追跡し、再実行では新規ファイルだけを読む。少量・定期・SQL 完結向き。大量継続なら Auto Loader（発展編）。

---

## 4. Silver / Gold（`src/02_silver.py`, `src/03_gold.py`）

- **Silver**：型変換（`is_correct`→boolean、`answered_at`→timestamp）、null 除去、`QUALIFY row_number()...` で重複除去。
- **Gold**：`dim_user` / `dim_question` / `fact_attempt`（domain を結合で付与、**`CLUSTER BY (domain, user_id)`** で Liquid Clustering）/ `agg_domain_accuracy`（ユーザー×ドメイン正答率）/ `v_attempt_enriched`（Genie 用結合ビュー）。

> **試験ポイント（Transformation・最適化）**：メダリオン（Bronze=生, Silver=整形, Gold=集計）。`CLUSTER BY` はパーティション＋ZORDER を置き換える Liquid Clustering で、キーを後から変更しても全書き換え不要。dedup は `dropDuplicates`/`distinct`/`QUALIFY row_number`。

---

## 5. ガバナンス（`src/04_governance.py`）★重点

**設計のポイント**：単一ユーザーでも効果を見られるよう、権限を**グループではなく `access_policy` 表**で管理。
`current_user()`（＝ログイン中のあなた）の行に `pii_access`（PIIを素で見るか）と `allowed_region`（見える地域）を持たせ、
その行を書き換えると列マスク/行フィルタの挙動が切り替わる。

流れ：
1. `access_policy` を作り、`current_user()` の行を **full access** で登録。
2. **列マスク**：`mask_email` UDF を作り、`ALTER TABLE dim_user ALTER COLUMN email SET MASK mask_email`。
3. **行フィルタ**：`rf_region` UDF を作り、`ALTER TABLE dim_user SET ROW FILTER rf_region ON (region)`。
4. **動作確認**：full access では email 素・全地域 → 自分の行を `pii_access=false, allowed_region='Kanto'` に更新して再クエリ → **email がマスクされ Kanto の行だけ**になる。
5. **タグ**：`ALTER TABLE dim_user ALTER COLUMN email SET TAGS ('pii'='true')` などで PII を明示。
6. **GRANT**：グループがあれば `GRANT SELECT ON TABLE ... TO \`analysts\``。

手動で試せる核心 SQL：
```sql
-- 列マスク関数
CREATE OR REPLACE FUNCTION quiz.mask_email(email STRING)
RETURN CASE WHEN (SELECT COALESCE(max(pii_access),false) FROM quiz.access_policy WHERE principal=current_user())
            THEN email ELSE concat('***@', split(email,'@')[1]) END;
ALTER TABLE quiz.dim_user ALTER COLUMN email SET MASK quiz.mask_email;

-- 行フィルタ関数
CREATE OR REPLACE FUNCTION quiz.rf_region(region STRING)
RETURN EXISTS (SELECT 1 FROM quiz.access_policy
               WHERE principal=current_user() AND (allowed_region='ALL' OR allowed_region=region));
ALTER TABLE quiz.dim_user SET ROW FILTER quiz.rf_region ON (region);

-- 権限を絞って挙動を確認（元に戻すには true / 'ALL' に）
UPDATE quiz.access_policy SET pii_access=false, allowed_region='Kanto' WHERE principal=current_user();
SELECT user_id, display_name, email, region FROM quiz.dim_user ORDER BY user_id;
```

> **試験ポイント（Governance）**：
> - **列マスク**＝SQL UDF＋`ALTER COLUMN ... SET MASK`（列の値を条件で隠す）。
> - **行フィルタ**＝真偽を返す UDF＋`SET ROW FILTER ... ON (col)`（見える行を制限）。
> - どちらも UC が一元管理。実運用では `is_account_group_member('グループ')` で判定するのが定番（ここではグループが作れない場合に備えマッピング表方式）。
> - **タグ**でデータ分類（どこにPIIがあるか）。**GRANT/REVOKE** はメタストア→カタログ→スキーマ→テーブルの階層で、`USE CATALOG`/`USE SCHEMA`＋`SELECT` が最小権限。

---

## 6. dev/prod（Automation Bundle）★重点（`databricks.yml`）

同じコードを **dev（quiz_dev）** と **prod（quiz_prod）** へ、カタログだけ変えて配置する。

```bash
# リポジトリのルート（databricks.yml がある場所）で
databricks bundle validate                 # 構文/構成の検証
databricks bundle deploy -t dev            # dev へ配置（quiz_dev）
databricks bundle run quiz_analytics_job -t dev   # dev で実行

databricks bundle deploy -t prod           # prod へ昇格（quiz_prod）
databricks bundle run quiz_analytics_job -t prod
```

`databricks.yml` の要点：`targets.dev` と `targets.prod` で `variables.catalog` を `quiz_dev` / `quiz_prod` に切り替え。
ジョブの各タスクへ `catalog`/`schema` を `base_parameters` で配布 → **1つのコードが両環境で動く**。

> **試験ポイント（CI/CD）**：Automation Bundle（旧 Databricks Asset Bundles）は `databricks.yml` にリソースと targets を宣言。`variables`/`overrides` で環境差を吸収し、CLI（validate→deploy→run）で再現可能に昇格。手動コピーやハードコードは非再現的。

**Free で CREATE CATALOG 不可のとき**：`databricks.yml` の dev を `catalog: workspace, schema: quiz_dev`、prod を `catalog: workspace, schema: quiz_prod` に変更。これだけで dev/prod 分離が成立する（カタログ分離→スキーマ分離へのフォールバック）。

---

## 7. ジョブ化とトリガー（`resources/quiz_analytics.job.yml`）

Bundle deploy でジョブ `quiz-analytics [...]` が作られる。00→01→02→03→04→05 を **依存関係（DAG）**で直列実行。

- **Repair run**：どこかで失敗したら、修正後に「Repair run」で失敗タスク以降だけ再実行（全体再実行しない）。
- **トリガー**：既定は日次スケジュール（停止状態）。データ駆動にするなら、UI でトリガーを **file arrival**（Volume にファイル到着で起動）や **table update**（テーブル更新で起動）へ変更。

> **試験ポイント（Lakeflow Jobs）**：タスクの依存（depends_on）で DAG を定義、`base_parameters` で値を渡す、トリガー種別は scheduled / file arrival / table update。失敗時は Repair run。

---

## 8. Genie で分析（`src/05_genie.py`）

1. ノートブックで分析ビュー（`v_domain_summary` 他）を作成。
2. 左メニュー **Genie**（または Genie Agents）→ New Space → データに `quiz_dev.quiz` の
   **`v_all_attempts`**（seed＋アプリ統合）/ `v_domain_summary` / `v_region_summary` / `v_hard_questions` / `v_attempt_enriched` を追加。
3. 日本語で質問：
   - 「ドメイン別の正答率を低い順に見せて」
   - 「私が一番苦手なドメインは？」
   - 「地域ごとの正答率を比較して」
   - 「正答率が50%未満の問題を挙げて」

> `dim_user` にマスク/行フィルタが効いているので、Genie の結果にもガバナンスが反映される（PII は保護されたまま分析できる、を体感）。

---

## 9. 発展編（余力があれば）

- **Auto Loader**（Ingestion 深掘り）：Bronze を COPY INTO の代わりにストリーミング取り込みで。
  ```python
  (spark.readStream.format("cloudFiles")
     .option("cloudFiles.format","csv").option("header","true")
     .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/raw/_schema/attempts")
     .load(f"/Volumes/{catalog}/{schema}/raw/")
     .writeStream.option("checkpointLocation", f"/Volumes/{catalog}/{schema}/raw/_chk/attempts")
     .trigger(availableNow=True).toTable("bronze_attempts_al"))
  ```
- **ABAC（Governance 一元管理）**：ガバナンスタグ `pii` を定義し、ABAC の列マスク/行フィルタ**ポリシー**をスキーマに付与すると、`pii` タグの付いた列すべてに一括適用できる（個別 `SET MASK` 不要）。Catalog Explorer → Governance、またはポリシーSQLで作成。環境で使えれば試す（5章の個別適用だけでも試験観点は満たす）。
- **Liquid Clustering の効果測定**：`fact_attempt` に `OPTIMIZE` を実行、`DESCRIBE DETAIL` でクラスタリング列を確認。
- **監視**：ジョブの run history で実行時間の推移を確認。Spark UI でステージ/タスクの偏り（スキュー）を観察。

---

## 10. 実データを混ぜる（任意）

模擬題アプリ（`index.html`）に **エクスポート**ボタンを追加済み。トップの「データ書き出し（分析用）」から
`quiz_attempts.csv` / `quiz_stats.csv` / `quiz_questions.csv` をダウンロードし、`raw` ボリュームに追加アップロードすれば、
自分の実受験データも同じパイプラインで分析できる（列は seed と互換）。単一ユーザー分なので、合成データと併用して分析する。

---

## トラブルシュート / Free Edition の注意
- **`CREATE CATALOG` が失敗** → 6章のフォールバック（catalog=workspace、schema=quiz_dev/quiz_prod）。
- **`SET MASK`/`SET ROW FILTER` が失敗** → サーバーレスかつ UC 管理テーブルであることを確認（bronze/silver/gold は UC 管理テーブル）。
- **グループ GRANT ができない** → 単一ユーザーでは対象グループが無い場合あり。構文の確認までで可（マスク/行フィルタの体感は access_policy 方式で完結）。
- **同時タスク上限** → Free は同時タスク最大5。本ジョブは直列なので影響なし。
- **ボリュームにファイルが見えない** → パスは `/Volumes/<catalog>/<schema>/raw/`。アップロード先カタログ/スキーマを再確認。

---

### ファイル構成
```
quiz-analytics-lab/
├── LAB_GUIDE.md                     ← このファイル
├── databricks.yml                   ← Automation Bundle（dev/prod）
├── resources/quiz_analytics.job.yml ← Lakeflow ジョブ
├── src/00_setup.py … 05_genie.py    ← 各ステップのノートブック
└── seed/                            ← 合成データ（users/questions/attempts/access_control）
```
