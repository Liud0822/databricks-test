# 出題アプリ（Databricks App）セットアップ

答題アプリを **Streamlit の Databricks App** として動かし、解答を Delta（`app_attempts`）へ書き込む。
これで「アプリで解く → UC に溜まる → 分析システム（Genie/集計）で分析」のループが閉じる。

**試験領域**: Platform（Apps / SQLウェアハウス）, Governance（App SP への GRANT）, Ingestion（表ロード）

> Free Edition: アプリは最大3つ、起動から24時間で自動停止（いつでも再起動可）。

---

## 前提
- 分析ラボの `00_setup`（カタログ/スキーマ/ボリューム）が済んでいること。
- サーバーレス **SQL ウェアハウス**が1つあること（無ければ SQL Warehouses で作成）。

## 手順

**1) 問題表と解答表を作る**
`seed/questions_full.csv` を `/Volumes/quiz_dev/quiz/raw/` にアップロード →
`src/06_load_app_tables.py` を実行（`catalog=quiz_dev, schema=quiz`）。
→ `questions_app`（45問・全文）と `app_attempts`（空）ができる。

**2) App を作成**
左メニュー **Compute → Apps → Create app**（または Git フォルダから）。
ソースを Git フォルダの `quiz-analytics-lab/app`（`app.py` / `app.yaml` / `requirements.txt` がある場所）に設定。

**3) SQL ウェアハウスを接続**
App の **Edit → Resources** で SQL warehouse を追加し、その値を環境変数
**`DATABRICKS_WAREHOUSE_ID`** に割り当てる（`app.py` がこれを使って接続する）。

**4) App サービスプリンシパルに権限付与**
App 詳細画面で SP 名（例 `app-xxxx`）を確認 → `06_load_app_tables` 末尾のセルの `APP_SP` を
その名前に置き換えて実行。付与内容:
```sql
GRANT USE CATALOG ON CATALOG quiz_dev TO `<APP_SP>`;
GRANT USE SCHEMA  ON SCHEMA  quiz_dev.quiz TO `<APP_SP>`;
GRANT SELECT ON TABLE quiz_dev.quiz.questions_app TO `<APP_SP>`;      -- 読み取り
GRANT SELECT, MODIFY ON TABLE quiz_dev.quiz.app_attempts TO `<APP_SP>`; -- 書き込み
```
> **試験ポイント**：App は自分の SP で SQL ウェアハウス経由で UC にアクセスする。UC の最小権限（USE＋SELECT／MODIFY）を SP に付ける、が要点。

**5) Deploy して開く**
Deploy → URL を開く → 「開始する」で10問。結果画面で `app_attempts` に保存される。

**6) 分析につなぐ（ループが閉じる）**
- `05_genie` を実行すると **`v_all_attempts`（seedの履歴 ∪ アプリの解答）** が作られる。アプリで解くたびに
  `app_attempts` が増え、このビュー経由で集計に自動反映される＝「アプリで解く→分析」が繋がる。
- Genie Space には `v_all_attempts` / `v_domain_summary` / `v_hard_questions` を登録。
  「アプリ(source=app)と履歴(source=seed)で正答率を比較して」などを質問できる。
- `app_attempts` は `domain` 列を持つので、`v_all_attempts` で `fact_attempt` とそのまま UNION される。

## dev/prod
`app.yaml` の `QUIZ_CATALOG` を `quiz_prod` にすれば本番カタログを見るアプリになる（dev/prod で別アプリにできる）。

## つまずきポイント
- **「SQLウェアハウスが未接続」**：手順3の Resource 割り当てと env 名 `DATABRICKS_WAREHOUSE_ID` を確認。
- **読み込み/保存に失敗**：手順4の GRANT（SP に SELECT / MODIFY）を確認。
- **アプリが停止**：24時間で自動停止。App 画面から再起動。

## ファイル
```
quiz-analytics-lab/
├── app/
│   ├── app.py            出題アプリ本体（Streamlit）
│   ├── app.yaml          起動設定・環境変数
│   └── requirements.txt  依存
├── src/06_load_app_tables.py  questions_app / app_attempts 作成＋GRANT
└── seed/questions_full.csv    問題全文（45問）
```
