"""
Databricks App（Streamlit）— 模擬題 出題アプリ
- 問題は Unity Catalog の questions_app テーブルから読む
- 解答は app_attempts テーブルへ INSERT（SQLウェアハウス経由）
  → 既存の分析システム（Genie/集計）がそのまま分析できる

試験領域: Platform（Apps / SQLウェアハウス）, Governance（App SP への GRANT）
"""
import os, json, uuid, random
import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config

CATALOG = os.getenv("QUIZ_CATALOG", "quiz_dev")
SCHEMA  = os.getenv("QUIZ_SCHEMA", "quiz")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")   # SQLウェアハウスをApp resourceとして接続すると注入される
Q_TABLE = f"{CATALOG}.{SCHEMA}.questions_app"
A_TABLE = f"{CATALOG}.{SCHEMA}.app_attempts"
N_QUESTIONS = 10

st.set_page_config(page_title="DEA 模擬題", page_icon="📝", layout="centered")


# ---- 接続（キャッシュ） ----
@st.cache_resource(ttl=600)
def get_conn():
    cfg = Config()  # App の OAuth 認証を自動で読む
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=600)
def load_questions():
    with get_conn().cursor() as cur:
        cur.execute(f"SELECT question_id, domain, question, options_json, answer, explanation, concept FROM {Q_TABLE}")
        df = cur.fetchall_arrow().to_pandas()
    df["options"] = df["options_json"].apply(json.loads)
    return df


def viewer_email():
    try:
        h = st.context.headers
        return h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username") or "app_user"
    except Exception:
        return "app_user"


def write_attempts(rows):
    """rows: list of dict(question_id, domain, is_correct)"""
    ins = (f"INSERT INTO {A_TABLE} "
           f"(attempt_id, principal, session_id, mode, question_id, domain, is_correct, answered_at) "
           f"VALUES (:aid, :prin, :sid, :mode, :qid, :dom, :ok, current_timestamp())")
    prin = viewer_email()
    with get_conn().cursor() as cur:
        for r in rows:
            cur.execute(ins, {
                "aid": str(uuid.uuid4()), "prin": prin, "sid": st.session_state.sid,
                "mode": "app", "qid": int(r["question_id"]), "dom": r.get("domain"),
                "ok": int(r["is_correct"]),
            })


# ---- 状態 ----
def start_quiz(df):
    picks = df.sample(min(N_QUESTIONS, len(df))).to_dict("records")
    st.session_state.qs = picks
    st.session_state.idx = 0
    st.session_state.answers = [None] * len(picks)
    st.session_state.sid = "app-" + uuid.uuid4().hex[:12]
    st.session_state.phase = "quiz"
    st.session_state.saved = False


st.title("📝 Databricks DEA 模擬題")
st.caption(f"出題元: {Q_TABLE} ／ 解答保存先: {A_TABLE}")

if not WAREHOUSE_ID:
    st.error("SQLウェアハウスが未接続です。App の設定で SQL warehouse リソースを追加し、環境変数 DATABRICKS_WAREHOUSE_ID を割り当ててください。")
    st.stop()

try:
    df = load_questions()
except Exception as e:
    st.error(f"問題の読み込みに失敗しました。テーブル {Q_TABLE} と App SP への SELECT 権限を確認してください。\n\n{e}")
    st.stop()

if "phase" not in st.session_state:
    st.session_state.phase = "home"

# ---- ホーム ----
if st.session_state.phase == "home":
    st.write(f"全 {len(df)} 問から {N_QUESTIONS} 問を出題します。解答は Delta テーブルに記録され、分析システムで集計されます。")
    if st.button("▶ 開始する", type="primary"):
        start_quiz(df); st.rerun()

# ---- 出題 ----
elif st.session_state.phase == "quiz":
    i = st.session_state.idx
    qs = st.session_state.qs
    q = qs[i]
    st.progress((i) / len(qs), text=f"第 {i+1} / {len(qs)} 問")
    st.markdown(f"**[{q['domain']}] #{q['question_id']}**")
    st.markdown(q["question"])
    labels = [f"{chr(65+j)}. {opt}" for j, opt in enumerate(q["options"])]
    choice = st.radio("選択肢", labels, index=None, key=f"radio_{i}")

    answered = st.session_state.answers[i] is not None
    if not answered:
        if st.button("解答する", disabled=(choice is None)):
            st.session_state.answers[i] = labels.index(choice)
            st.rerun()
    else:
        sel = st.session_state.answers[i]
        ok = sel == q["answer"]
        st.success(f"◯ 正解（{chr(65+q['answer'])}）") if ok else st.error(f"✕ 不正解（正解：{chr(65+q['answer'])}）")
        st.markdown(f"**解説**：{q['explanation']}")
        if q.get("concept"):
            st.info(f"📘 用語・ポイント：{q['concept']}")
        if i < len(qs) - 1:
            if st.button("次へ ▶", type="primary"):
                st.session_state.idx += 1; st.rerun()
        else:
            if st.button("結果を見る ▶", type="primary"):
                st.session_state.phase = "result"; st.rerun()

# ---- 結果 ----
elif st.session_state.phase == "result":
    qs = st.session_state.qs
    ans = st.session_state.answers
    rows = [{"question_id": qs[i]["question_id"], "domain": qs[i]["domain"], "is_correct": 1 if ans[i] == qs[i]["answer"] else 0} for i in range(len(qs))]
    correct = sum(r["is_correct"] for r in rows)
    total = len(rows)
    pct = round(100 * correct / total)

    # Delta への保存（1回だけ）
    if not st.session_state.get("saved"):
        try:
            write_attempts(rows)
            st.session_state.saved = True
        except Exception as e:
            st.warning(f"解答の保存に失敗しました（App SP に {A_TABLE} への MODIFY 権限が必要）：{e}")

    st.metric("スコア", f"{pct}%", f"{correct} / {total} 問")
    if st.session_state.get("saved"):
        st.caption("✅ 解答を app_attempts に保存しました（分析システムで集計できます）")

    # ドメイン別
    dom = {}
    for i, r in enumerate(rows):
        d = qs[i]["domain"]; dom.setdefault(d, [0, 0]); dom[d][1] += 1; dom[d][0] += r["is_correct"]
    st.subheader("ドメイン別")
    st.dataframe(pd.DataFrame([{"domain": k, "correct": v[0], "total": v[1]} for k, v in dom.items()]), hide_index=True)

    if st.button("もう一度"):
        start_quiz(load_questions()); st.rerun()
