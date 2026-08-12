# app.py — Real-Time Fraud Detection Dashboard
# Streamlit + Altair; robust CSV parsing; ISO8601 timestamps; AUPRC-first metrics.

import os
import time
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt

# Prefer average_precision_score (AUPRC) but fall back gracefully
try:
    from sklearn.metrics import average_precision_score, precision_recall_curve, auc, precision_score, recall_score
    HAVE_SK = True
except Exception:
    HAVE_SK = False

# ---------- Config ----------
RESULTS_CSV = "stream_results.csv"   # tx_id,pred,label,p_fraud,ts
ALERTS_CSV  = "kafka_alerts.csv"     # tx_id,score,amount,time
DRIFT_CSV   = "kafka_drift_log.csv"  # DriftAt

st.set_page_config(
    page_title="Real-Time Fraud Detection — Live Ops",
    layout="wide",
    page_icon="🧯",
)

# ---------- Helpers ----------
@st.cache_data(show_spinner=False)
def safe_read_csv(path: str, nrows: int | None = None) -> pd.DataFrame:
    """Read a CSV that is being appended to, skipping malformed lines and
    forcing the exact 5 columns we expect for stream_results.csv.
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame(columns=["tx_id", "pred", "label", "p_fraud", "ts"])

    try:
        df = pd.read_csv(
            path,
            header=None,
            names=["tx_id", "pred", "label", "p_fraud", "ts"],
            on_bad_lines="skip",
            engine="python",          # more forgiving for trailing writes
            nrows=nrows,
        )
    except Exception:
        # Last-chance: read with minimal parsing then coerce
        raw = open(path, "r", errors="ignore").read().strip().splitlines()
        rows = []
        for line in raw[: nrows if nrows else None]:
            parts = line.split(",")
            if len(parts) >= 5:
                rows.append(parts[:5])
        df = pd.DataFrame(rows, columns=["tx_id", "pred", "label", "p_fraud", "ts"])

    # Coerce types safely
    df["tx_id"]   = pd.to_numeric(df["tx_id"], errors="coerce").astype("Int64")
    df["pred"]    = pd.to_numeric(df["pred"], errors="coerce")
    df["label"]   = pd.to_numeric(df["label"], errors="coerce")
    df["p_fraud"] = pd.to_numeric(df["p_fraud"], errors="coerce")

    # Timestamps: accept ISO8601 in mixed forms; keep tz-aware UTC
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)

    # Drop rows that failed to parse
    df = df.dropna(subset=["tx_id", "pred", "label", "p_fraud", "ts"])
    # Keep only the most recent by timestamp if duplicates slipped in
    df = df.sort_values("ts").reset_index(drop=True)
    return df


def auprc_score(y_true: np.ndarray, y_score: np.ndarray) -> float:
    if len(y_true) == 0:
        return float("nan")
    if HAVE_SK:
        try:
            return float(average_precision_score(y_true, y_score))
        except Exception:
            # Fallback via PR curve → AUC
            p, r, _ = precision_recall_curve(y_true, y_score)
            return float(auc(r, p))
    # Minimal fallback if sklearn not present
    # (precision at top-K where K = sum(y_true); not exact AUPRC, but indicative)
    k = int(np.nansum(y_true))
    if k <= 0:
        return float("nan")
    order = np.argsort(-y_score)
    topk = y_true[order][:k]
    prec = np.mean(topk) if len(topk) else float("nan")
    return float(prec)


def compute_metrics(df_window: pd.DataFrame, roll_window: int) -> dict:
    y_true = df_window["label"].astype(int).to_numpy()
    y_pred = df_window["pred"].astype(int).to_numpy()
    y_score = df_window["p_fraud"].to_numpy()

    # classification counts
    frauds_actual = int(np.nansum(y_true))
    frauds_pred   = int(np.nansum(y_pred))

    # precision/recall
    if HAVE_SK and len(y_true):
        try:
            precision = float(precision_score(y_true, y_pred, zero_division=0))
            recall    = float(recall_score(y_true, y_pred, zero_division=0))
        except Exception:
            precision = float("nan")
            recall = float("nan")
    else:
        # simple manual calc
        tp = int(np.sum((y_true == 1) & (y_pred == 1)))
        fp = int(np.sum((y_true == 0) & (y_pred == 1)))
        fn = int(np.sum((y_true == 1) & (y_pred == 0)))
        precision = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
        recall    = tp / (tp + fn) if (tp + fn) > 0 else float("nan")

    # AUPRC (window)
    auprc = auprc_score(y_true, y_score)

    # Rolling accuracy over last roll_window
    if len(df_window) >= roll_window:
        tail = df_window.tail(roll_window)
        ra = float(np.mean((tail["pred"].astype(int) == tail["label"].astype(int)).to_numpy()))
    else:
        ra = float(np.mean((df_window["pred"].astype(int) == df_window["label"].astype(int)).to_numpy())) if len(df_window) else float("nan")

    return {
        "rows": len(df_window),
        "frauds_actual": frauds_actual,
        "frauds_pred": frauds_pred,
        "precision": precision,
        "recall": recall,
        "auprc": auprc,
        "roll_acc": ra,
    }


def make_dual_axis_chart(df: pd.DataFrame, roll_window: int) -> alt.Chart:
    # Compute rolling acc (centered on each ts) and instantaneous fraud rate
    df = df.copy()
    eq = (df["pred"].astype(int) == df["label"].astype(int)).astype(float)
    df["roll_acc"] = eq.rolling(roll_window, min_periods=1).mean()

    # Fraud rate over a small window (use same roll_window for simplicity)
    fr = (df["label"] == 1).astype(float)
    df["fraud_rate"] = fr.rolling(roll_window, min_periods=1).mean()

    # Charts
    acc_line = alt.Chart(df).mark_line(color="steelblue").encode(
        x=alt.X("ts:T", title="time"),
        y=alt.Y("roll_acc:Q", title=f"Rolling accuracy (window={roll_window})", scale=alt.Scale(domain=[0, 1])),
        tooltip=[alt.Tooltip("ts:T"), alt.Tooltip("roll_acc:Q", format=".3f")],
    )

    max_fr = float(df["fraud_rate"].max()) if len(df) else 0.0
    max_fr = max(0.01, max_fr * 1.1)  # keep visible even when tiny

    fraud_rate_line = alt.Chart(df).mark_line(color="orange").encode(
        x="ts:T",
        y=alt.Y("fraud_rate:Q", title="Fraud Rate", scale=alt.Scale(domain=[0, max_fr])),
        tooltip=[alt.Tooltip("ts:T"), alt.Tooltip("fraud_rate:Q", format=".4f")],
    )

    return alt.layer(acc_line, fraud_rate_line).resolve_scale(y="independent")


# ---------- UI ----------
st.sidebar.header("Controls")

window_n = st.sidebar.slider("Window size (last N rows)", min_value=1_000, max_value=50_000, value=10_000, step=500)
roll_window = st.sidebar.slider("Rolling window for accuracy", min_value=50, max_value=5_000, value=1_000, step=50)

auto = st.sidebar.checkbox("Auto-refresh", value=True)
refresh_s = st.sidebar.slider("Refresh every (seconds)", 1, 30, 5)
st.sidebar.caption("Tip: **AUPRC** is more meaningful than accuracy on highly imbalanced data.")

st.title("Real-Time Fraud Detection Dashboard")

# Read latest data (only last window_n for speed)
df_all = safe_read_csv(RESULTS_CSV, nrows=None)
df_all = df_all.tail(window_n).reset_index(drop=True)

# Empty-state handling
if df_all.empty:
    st.info("Waiting for data… Keep the producer & consumer running so CSVs update in real time.")
    if auto:
        time.sleep(refresh_s)
        st.rerun()
    st.stop()

# KPI metrics
m = compute_metrics(df_all, roll_window)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Rows in window", f"{m['rows']:,}")
k2.metric("Frauds (actual 1s)", f"{m['frauds_actual']:,}")
k3.metric("Predicted frauds", f"{m['frauds_pred']:,}")
k4.metric("AUPRC (window)", f"{m['auprc']:.3f}" if np.isfinite(m["auprc"]) else "—")
k5.metric("Precision", f"{m['precision']:.3f}" if np.isfinite(m["precision"]) else "—")

k6, k7 = st.columns(2)
k6.metric("Recall", f"{m['recall']:.3f}" if np.isfinite(m["recall"]) else "—")
k7.metric("Rolling accuracy", f"{m['roll_acc']:.3f}" if np.isfinite(m["roll_acc"]) else "—")

# Charts
left, right = st.columns(2)

with left:
    st.subheader("Fraud score (p_fraud) over time")
    pmax = float(df_all["p_fraud"].max()) if len(df_all) else 1.0
    pf_chart = (
        alt.Chart(df_all)
        .mark_line()
        .encode(
            x=alt.X("ts:T", title="time"),
            y=alt.Y("p_fraud:Q", title="p_fraud", scale=alt.Scale(domain=[0, max(1.0, pmax)])),
            tooltip=[alt.Tooltip("ts:T"), alt.Tooltip("p_fraud:Q", format=".3f")],
        )
        .properties(height=320)
    )
    st.altair_chart(pf_chart, use_container_width=True)

with right:
    st.subheader(f"Rolling accuracy (window={roll_window}) & Fraud rate")
    combo = make_dual_axis_chart(df_all, roll_window).properties(height=320)
    st.altair_chart(combo, use_container_width=True)

# Tail tables
st.markdown("---")
t1, t2 = st.columns(2)
with t1:
    st.caption("Live stream results (tail)")
    st.dataframe(df_all.tail(20), use_container_width=True, height=320)
with t2:
    if os.path.exists(ALERTS_CSV) and os.path.getsize(ALERTS_CSV) > 0:
        alerts = pd.read_csv(
            ALERTS_CSV,
            header=None,
            names=["tx_id", "score", "amount", "time"],
            on_bad_lines="skip",
            engine="python",
        )
        # Try to parse time; tolerate numeric "Time" from the dataset
        alerts["time"] = pd.to_datetime(alerts["time"], errors="coerce", utc=True)
        st.caption("Fraud alerts (tail)")
        st.dataframe(alerts.tail(20), use_container_width=True, height=320)
    else:
        st.caption("Fraud alerts (tail)")
        st.info("No alerts yet.")

# Drift log (optional)
if os.path.exists(DRIFT_CSV) and os.path.getsize(DRIFT_CSV) > 0:
    drift = pd.read_csv(DRIFT_CSV, header=None, names=["DriftAt"], on_bad_lines="skip", engine="python")
    drift["DriftAt"] = pd.to_datetime(drift["DriftAt"], errors="coerce", utc=True)
    drift = drift.dropna().tail(10)
    if not drift.empty:
        st.markdown("### Drift events")
        st.write(drift)
else:
    st.caption("No drifts detected.")

# Auto-refresh without deprecated experimental APIs
if auto:
    # Cache bust: clear only this dataset’s cache so fresh reads occur next run
    safe_read_csv.clear()
    time.sleep(refresh_s)
    st.rerun()