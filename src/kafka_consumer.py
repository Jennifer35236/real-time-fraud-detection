from kafka import KafkaConsumer, KafkaProducer
import json, joblib, os, csv
import pandas as pd
from collections import deque
from datetime import datetime, timezone

from sklearn.metrics import precision_recall_fscore_support, average_precision_score
from river.drift import ADWIN

# ---------- Config ----------
TOPIC_IN   = "transaction_topic"
TOPIC_OUT  = "alerts"
BOOTSTRAP  = "localhost:9092"

FEATURES = ['Time'] + [f'V{i}' for i in range(1, 29)] + ['Amount']
LABEL    = 'Class'

THRESHOLD      = 0.20         # decision threshold for labeling as fraud
WINDOW_SIZE    = 1000         # rolling window for metrics (recommended by dataset note)
METRIC_EVERY   = 200          # compute and log metrics every N events
# ----------------------------

# Load model + scaler
model  = joblib.load('rf_model.joblib')   # must support predict_proba
scaler = joblib.load('scaler.joblib')

# Kafka
consumer = KafkaConsumer(
    TOPIC_IN,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
alert_producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Files
ALERT_CSV    = 'kafka_alerts.csv'
DRIFT_CSV    = 'kafka_drift_log.csv'
RESULTS_CSV  = 'stream_results.csv'   # per-transaction log
METRICS_CSV  = 'metrics_window.csv'   # rolling metrics for dashboard

def ensure_file(path, header):
    if not os.path.exists(path):
        with open(path, 'w', newline='') as f:
            csv.writer(f).writerow(header)

ensure_file(ALERT_CSV,   ['tx_id','score','amount','time','ts'])
ensure_file(DRIFT_CSV,   ['DriftAt'])
ensure_file(RESULTS_CSV, ['tx_id','pred','label','p_fraud','ts'])
ensure_file(METRICS_CSV, ['ts','count','fraud_share_window','precision','recall','f1','auprc','threshold'])

# Drift detector runs on correctness stream
adwin = ADWIN()

# Rolling window storage
y_true_win  = deque(maxlen=WINDOW_SIZE)
p_fraud_win = deque(maxlen=WINDOW_SIZE)
pred_win    = deque(maxlen=WINDOW_SIZE)

print("✅ Consumer started. Waiting for messages…")
count = 0
correct = 0

for msg in consumer:
    tx = msg.value
    tx_id  = int(tx.get('tx_id', -1))
    y_true = int(tx.get(LABEL, 0))

    # Build row for model
    row = pd.DataFrame([[tx.get(f) for f in FEATURES]], columns=FEATURES)
    Xs  = scaler.transform(row)

    # Probability of fraud
    p_fraud = float(model.predict_proba(Xs)[0, 1])
    pred    = int(p_fraud >= THRESHOLD)

    # Global stats
    count += 1
    correct += int(pred == y_true)

    # Update drift on correctness
    adwin.update(int(pred == y_true))
    if adwin.drift_detected:
        with open(DRIFT_CSV, 'a', newline='') as f:
            csv.writer(f).writerow([datetime.now(timezone.utc).isoformat(timespec='seconds')])
        print("📉 Drift detected by ADWIN")

    # Emit alert & log to CSV if predicted fraud
    if pred == 1:
        alert = {
            'tx_id': tx_id,
            'score': p_fraud,
            'amount': tx.get('Amount'),
            'time': tx.get('Time'),
            'ts': datetime.now(timezone.utc).isoformat(timespec='seconds')
        }
        alert_producer.send(TOPIC_OUT, value=alert)
        with open(ALERT_CSV, 'a', newline='') as f:
            csv.writer(f).writerow([alert['tx_id'], alert['score'], alert['amount'], alert['time'], alert['ts']])

    # Per-transaction log for dashboard
    with open(RESULTS_CSV, 'a', newline='') as f:
        csv.writer(f).writerow([
            tx_id,
            pred,
            y_true,
            p_fraud,
            datetime.now(timezone.utc).isoformat(timespec='seconds')
        ])

    # Update rolling window
    y_true_win.append(y_true)
    p_fraud_win.append(p_fraud)
    pred_win.append(pred)

    # Periodically compute and log window metrics (AUPRC, P/R/F1)
    if (count % METRIC_EVERY == 0) and (len(y_true_win) >= 50):
        y_list = list(y_true_win)
        p_list = list(p_fraud_win)
        pred_list = list(pred_win)

        # Precision, Recall, F1 for positive class
        prec, rec, f1, _ = precision_recall_fscore_support(
            y_list, pred_list, average='binary', zero_division=0
        )
        # AUPRC needs probabilities
        try:
            auprc = float(average_precision_score(y_list, p_list))
        except Exception:
            auprc = 0.0

        fraud_share = sum(y_list) / len(y_list)

        with open(METRICS_CSV, 'a', newline='') as f:
            csv.writer(f).writerow([
                datetime.now(timezone.utc).isoformat(timespec='seconds'),
                count,
                f"{fraud_share:.6f}",
                f"{prec:.6f}",
                f"{rec:.6f}",
                f"{f1:.6f}",
                f"{auprc:.6f}",
                f"{THRESHOLD:.2f}"
            ])

    # Console heartbeat
    if tx_id % 100 == 0:
        acc = correct / count if count else 0.0
        print(f"tx={tx_id}  p={p_fraud:.4f}  pred={pred}  y={y_true}  acc={acc:.4f}")
