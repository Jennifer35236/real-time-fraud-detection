import pandas as pd
from kafka import KafkaProducer
import json, time

# ---------------- CONFIG ----------------
CSV        = "creditcard.csv"          # path to your dataset
TOPIC      = "transaction_topic"
BOOTSTRAP  = "localhost:9092"
SLEEP_SEC  = 0.01                      # 10ms between messages to simulate streaming

# Columns your consumer expects
FEATURES = ['Time'] + [f'V{i}' for i in range(1, 29)] + ['Amount']
LABEL    = 'Class'

# ---------------- LOADING DATA ----------------
df = pd.read_csv(CSV)

# Validate columns
missing = [c for c in FEATURES + [LABEL] if c not in df.columns]
if missing:
    raise ValueError(f"Missing columns in {CSV}: {missing}")

# Ensure numeric types (prevents JSON serialization issues)
for c in FEATURES + [LABEL]:
    df[c] = pd.to_numeric(df[c], errors='coerce')

# ---------------- KAFKA PRODUCER ----------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=5
)

print("🚀 Producer connected. Streaming transactions…")

# Stream rows in original (chronological) order
for idx, row in df.iterrows():
    # Build message
    msg = {
        'tx_id': int(idx),
        'send_ts': time.time(),  # used by consumer for end-to-end latency
    }

    # Features + label
    for c in FEATURES:
        # cast to float (JSON-friendly)
        val = row[c]
        msg[c] = None if pd.isna(val) else float(val)

    # label as int
    y = row[LABEL]
    msg[LABEL] = 0 if pd.isna(y) else int(y)

    # Send
    producer.send(TOPIC, value=msg)

    # Log every 500 rows
    if idx % 500 == 0:
        print(f"→ sent row {idx}")

    time.sleep(SLEEP_SEC)

producer.flush()
print("✅ Producer finished streaming.")
