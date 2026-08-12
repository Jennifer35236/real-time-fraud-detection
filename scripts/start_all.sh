#!/bin/bash

# === CONFIG ===
PROJECT_DIR=~/real_time_fraud_detection_project
VENV_ACTIVATE="$PROJECT_DIR/.venv/bin/activate"

# === 1) Start Kafka/Zookeeper ===
osascript <<END
tell application "Terminal"
    do script "cd $PROJECT_DIR && docker compose up -d && docker compose ps"
end tell
END

# Wait for Kafka to fully start
sleep 12

# === 2) Start Consumer FIRST ===
osascript <<END
tell application "Terminal"
    do script "cd $PROJECT_DIR && source $VENV_ACTIVATE && python3 kafka_consumer.py"
end tell
END

# Give consumer a moment to connect
sleep 4

# === 3) Start Producer ===
osascript <<END
tell application "Terminal"
    do script "cd $PROJECT_DIR && source $VENV_ACTIVATE && python3 kafka_producer.py"
end tell
END

# Give producer time to send first batch
sleep 4

# === 4) Start Streamlit Dashboard ===
osascript <<END
tell application "Terminal"
    do script "cd $PROJECT_DIR && source $VENV_ACTIVATE && streamlit run app.py"
end tell
END
