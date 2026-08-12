# Implementation notes

This public repository is based on recovered source files from the MSc Research Practicum project **Hybrid Real-Time Fraud Detection in Finance**.

The recovered executable path included here uses:

1. A batch-trained `RandomForestClassifier` with `StandardScaler`.
2. Kafka for simulated transaction streaming.
3. ADWIN for drift monitoring over prediction correctness.
4. Streamlit for live operational monitoring.

A separate recovered development consumer used River's `HoeffdingTreeClassifier` with `learn_one()` for online updates. It is not used as the primary consumer in this cleaned public repository so that the repo has one clear runnable path.

The academic report discusses Adaptive Random Forest as the research model. This repository does not rename the recovered Random Forest implementation as Adaptive Random Forest; that distinction is kept explicit.
