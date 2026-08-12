# Runtime outputs

The pipeline writes operational CSV files during execution.

Recovered examples included in this portfolio package:

- `kafka_alerts.csv`
- `kafka_drift_log.csv`
- `kafka_perf.csv`

Other generated files referenced by the consumer/dashboard include:

- `stream_results.csv`
- `metrics_window.csv`

These runtime files are excluded from future commits via `.gitignore`.
