import pandas as pd, joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

CSV = "creditcard.csv"
FEATURES = ['Time'] + [f'V{i}' for i in range(1,29)] + ['Amount']
LABEL = 'Class'

df = pd.read_csv(CSV)
X = df[FEATURES]
y = df[LABEL]

scaler = StandardScaler().fit(X)
X_scaled = scaler.transform(X)

rf = RandomForestClassifier(
    n_estimators=200, random_state=42, class_weight='balanced_subsample', n_jobs=-1
).fit(X_scaled, y)

joblib.dump(scaler, 'scaler.joblib')
joblib.dump(rf, 'rf_model.joblib')
print("✅ Saved scaler.joblib and rf_model.joblib")
