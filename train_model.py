import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler

df = pd.read_csv('processed_weather.csv')
print(f'Training on {len(df)} rows')

FEATURES = ['hour_of_day', 'day_of_week', 'temp_lag_1h',
            'temp_rolling_avg', 'windspeed']
TARGET = 'temperature'

X = df[FEATURES]
y = df[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

mlflow.set_experiment('weather-forecast')

with mlflow.start_run(run_name='RandomForest_v2'):
    N_ESTIMATORS = 200
    MAX_DEPTH = 15

    model = RandomForestRegressor(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        random_state=42
    )
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    mlflow.log_param('n_estimators', N_ESTIMATORS)
    mlflow.log_param('max_depth', MAX_DEPTH)
    mlflow.log_metric('mae', mae)
    mlflow.log_metric('r2', r2)
    mlflow.sklearn.log_model(model, 'model')

    print(f'MAE: {mae:.2f} degrees C')
    print(f'R2 score: {r2:.3f}')
    print('Model logged to MLflow at http://localhost:5000')