import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

FORECAST_DAYS = 30
MA_WINDOW = 6


def preprocess_history(history_data):
    df = pd.DataFrame(history_data)
    if df.empty:
        return pd.DataFrame(), None, None
    df["record_date"] = pd.to_datetime(df["record_date"])
    df = df.sort_values("record_date").reset_index(drop=True)
    df["days"] = (df["record_date"] - df["record_date"].min()).dt.days
    return df, df["record_date"].min(), df["record_date"].max()


def get_land_price_analytics(history_data):
    df, min_date, max_date = preprocess_history(history_data)
    analytics = {
        "forecasted_price": None,
        "regression_line": [],
        "moving_average": [],
        "data_sufficient": False,
    }
    if len(df) < 5:
        analytics["forecasted_price"] = int(df["price_inr"].iloc[-1]) if not df.empty else None
        return analytics
    analytics["data_sufficient"] = True
    X = df[["days"]]
    y = df["price_inr"]
    model = LinearRegression()
    model.fit(X, y)
    future_day = df["days"].max() + FORECAST_DAYS
    forecast_price_lr = model.predict([[future_day]])[0]
    analytics["forecasted_price"] = int(forecast_price_lr)
    df["reg_price"] = model.predict(X)
    analytics["regression_line"] = [{"date": row["record_date"], "price": int(row["reg_price"])} for index, row in df.iterrows()]
    df["ma_price"] = df["price_inr"].rolling(window=MA_WINDOW, min_periods=1).mean()
    analytics["moving_average"] = [{"date": row["record_date"], "price": int(row["ma_price"])} for index, row in df.dropna(subset=["ma_price"]).iterrows()]
    return analytics


def calculate_fair_option_premium(predicted_price, strike_price):
    if predicted_price is None or strike_price is None:
        return 0
    intrinsic_value = predicted_price - strike_price
    return int(intrinsic_value * 0.2) if intrinsic_value > 0 else 0
