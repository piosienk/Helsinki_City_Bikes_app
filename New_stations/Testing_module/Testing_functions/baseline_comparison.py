import pandas as pd
from math import sqrt
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


def test_mean_median(df, data_type="weekend"):
    if data_type == "weekend":
        path_mean = "./Data/Statistics/baseline_mean_results_weekend.csv"
        path_median = "./Data/Statistics/baseline_median_results_weekend.csv"

    else:
        path_mean = "./Data/Statistics/baseline_mean_results_working.csv"
        path_median = "./Data/Statistics/baseline_median_results_working.csv"

    y_true = df.loc[:, "number"]
    y_mean = y_true.mean()
    y_mean = [y_mean for i in range(len(y_true))]
    y_median = y_true.median()
    y_median = [y_median for i in range(len(y_true))]

    mean_rmse = sqrt(mean_squared_error(y_true, y_mean))
    mean_mae = mean_absolute_error(y_true, y_mean)
    mean_r2 = r2_score(y_true, y_mean)

    median_rmse = sqrt(mean_squared_error(y_true, y_median))
    median_mae = mean_absolute_error(y_true, y_median)
    median_r2 = r2_score(y_true, y_median)

    df_mean_results = pd.DataFrame({'mae': mean_mae, "rmse": mean_rmse, "r2": mean_r2}, index=["mean"])
    df_median_results = pd.DataFrame({'mae': median_mae, "rmse": median_rmse, "r2": median_r2}, index=["median"])

    df_mean_results.to_csv(path_mean)
    df_median_results.to_csv(path_median)
    return df_mean_results, df_median_results


def get_stats(df, data_type):
    path = "./Data/Statistics/stations_distribution" + data_type + ".csv"

    df.number.describe(percentiles=[0.25, 0.5, 0.75, 0.90, 0.95, 0.99]).to_csv(path)

    return df.number.describe(percentiles=[0.25, 0.5, 0.75, 0.90, 0.95, 0.99])
