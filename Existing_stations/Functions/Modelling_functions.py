from pycaret.regression import *
from sklearn.metrics import mean_absolute_error, mean_squared_error
from Existing_stations.Functions.TimeSeriesSplit_functions import TimeSeriesSplit
from Existing_stations.Functions.Metrics_functions import mean_absolute_percentage_error, mean_absolute_scaled_error


def train_models_and_save_predictions(path_to_data=
                                      '../Existing_stations/Data/Time_series/time_series_data_transformed.csv',
                                      path_to_stats='../Existing_stations/Data/Results/stats.csv',
                                      path_to_predictions='../Existing_stations/Data/Results/predictions.csv'):
    """
    :param path_to_predictions:
    :param path_to_stats:
    :param path_to_data:
    :return:
    """
    results_all = pd.DataFrame()
    df_predictions_all = pd.DataFrame()
    dt_all = pd.read_csv(path_to_data, index_col=0)
    for station in dt_all['departure'].unique():
        print(station)
        dt = dt_all.loc[dt_all.departure == station, :].reset_index(drop=True)
        dt = dt.loc[~dt.cal_month.isin([1, 2, 3, 11, 12]), :]
        dt.number = dt.number + 0.1
        train = dt[:-84]
        test = dt[-84::]
        tscv = TimeSeriesSplit(n_splits=5, test_size=84)
        s = setup(data=train,
                  test_data=test,
                  target='number',
                  fold_strategy=tscv,
                  data_split_shuffle=False,
                  normalize=True,
                  ignore_features=['departure', 'date', 'cal_year', 'Snow Depth'],
                  categorical_features=['cal_month', 'cal_day', 'cal_hour',
                                        'holiday', 'Uudenvuodenpäivä', 'Loppiainen',
                                        'Pitkäperjantai', 'Pääsiäispäivä', '2. pääsiäispäivä',
                                        'Vappu',
                                        'Helatorstai', 'Helluntaipäivä', 'Juhannusaatto',
                                        'Juhannuspäivä',
                                        'Pyhäinpäivä', 'Itsenäisyyspäivä', 'Jouluaatto',
                                        'Joulupäivä',
                                        'Tapaninpäivä'],
                  numeric_features=['Temperature', 'Cloud Cover', 'Wind Gust', 'Precipitation', 'Sea Level Pressure',
                                    'Relative Humidity', 'Wind Speed'
                                    ],
                  silent=True,
                  verbose=False,
                  session_id=1,
                  preprocess=True, html=False)
        best = compare_models(sort='MAE', verbose=True, include=['lightgbm', 'gbr', 'br', 'ridge', 'omp'])
        final_best = finalize_model(best)
        predictions = predict_model(final_best, data=test)
        predictions.loc[predictions.Label < 0, 'Label'] = 0
        predictions.Label = predictions.Label.astype('int')
        results = pd.DataFrame({'station': [station],
                                'model MAE': [mean_absolute_error(predictions.number, predictions.Label)],
                                'baseline mean MAE': [mean_absolute_error(predictions.number,
                                                                          np.repeat(train.number.mean(),
                                                                                    len(predictions.number)))],
                                'baseline median MAE': [mean_absolute_error(predictions.number,
                                                                            np.repeat(train.number.median(),
                                                                                      len(predictions.number)))],
                                'model MSE': [mean_squared_error(predictions.number, predictions.Label)],
                                'baseline mean MSE': [mean_squared_error(predictions.number,
                                                                         np.repeat(train.number.mean(),
                                                                                   len(predictions.number)))],
                                'baseline median MSE': [mean_squared_error(predictions.number,
                                                                           np.repeat(train.number.median(),
                                                                                     len(predictions.number)))],
                                'model MAPE': [mean_absolute_percentage_error(predictions.number, predictions.Label)],
                                'baseline mean MAPE': [mean_absolute_percentage_error(predictions.number,np.repeat(train.number.mean(), len(predictions.number)))],
                                'baseline median MAPE': [mean_absolute_percentage_error(predictions.number, np.repeat(train.number.median(), len(predictions.number)))],
                                'model MASE': [mean_absolute_scaled_error(train, predictions.number, predictions.Label)],
                                'baseline mean MASE': [mean_absolute_scaled_error(train, predictions.number, np.repeat(train.number.mean(), len(predictions.number)))],
                                'baseline median MASE': [mean_absolute_scaled_error(train, predictions.number, np.repeat(train.number.median(), len(predictions.number)))],
                                'model type': [pull().iloc[0:1].Model[0]]})
        results_all = pd.concat([results, results_all]).reset_index(drop=True)
        results_all.to_csv(path_to_stats)
        all_preds = predict_model(final_best, data=dt)
        df = dt_all.loc[dt_all.departure == station, :].reset_index(drop=True)
        df = df.merge(all_preds, how = 'left', on = ['date'])
        df.loc[df.Label.isna(),'Label'] = 0
        df = df[['cal_year_x', 'cal_month_x', 'cal_day_x', "number_x", "Label"]]
        df['date'] = pd.to_datetime(
            df['cal_year_x'].astype(str) + df['cal_month_x'].astype(str) + df['cal_day_x'].astype(str),
            format='%Y%m%d')
        df = df[['date', 'number_x', 'Label']]
        df = df.groupby(['date']).agg(['mean']).reset_index()
        df.columns = ['date', 'Value', 'Prediction']
        df.loc[:, 'station'] = station
        df_predictions_all = pd.concat([df_predictions_all, df]).reset_index(drop=True)
        df_predictions_all.to_csv(path_to_predictions)
        path_to_model = '../Existing_stations/Data/Models/model_for_' + station.replace("/", "-")
        save_model(final_best, path_to_model)
    return
