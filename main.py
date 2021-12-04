# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from functions.functions import train_models_and_save_predictions
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from functions.functions import *
from functions.Historical_weather_load_transform import *
from functions.unit_tests import *

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    #df = pd.read_csv('./data/all_dt.csv')
    #print(np.max(df.departure))
    #historical_weather_load(end_date = np.max(df.departure))

    #df = pd.read_pickle('./data/hist_weather_dict.pickle')
    #historical_weather_transformations(df)

    #weather_dict = pd.read_pickle('./data/hist_mod_weather_dict.pickle')
    #metadata_dict = pd.read_pickle('./data/hist_metadata_dict.pickle')
    #create_weather_stats(weather_dict, metadata_dict)


    #test_hist_data(metadata_dict, weather_dict)

    predictions = pd.read_csv('./data/predictions.csv', index_col=0)
    df_plot = predictions.loc[predictions.station == 'Toinen linja', :]
    fig = px.line(df_plot, x='date', y=["Value", "Prediction"], template='plotly_dark')
    fig.show()
    df_forecast = forecast_weather_transformation(df=pd.read_pickle("./data/forecast_weather_dict.pickle"))
    path_to_model = './data/models/model_for_' + 'Toinen linja'
    mdl = load_model(path_to_model)
    preds = predict_model(mdl, data=df_forecast)
    preds.loc[preds.cal_month.isin([1, 2, 3, 11, 12]), 'Label'] = 0
    fig2 = px.line(preds, x='date', y=["Label"], template='plotly_dark')
    fig2.show()

    # path_to_data = './data/time_series_data_transformed.csv'
    # path_to_stats = './data/stats.csv'
    # path_to_predictions = './data/predictions.csv'
    #
    # results_all = pd.DataFrame()
    # df_predictions_all = pd.DataFrame()
    # dt_all = pd.read_csv(path_to_data, index_col=0)
    # for station in dt_all['departure'].unique():
    #     print(station)
    #     dt = dt_all.loc[dt_all.departure == station, :].reset_index(drop=True)
    #     dt = dt.loc[~dt.cal_month.isin([1, 2, 3, 11, 12]), :]
    #     dt.number = dt.number + 0.1
    #     for idx in range(0,5):
    #         train = dt[:dt.shape[0]-84*(idx+1)]
    #         test = dt[dt.shape[0]-84*(idx+1):dt.shape[0]-84*idx]
    #         print(train.shape)
    #         print(test.shape)
    #         tscv = TimeSeriesSplit(n_splits=5, test_size=84)
    #         s = setup(data=train,
    #               test_data=test,
    #               target='number',
    #               fold_strategy=tscv,
    #               data_split_shuffle=False,
    #               normalize=True,
    #               ignore_features=['departure', 'date', 'cal_year', 'Snow Depth'],
    #               categorical_features=['cal_month', 'cal_day', 'cal_hour',
    #                                     'holiday', 'Uudenvuodenpäivä', 'Loppiainen',
    #                                     'Pitkäperjantai', 'Pääsiäispäivä', '2. pääsiäispäivä',
    #                                     'Vappu',
    #                                     'Helatorstai', 'Helluntaipäivä', 'Juhannusaatto',
    #                                     'Juhannuspäivä',
    #                                     'Pyhäinpäivä', 'Itsenäisyyspäivä', 'Jouluaatto',
    #                                     'Joulupäivä',
    #                                     'Tapaninpäivä'],
    #               numeric_features=['Temperature', 'Cloud Cover', 'Wind Gust', 'Precipitation', 'Sea Level Pressure',
    #                                 'Relative Humidity', 'Wind Speed'
    #                                 ],
    #               silent=True,
    #               verbose=False,
    #               session_id=1,
    #               preprocess=True, html=False)
    #         best = compare_models(sort='MAE', verbose=True, include=['lightgbm', 'gbr', 'br', 'ridge', 'omp'])
    #         final_best = finalize_model(best)
    #         predictions = predict_model(final_best, data=test)
    #         predictions.loc[predictions.Label < 0, 'Label'] = 0
    #         predictions.Label = predictions.Label.astype('int')
    #         results = pd.DataFrame({'station': [station],
    #                                 'idx': [idx],
    #                             'model MAE': [mean_absolute_error(predictions.number, predictions.Label)],
    #                             'baseline mean MAE': [mean_absolute_error(predictions.number,
    #                                                                       np.repeat(train.number.mean(),
    #                                                                                 len(predictions.number)))],
    #                             'baseline median MAE': [mean_absolute_error(predictions.number,
    #                                                                         np.repeat(train.number.median(),
    #                                                                                   len(predictions.number)))],
    #                             'model MSE': [mean_squared_error(predictions.number, predictions.Label)],
    #                             'baseline mean MSE': [mean_squared_error(predictions.number,
    #                                                                      np.repeat(train.number.mean(),
    #                                                                                len(predictions.number)))],
    #                             'baseline median MSE': [mean_squared_error(predictions.number,
    #                                                                        np.repeat(train.number.median(),
    #                                                                                  len(predictions.number)))],
    #                             'model MAPE': [mean_absolute_percentage_error(predictions.number, predictions.Label)],
    #                             'baseline mean MAPE': [mean_absolute_percentage_error(predictions.number,
    #                                                                                   np.repeat(train.number.mean(),
    #                                                                                             len(predictions.number)))],
    #                             'baseline median MAPE': [mean_absolute_percentage_error(predictions.number,
    #                                                                                     np.repeat(train.number.median(),
    #                                                                                               len(predictions.number)))],
    #                             'model MASE': [
    #                                 mean_absolute_scaled_error(train, predictions.number, predictions.Label)],
    #                             'baseline mean MASE': [mean_absolute_scaled_error(train, predictions.number,
    #                                                                               np.repeat(train.number.mean(),
    #                                                                                         len(predictions.number)))],
    #                             'baseline median MASE': [mean_absolute_scaled_error(train, predictions.number,
    #                                                                                 np.repeat(train.number.median(),
    #                                                                                           len(predictions.number)))],
    #                             'model type': [pull().iloc[0:1].Model[0]]})
    #         results_all = pd.concat([results, results_all]).reset_index(drop=True)
    #         results_all.to_csv(path_to_stats)
    #         if idx == 0:
    #             all_preds = predict_model(final_best, data=dt)
    #             df = dt_all.loc[dt_all.departure == station, :].reset_index(drop=True)
    #             df = df.merge(all_preds, how='left', on=['date'])
    #             df.loc[df.Label.isna(), 'Label'] = 0
    #             df = df[['cal_year_x', 'cal_month_x', 'cal_day_x', "number_x", "Label"]]
    #             df['date'] = pd.to_datetime(
    #                 df['cal_year_x'].astype(str) + df['cal_month_x'].astype(str) + df['cal_day_x'].astype(str),
    #                 format='%Y%m%d')
    #             df = df[['date', 'number_x', 'Label']]
    #             df = df.groupby(['date']).agg(['mean']).reset_index()
    #             df.columns = ['date', 'Value', 'Prediction']
    #             df.loc[:, 'station'] = station
    #             df_predictions_all = pd.concat([df_predictions_all, df]).reset_index(drop=True)
    #             df_predictions_all.to_csv(path_to_predictions)
    #             path_to_model = './data/models/model_for_' + station.replace("/", "-")
    #             save_model(final_best, path_to_model)

    #forecast_weather_download()
    #forecast_weather_transformation(df=pd.read_pickle('./data/forecast_weather_dict.pickle'))

    #prepare_time_series_data(load_and_filter_data('./data/all_dt.csv'))
    #transform_time_series('./data/time_series_data.csv', './data/weather_time_series.csv')

    #forecast_weather_download('./data/')
    #df = forecast_weather_transformation()
    #print(df)
    #train_models_and_save_predictions()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
