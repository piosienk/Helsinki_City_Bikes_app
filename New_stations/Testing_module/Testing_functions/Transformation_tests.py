import unittest
import pandas as pd

from New_stations.Functions.Data_ETL_functions.Stations_module.Stations_set_prep import split_ww_data, \
    calculate_graph_variables
from New_stations.Functions.Data_ETL_functions.Joint_ETL_module.Places_ETL import places_data_transformation
import os

from New_stations.Functions.Data_ETL_functions.Stations_module.Stations_ETL import load_and_filter_data

os.chdir("//")


class UnitTests(unittest.TestCase):

    def test_places_transformation(self):
        df_testing = pd.read_csv("./Testing_module/Testing_data/testing_summary_places_05.csv", index_col=0)
        df_testing_cols = df_testing.columns.values

        df_all_stations = places_data_transformation()

        df_all_stations_cols = df_all_stations.columns.values

        self.assertCountEqual(df_testing_cols, df_all_stations_cols)
        self.assertFalse(df_all_stations.isna().any().any())

    def test_existing_stations_transformation(self):
        df_testing_weekend = pd.read_csv("./Testing_module/Testing_data/testing_df_weekend_from_2018.csv", index_col=0)
        df_testing_working = pd.read_csv("./Testing_module/Testing_data/testing_df_working_from_2018.csv", index_col=0)

        df_testing_work_cols = df_testing_working.columns.values
        df_testing_week_cols = df_testing_weekend.columns.values

        df_weekend_numbers, df_working_numbers = split_ww_data()

        df_work_cols = df_weekend_numbers.columns.values
        df_week_cols = df_working_numbers.columns.values

        self.assertCountEqual(df_testing_week_cols, df_week_cols)
        self.assertCountEqual(df_testing_work_cols, df_work_cols)
        self.assertFalse(df_weekend_numbers.isna().any().any())
        self.assertFalse(df_working_numbers.isna().any().any())

    def test_graphs_transformation(self):
        df_testing_metrics = pd.read_csv("./Testing_module/Testing_data/testing_stations_metrics_10.csv", index_col=0)

        df_testing_metrics_cols = df_testing_metrics.columns.values

        df_raw = load_and_filter_data("Data/Bikes/database.csv")
        df_metrics = calculate_graph_variables(df_raw)

        df_metrics_cols = df_metrics.columns.values

        self.assertCountEqual(df_testing_metrics_cols, df_metrics_cols)
        self.assertFalse(df_metrics.isna().any().any())


if __name__ == '__main__':
    unittest.main()
