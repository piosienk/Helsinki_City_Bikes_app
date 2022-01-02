import unittest
import pandas as pd
from New_stations.Functions.Data_ETL_functions.Stations_module.Stations_set_creation import aggregate_stations_data
from New_stations.Functions.Data_ETL_functions.Locations_module.Locations_set_creation import aggregate_locations_data
import os

os.chdir("//")


class UnitTests(unittest.TestCase):

    def test_aggregate_stations(self):
        df_testing = pd.read_csv("./Testing_module/Testing_data/testing_working_day_modelling_dataset.csv", index_col=0)
        df_testing_cols = df_testing.columns.values

        df_weekend, df_working = aggregate_stations_data()

        df_weekend_cols = df_weekend.columns.values
        df_working_cols = df_working.columns.values

        self.assertCountEqual(df_testing_cols, df_weekend_cols)  # add assertion here
        self.assertCountEqual(df_testing_cols, df_working_cols)  # add assertion here
        self.assertFalse(df_weekend.isna().any().any())
        self.assertFalse(df_working.isna().any().any())

    def test_aggregate_locations(self):
        df_testing = pd.read_csv("./Testing_module/Testing_data/testing_locations_modelling_dataset.csv", index_col=0)
        df_testing_cols = df_testing.columns.values

        df_new_locations = aggregate_locations_data()

        df_locations_cols = df_new_locations.columns.values

        self.assertCountEqual(df_testing_cols, df_locations_cols)
        self.assertFalse(df_new_locations.isna().any().any())


if __name__ == '__main__':
    unittest.main()
