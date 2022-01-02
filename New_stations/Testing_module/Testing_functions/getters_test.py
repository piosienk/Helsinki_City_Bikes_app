import unittest

import pandas as pd
import osmnx as ox

from New_stations.Functions.Data_ETL_functions.Joint_ETL_module.Places_ETL import get_distance_to_attractions
from New_stations.Functions.Data_ETL_functions.Locations_module.Locations_ETL import check_if_water, \
    get_distance_to_other
import os

os.chdir("//")


class MyTestCase(unittest.TestCase):

    def test_get_if_water(self):
        df_water = pd.read_csv("./Testing_module/Testing_data/water_testing_locations.csv")
        df_land = pd.read_csv("./Data/Testing_functions/land_testing_locations.csv")

        df_water_results = check_if_water(df_water, path="./Testing_module/Testing_data/")
        df_land_results = check_if_water(df_land, path="./Testing_module/Testing_data/")

        self.assertEqual(df_water_results.is_water.all(), True)  # add assertion here
        self.assertEqual(df_land_results.is_water.any(), False)  # add assertion here

    def test_get_distance_to_other(self):
        df_whole_sample = pd.read_csv("./Data/Locations/locations_final_list.csv", index_col=0)
        df_the_same_place = pd.read_csv("./Testing_module/Testing_data/Testing_distance.csv", index_col=0)

        df_new_whole_sample = get_distance_to_other(df_whole_sample.iloc[:100, :])
        df_new_the_same_place = get_distance_to_other(df_the_same_place)

        self.assertCountEqual(df_new_whole_sample.reset_index(drop=True), df_whole_sample.reset_index(drop=True))
        self.assertEqual(max(df_new_the_same_place.Nearest_station), 0)

    def test_get_distance_to_attractions(self):
        G = ox.load_graphml("./Data/Graph/Helsinki_roads_graph.graphml")
        df_distance_to_attractions1 = get_distance_to_attractions(G, 283277347)
        df_distance_to_attractions2 = get_distance_to_attractions(G, 9178001410)
        df_distance_to_attractions3 = get_distance_to_attractions(G, 6130847586)

        self.assertTrue(abs(df_distance_to_attractions1.loc[:, "distance"].iloc[0] - 2.9) < 0.1)
        self.assertTrue(abs(df_distance_to_attractions2.loc[:, "distance"].iloc[8] - 3.9) < 0.1)
        self.assertTrue(abs(df_distance_to_attractions3.loc[:, "distance"].iloc[4] - 3.2) < 0.1)


if __name__ == '__main__':
    unittest.main()
