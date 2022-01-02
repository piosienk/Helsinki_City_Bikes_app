import os

from pycaret.regression import *


def train_locations_model(model_type):
    """

    :param model_type: specify whether the model will be trained on weekend or working-day data

    Functions creates regression model using PyCaret. Firstly, outliers (based on the 97.5 percentile threshold)
    and features with multicollinearity are removed. Then PyCaret performs features selection based on decision trees.
    Features are normalized, transformed (target variable is also transformed with box-cox method). Tuning of
    hyperparameters is performed with 10-fold CV.

    :return: created and tuned model
    """

    if model_type == "working_days":
        path = "./Data/Modelling/working_day_modelling_dataset.csv"
        seed_id = 117
        save_model_path = './Data/Modelling/working_tuned_random_forest_regressor'
        save_model_scores = './Data/Statistics/working_tuned_random_forest_regressor_scores.csv'
        save_model_features = "./Data/Statistics/working_tuned_random_forest_regressor_features.csv"
    elif model_type == "weekend":
        path = "./Data/Modelling/weekend_modelling_dataset.csv"
        seed_id = 123
        save_model_path = './Data/Modelling/weekend_tuned_random_forest_regressor'
        save_model_scores = './Data/Statistics/weekend_tuned_random_forest_regressor_scores.csv'
        save_model_features = "./Data/Statistics/weekend_tuned_random_forest_regressor_features.csv"

    else:
        print("Not applicable model_type selected. Assuming model_type == 'weekend'")
        path = "./Data/Modelling/weekend_modelling_dataset.csv"
        seed_id = 123
        save_model_path = './Data/Modelling/weekend_tuned_random_forest_regressor'
        save_model_scores = './Data/Statistics/weekend_tuned_random_forest_regressor_scores.csv'
        save_model_features = "./Data/Statistics/weekend_tuned_random_forest_regressor_features.csv"

    df = pd.read_csv(path, index_col=0)
    percentile_975 = df.number.describe(percentiles=[0.25, 0.50, 0.75, 0.90, 0.95, 0.975, 0.99])["97.5%"]

    df_dropped = df.drop(["latitude", "longitude"], axis=1, inplace=False)
    df_dropped = df_dropped.loc[df_dropped.number <= percentile_975, :]

    exp_reg = setup(
        df_dropped, target="number", session_id=seed_id, train_size=0.999, ignore_features=['station'],
        normalize=True, transformation=True, transform_target=True, remove_multicollinearity=True,
        polynomial_features=True, polynomial_threshold=0.5, multicollinearity_threshold=0.9, feature_selection=True,
        feature_selection_threshold=0.6, transform_target_method="box-cox", silent=True, fold_shuffle=True, fold=10)

    random_forest_regressor = create_model("rf")
    print(pull())
    tuned_random_forest_regressor = tune_model(random_forest_regressor, n_iter=50)
    best_model_results = pull()
    print(best_model_results)

    features_importance = pd.DataFrame({'Feature': get_config('X_train').columns,
                                        'Value': tuned_random_forest_regressor.feature_importances_}).sort_values(
        by='Value', ascending=False)

    features_importance.to_csv(save_model_features)

    best_model_results.to_csv(save_model_scores)

    finalized_tuned_model = finalize_model(tuned_random_forest_regressor)
    save_model(finalized_tuned_model, save_model_path)

    return finalized_tuned_model


def predict_stations(df_locations, weekend_model, working_model, min_distance_in_grid):
    """

    :param df_locations: dataframe with new locations data
    :param weekend_model: created model for weekend bike rides
    :param working_model: created model for working-day bike rides
    :param min_distance_in_grid: minimal distance between two locations (if nearest stations is farther than
    min distance, it means that it's seperated point (e.g. island without connection to the mainland))
    :return: dataframes with predicted bike rides for each localization
    """

    df_locations = df_locations.loc[df_locations.Nearest_station < min_distance_in_grid, :]

    # Align with training set
    df_locations["station"] = "0"
    df_locations["number"] = 0
    df_locations = df_locations.rename(columns={'lat': 'latitude'})
    df_locations = df_locations.rename(columns={'lon': 'longitude'})

    df_locations_weekend = df_locations.drop(["Nearest_station", "Neigh_1_number_working", 'Neigh_2_number_working',
                                              'Neigh_3_number_working'], axis=1, inplace=False)

    df_locations_weekend = df_locations_weekend.rename(columns={'Neigh_1_number_weekend': 'Neigh_1_number'})
    df_locations_weekend = df_locations_weekend.rename(columns={'Neigh_2_number_weekend': 'Neigh_2_number'})
    df_locations_weekend = df_locations_weekend.rename(columns={'Neigh_3_number_weekend': 'Neigh_3_number'})

    df_locations_working = df_locations.drop(["Nearest_station", "Neigh_1_number_weekend", 'Neigh_2_number_weekend',
                                              'Neigh_3_number_weekend'], axis=1, inplace=False)

    df_locations_working = df_locations_working.rename(columns={'Neigh_1_number_working': 'Neigh_1_number'})
    df_locations_working = df_locations_working.rename(columns={'Neigh_2_number_working': 'Neigh_2_number'})
    df_locations_working = df_locations_working.rename(columns={'Neigh_3_number_working': 'Neigh_3_number'})

    df_predicted_locations_working = predict_model(working_model, df_locations_working)
    df_predicted_locations_working.Label.describe(percentiles=[0.5, 0.75, 0.9, 0.95, 0.99])
    if os.path.exists("./Data/Results/working_locations_predicted.csv"):

        os.replace("./Data/Results/working_locations_predicted.csv", "./Data/Temp/old_working_locations_predicted.csv")
        df_predicted_locations_working.to_csv("./Data/Results/working_locations_predicted.csv")

    else:
        df_predicted_locations_working.to_csv("./Data/Results/working_locations_predicted.csv")

    df_predicted_locations_weekend = predict_model(weekend_model, df_locations_weekend)
    df_predicted_locations_weekend.Label.describe(percentiles=[0.5, 0.75, 0.9, 0.95, 0.99])
    if os.path.exists("./Data/Results/weekend_locations_predicted.csv"):

        os.replace("./Data/Results/weekend_locations_predicted.csv", "./Data/Temp/old_weekend_locations_predicted.csv")
        df_predicted_locations_weekend.to_csv("./Data/Results/weekend_locations_predicted.csv")

    else:
        df_predicted_locations_weekend.to_csv("./Data/Results/weekend_locations_predicted.csv")

    return df_predicted_locations_weekend, df_predicted_locations_working
