#
# k_nearest_neighbors.py
# KNN Model.
#
# Derek Avila - Fall 2023
#

import pandas as pd
import json
import random
import numpy as np
from surprise import Dataset, Reader, KNNBasic, accuracy
from surprise.model_selection import train_test_split

# Load a random subset of the data
data = []
with open('../data/yelp_academic_dataset_review.json', 'r', encoding='utf-8') as file:
    for line in file:
        if random.random() < 0.015:
            data.append(json.loads(line))

df = pd.DataFrame(data)
df['stars'] = pd.to_numeric(df['stars'])  # Convert 'stars' to numeric

reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(df[['user_id', 'business_id', 'stars']], reader)

trainset, testset = train_test_split(data, test_size=0.25)

algo_knn = KNNBasic()
algo_knn.fit(trainset)
predictions_knn = algo_knn.test(testset)

# Calculate RMSE for KNN
rmse_knn = accuracy.rmse(predictions_knn)
mae_knn = accuracy.mae(predictions_knn)
print(f"RMSE: {rmse_knn}")
print(f"MAE: {mae_knn}")

# Calculate NDCG for KNN
#
# Calculate Normalized Discounted Cumulative Gain (NDCG) at a given value of k.
#
# Parameters:
#   - predictions: List of predictions.
#   - k: The number of top predictions to consider.
#
# Returns: NDCG@k value.
def ndcg_at(predictions, k=10):
    # Sort predictions by estimated rating
    ranked_predictions = sorted(predictions, key=lambda x: x.est, reverse=True)

    # Extract the true ratings from the test set
    true_ratings = np.array([pred.r_ui for pred in ranked_predictions])

    # Calculate the discounted cumulative gain
    dcg = np.sum((2 ** true_ratings - 1) / np.log2(np.arange(2, len(true_ratings) + 2)))

    # Sort true ratings in descending order to calculate ideal discounted cumulative gain
    ideal_ratings = np.sort(true_ratings)[::-1]

    # Calculate the ideal discounted cumulative gain
    idcg = np.sum((2 ** ideal_ratings - 1) / np.log2(np.arange(2, len(ideal_ratings) + 2)))

    # Calculate normalized discounted cumulative gain
    ndcg = dcg / idcg if idcg > 0 else 0

    return ndcg

ndcg_knn = ndcg_at(predictions_knn, k=10)
print(f"NDCG@10: {ndcg_knn}")
