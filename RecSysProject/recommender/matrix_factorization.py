#
# matrix_factorization.py
# # Matrix Factorization Collaborative Filtering Model.
#
# Derek Avila - Fall 2023
#

import json
import pandas as pd
from math import log2
from surprise import AlgoBase, accuracy, Dataset, Reader, SVD
from surprise.model_selection import train_test_split


class MF(AlgoBase):
    # Initialize the Matrix Factorization model.
    #
    # Parameters:
    #   - learning_rate: The learning rate for model training.
    #   - num_epochs: Number of epochs for model training.
    #   - num_factors: Number of latent factors in the model.
    def __init__(self, learning_rate=0.01, num_epochs=10, num_factors=100):
        self.learning_rate = learning_rate
        self.num_epochs = num_epochs
        self.num_factors = num_factors
        self.model = None

    # Fit the Matrix Factorization model to the training data.
    #
    # Parameters:
    #   - train: The training dataset.
    #
    # Returns: None
    def fit(self, train):
        self.trainset = train
        self.model = SVD(n_factors=self.num_factors, n_epochs=self.num_epochs, lr_all=self.learning_rate)
        self.model.fit(self.trainset)

    # Estimate the rating for a user-item pair.
    #
    # Parameters:
    #   - u: User ID.
    #   - i: Item ID.
    #
    # Returns: Estimated rating for the user-item pair.
    def estimate(self, u, i):
        if self.model is not None:
            return self.model.predict(uid=u, iid=i).est
        else:
            raise Exception("Model has not been trained.")

# Load data from the JSON file
data = []
with open('../data/yelp_academic_dataset_review.json', 'r', encoding='utf-8') as file:
    for line in file:
        data.append(json.loads(line))

df = pd.DataFrame(data)
reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(df[['user_id', 'business_id', 'stars']], reader)

# Split the data into train and test sets with a 75/25 split
trainset, testset = train_test_split(data, test_size=0.25, random_state=42)

model = MF(learning_rate=0.005, num_epochs=20, num_factors=100)
model.fit(trainset)

# Predictions on the test set
predictions = model.test(testset)

# Calculate and print MAE and RMSE
mae = accuracy.mae(predictions)
rmse = accuracy.rmse(predictions)
print(f'MAE: {mae}')
print(f'RMSE: {rmse}')

# Calculate NDCG for MF
#
# Calculate Normalized Discounted Cumulative Gain (NDCG) at a given value of k.
#
# Parameters:
#   - predictions: List of predictions.
#   - k: The number of top predictions to consider.
#
# Returns: NDCG@k value.
def ndcg(predictions, k=10):
    relevant_items = set([str(row.iid) for row in predictions if int(row.r_ui) >= 3])
    
    dcg = 0
    idcg = sum(1.0 / (log2(i + 1) if i > 0 else 1.0) for i in range(1, min(k, len(relevant_items)) + 1))
    
    for i, prediction in enumerate(predictions[:k]):
        if str(prediction.iid) in relevant_items:
            dcg += 1.0 / log2(i + 2)
    
    return dcg / idcg

ndcg_value = ndcg(predictions)
print(f'NDCG@{len(predictions)}: {ndcg_value}')
