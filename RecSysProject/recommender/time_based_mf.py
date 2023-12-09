#
# time_based_mf.py
# Time based Matrix Factorization Collaborative Filtering Model.
#
# Derek Avila - Fall 2023
#

import pandas as pd
import numpy as np
from surprise import Dataset, Reader, SVD, AlgoBase
from surprise.model_selection import train_test_split
from surprise import accuracy
import json
import logging
from datetime import datetime

# Calculate NDCG for MF
#
# Calculate Normalized Discounted Cumulative Gain (NDCG) at a given value of k.
#
# Parameters:
#   - predictions: List of predictions.
#   - k: The number of top predictions to consider.
#
# Returns: NDCG@k value.
def ndcg_at_k(predictions, k=10):
    top_k = dict()
    for uid, iid, true_r, est, _ in predictions:
        if uid not in top_k:
            top_k[uid] = []
        top_k[uid].append((iid, est, true_r))

    ndcg_sum = 0
    for uid, user_ratings in top_k.items():
        user_ratings.sort(key=lambda x: x[1], reverse=True)
        dcg = 0
        idcg = 0

        for i in range(min(k, len(user_ratings))):
            dcg += (2 ** user_ratings[i][2] - 1) / np.log2(i + 2)

        user_ratings.sort(key=lambda x: x[2], reverse=True)
        for i in range(min(k, len(user_ratings))):
            idcg += (2 ** user_ratings[i][2] - 1) / np.log2(i + 2)

        if idcg == 0:
            ndcg_sum += 0
        else:
            ndcg_sum += dcg / idcg

    return ndcg_sum / len(top_k)


# Custom function to recommend businesses based on opening and closing times
#
# Recommends businesses to a user based on their opening and closing times and predicted ratings.
#
# Parameters:
#   - user_id: User ID for whom recommendations are generated.
#   - business_hours_data: DataFrame containing business opening and closing times.
#   - algo: Trained collaborative filtering algorithm (Surprise SVD in this case).
#   - business_dict: Dictionary mapping business IDs to business details.
#   - num_recommendations: Number of business recommendations to generate (default is 10).
#
# Returns: List of recommended business names.
def recommend_businesses(user_id, business_hours_data, algo, business_dict, num_recommendations=10):
    unrated_businesses = business_hours_data[~business_hours_data['business_id'].isin(merged_data[merged_data['user_id'] == user_id]['business_id'])]['business_id'].unique()
    unrated_df = pd.DataFrame({'user_id': [user_id] * len(unrated_businesses), 'business_id': unrated_businesses})
    unrated_df['estimated_rating'] = unrated_df.apply(lambda row: algo.predict(user_id, row['business_id']).est, axis=1)

    current_day = datetime.now().strftime('%A').lower()
    current_time = datetime.now().time()
    current_time_intervals = (current_time.hour * 60 + current_time.minute) // 30

    unrated_df = unrated_df.merge(business_hours_data[['business_id', f'{current_day}_open', f'{current_day}_close']], on='business_id')

    # Convert opening and closing times to time in minutes
    unrated_df[f'{current_day}_open'] = unrated_df[f'{current_day}_open'].apply(lambda x: int(datetime.strptime(x, '%H:%M').hour * 60 + datetime.strptime(x, '%H:%M').minute) if x is not None else -1)
    unrated_df[f'{current_day}_close'] = unrated_df[f'{current_day}_close'].apply(lambda x: int(datetime.strptime(x, '%H:%M').hour * 60 + datetime.strptime(x, '%H:%M').minute) if x is not None else 1440)  # Assuming 1440 is the maximum value (24 hours)

    unrated_df = unrated_df[
        (unrated_df[f'{current_day}_open'] <= current_time_intervals) &
        (unrated_df[f'{current_day}_close'] > current_time_intervals)
    ]

    unrated_df = unrated_df.sort_values(by='estimated_rating', ascending=False)

    recommended_business_ids = unrated_df.head(num_recommendations)['business_id'].tolist()
    recommended_business_names = [business_dict.get(business_id, {}).get('name', f'Unknown Business {business_id}') for business_id in recommended_business_ids]

    return recommended_business_names


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

def parquet_read(filepath, logger=logging.getLogger('parquet_read')):
    try:
        logger.info(f'Reading from file at {filepath}...')
        df = pd.read_parquet(filepath, engine='pyarrow')
        logger.info(f'Done.')
        return df
    except Exception as e:
        logger.error(f'An error occurred while attempting to read {filepath}.', exc_info=True)


if __name__ == '__main__':
    # Load business hours data from Parquet file
    business_hours_data = parquet_read('../data_preprocess/business_hours_data.parquet')
    # Load data from the JSON file
    data = []
    with open('../data/yelp_academic_dataset_review.json', 'r', encoding='utf-8') as file:
        for line in file:
            data.append(json.loads(line))
    reviews_data = pd.DataFrame(data)

    # Merge the two dataframes on 'business_id'
    merged_data = pd.merge(reviews_data, business_hours_data, on='business_id')

    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(merged_data[['user_id', 'business_id', 'stars']], reader)

    # Split the data into train and test sets
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

    ndcg_10 = ndcg_at_k(predictions, k=10)
    print(f'NDCG@10: {ndcg_10}')

    # Load business data from the business JSON file
    business_data = []
    with open('../data/yelp_academic_dataset_business.json', 'r', encoding='utf-8') as file:
        for line in file:
            business_data.append(json.loads(line))

    # Convert the list of businesses to a dictionary for easy lookup
    business_dict = {business['business_id']: business for business in business_data}

    # Recommend businesses for a randomly selected user
    random_user_id = np.random.choice(merged_data['user_id'].unique())
    recommended_businesses = recommend_businesses(random_user_id, business_hours_data, model, business_dict)
    print(f'Recommended businesses for user {random_user_id}: {recommended_businesses}')
    
