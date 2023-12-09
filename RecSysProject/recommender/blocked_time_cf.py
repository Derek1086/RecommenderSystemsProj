from ingest.utils import get_path
from ingest.json_ingest import load_json_parallel
from ingest.parquet_ingest import parquet_read
from surprise import AlgoBase, Dataset, Reader
from surprise.model_selection import train_test_split
from surprise.accuracy import rmse, mae
import pandas as pd
import logging
from sys import stdout
from datetime import datetime, timedelta


# Custom Algorithm Class
class TimeBasedRecommender(AlgoBase):
    def __init__(self, business_hours, reviews):
        AlgoBase.__init__(self)
        self.business_hours = business_hours
        self.reviews = reviews
        self.business_hours_processed = None
        self.weighted_avg = None

    def fit(self, trainset):
        AlgoBase.fit(self, trainset)

        # Convert timestamps in reviews to datetime
        self.reviews['date'] = pd.to_datetime(self.reviews['date'])

        # Weighted Average Calculation
        # Assuming newer reviews are more relevant
        current_time = pd.Timestamp.now()
        self.reviews['time_diff'] = (current_time - self.reviews['date']).dt.days
        self.reviews['weight'] = 1 / (1 + self.reviews['time_diff'])
        self.reviews['weighted_star'] = self.reviews['stars'] * self.reviews['weight']
        self.weighted_avg = self.reviews.groupby('business_id')['weighted_star'].sum() / \
                            self.reviews.groupby('business_id')['weight'].sum()

        # Preprocessing Business Hours
        # Transform the business hours into 1-hour blocks
        self.business_hours_processed = self._preprocess_business_hours()
        return self

    def estimate(self, u, i):
        try:
            user_id = self.trainset.to_raw_uid(u)
            business_id = self.trainset.to_raw_iid(i)

            # Check if business hours match user's preferred hours
            if self._business_hours_match(user_id, business_id):
                # Get the weighted average rating of the business
                business_rating = self.weighted_avg.get(business_id, 0)
                return business_rating
            else:
                return 0
        except ValueError:
            return 0

    def _preprocess_business_hours(self):
        # Initialize an empty dictionary to store processed hours
        processed_hours = {}

        # Iterate over each business
        for idx, row in self.business_hours.iterrows():
            business_id = row['business_id']
            processed_hours[business_id] = []

            # List of days
            days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

            # Process each day
            for day in days:
                open_time = row[f'{day}_open']
                close_time = row[f'{day}_close']
                # Check if the business is open on this day
                if pd.notnull(open_time) and pd.notnull(close_time):
                    # Convert times to datetime objects
                    open_time = datetime.strptime(open_time, '%H:%M')
                    close_time = datetime.strptime(close_time, '%H:%M')
                    # Generate 1-hour time blocks
                    current_time = open_time
                    while current_time < close_time:
                        next_time = current_time + timedelta(hours=1)
                        # Add time block to the business's schedule
                        processed_hours[business_id].append((day, current_time.time(), next_time.time()))
                        current_time = next_time
        return processed_hours

    def _business_hours_match(self, user_id, business_id):
        # Get all reviews made by the user
        user_reviews = self.reviews[self.reviews['user_id'] == user_id]

        # If the user has no reviews, we cannot determine their preferred hours
        if user_reviews.empty:
            return False

        # Extract hours from review timestamps (assuming timestamp is in datetime format)
        user_hours = user_reviews['date'].dt.hour

        # Get the business's operating hours
        business_hours = self.business_hours_processed.get(business_id, [])

        # Check if any of the user's preferred hours overlap with the business hours
        for day, start, end in business_hours:
            # Extract hours from start and end times
            start_hour = start.hour
            end_hour = end.hour

            for hour in user_hours:
                if start_hour <= hour < end_hour:
                    return True

        return False


# Function to train and test the recommender
def train_and_evaluate(algo, data, test_size):
    trainset, testset = train_test_split(data, test_size=test_size)
    print(f'Training {1.0 - test_size}')
    algo.fit(trainset)
    print(f'Testing {test_size}')
    predictions = algo.test(testset)
    print(f"RMSE w/ test size = {test_size} = {rmse(predictions)}")
    print(f"MAE w/ test size = {test_size} = {mae(predictions)}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=stdout)
    business_hours_data = parquet_read(
        get_path('business_hours_data', 'data_preprocess', False, False))
    reviews_data = load_json_parallel(get_path('review'))
    logging.info(f'Done.')

    # Load the dataset
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(reviews_data[['user_id', 'business_id', 'stars']], reader)

    # Algorithm
    algo = TimeBasedRecommender(business_hours_data, reviews_data)

    # Train and test with 80-20 split
    train_and_evaluate(algo, data, test_size=0.2)

    # Train and test with 75-25 split
    train_and_evaluate(algo, data, test_size=0.25)