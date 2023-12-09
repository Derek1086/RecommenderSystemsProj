from ingest.json_ingest import load_json_parallel
from ingest.utils import get_path, get_image_path
import seaborn as sns
import matplotlib.pyplot as plt
import logging
from sys import stdout
import pandas as pd

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=stdout)
    print(f'User Analysis Script')
    user_df = load_json_parallel(get_path('user'))
    print(f'User Count: {user_df["user_id"].nunique()}')
    logging.info(f'Generating user average rating histogram...')
    plt.figure(figsize=(10, 6))
    sns.histplot(user_df['average_stars'], bins=[1.0, 2.0, 3.0, 4.0, 5.0], kde=False, edgecolor='black')

    plt.title('Distribution of User Star Ratings')
    plt.xlabel('Star Ratings')
    plt.ylabel('Number of Users')

    plt.savefig(get_image_path('avg_user_ratings'), dpi=300, bbox_inches='tight')
    plt.show()

    logging.info(f'Generating user rating count box plot...')
    plt.figure(figsize=(10, 6))
    sns.boxplot(user_df['review_count'])

    plt.title('Distribution of User Ratings (Count)')
    plt.xlabel('Number of Ratings')
    plt.ylabel('Number of Users')

    plt.savefig(get_image_path('avg_user_rating_count_boxplot'), dpi=300, bbox_inches='tight')
    plt.show()

    logging.info(f'Generating user rating count histogram...')
    # Define bins
    bins = [0, 2, 5, 10, 20, 30, 40, 50, 100, 250, 500, 1000, float('inf')]

    labels = ['0', '2', '5', '10', '20', '30', '40', '50', '100', '250', '500', '1000+']
    # Bin the data
    user_df['binned_ratings'] = pd.cut(user_df['review_count'], bins=bins, labels=labels)
    # Plotting the binned data
    sns.countplot(x=user_df['binned_ratings'])
    plt.xlabel('Ratings Count')
    plt.ylabel('Number of Users')
    plt.title('Scaled Histogram of Ratings Count')
    plt.xticks(rotation=45)
    plt.savefig(get_image_path('avg_user_rating_count_histogram'), dpi=300, bbox_inches='tight')
    plt.show()




