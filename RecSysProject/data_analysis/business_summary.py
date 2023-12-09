from ingest.json_ingest import load_json_parallel
from ingest.utils import get_path, get_image_path
import seaborn as sns
import matplotlib.pyplot as plt
import logging
from sys import stdout

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=stdout)
    print(f'Business Analysis Script')
    business_df = load_json_parallel(get_path('business'))
    print(f'Business Count: {business_df["business_id"].nunique()}')
    logging.info(f'Generating business rating histogram...')
    plt.figure(figsize=(10, 6))
    sns.histplot(business_df['stars'], bins=[1.0, 2.0, 3.0, 4.0, 5.0], kde=False, edgecolor='black')
    plt.title('Distribution of Business Star Ratings')
    plt.xlabel('Star Ratings')
    plt.ylabel('Number of Businesses')
    plt.savefig(get_image_path('avg_business_ratings'), dpi=300, bbox_inches='tight')
    plt.show()


