# Time-based Yelp Reviews
**Finance & Commerce**

## Team
- Derek Avila (dka46)
- Carson Rau (ruu6)

## Dataset
We will be using the [Yelp Academic Dataset](https://www.yelp.com/dataset) to complete this assignment. This dataset 
focuses on ~150,000 businesses
from 11 metropolitan areas. There are over 6.9 million total reviews, where additional factors (like business hours, and
parking availability) are also considered. There is an additional data file containing "check-in" data; which refers to
individual users visiting a business multiple times. This dataset is accessible in json format using the given link.

## Project

**Motivation**

People's wants and desires out of businesses change as the day progresses. 
In order to create more accurate recommendations, a system must consider any differences in ratings that happen
over the span of a business' operation. So, we want to create a recommender that will dynamically recommend items 
based on the time of the recommendation. In some cases, the direct time of recommendation is not suitable. In these
cases, we will request a "visit time" from the user to provide an accurate recommendation.

This is application-specific because it has a direct user application.

**Method**

Because the data set is large, we will use a hybrid recommender algorithm to test a multitude of approaches to recommendation.
We will focus on:
- Matrix Factorization (MF)
- K-Nearest Neighbors
- User-Based Collaborative Filtering (CF)
- Item-Item Content Filtering (Focusing on time of review)

**Intended Experiments**

- We will use RMSE, MAE, and NDCG to rank the recommendation algorithms.
- For each algorithm, we will test -- ideally -- on the whole dataset; however, if this cannot be achieved, we will
  stratify the businesses by location (as the dataset focuses on 11 metro regions) to aid in accelerating the computations.
- For each algorithm, we will use location and rating as the factors to train our dataset in a 75%/25% train/test split.
