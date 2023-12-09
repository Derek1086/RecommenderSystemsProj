## Implemented Algorithms

### Matrix Factorization (MF)

Matrix Factorization is a collaborative filtering technique that decomposes the user-item interaction matrix into low-rank matrices. The algorithm learns user and item embeddings to make personalized recommendations.

### K-Nearest Neighbors (KNN)

K-Nearest Neighbors is a memory-based collaborative filtering method that recommends items based on the preferences of similar users. 

### Time-Based Matrix Factorization (MF)

Similar to MF, however it recommends the top 10 businesses for a random user using 30 minute time intervals based on the day and time this algorithm is ran using the open and close hours of the businesses. 

### Item-to-Item Content Filtering (Time-based)

Item-to-Item Content Filtering recommends items based on the similarity of their content. In this implementation, the focus is on the time of the review. Items are recommended based on the temporal patterns of user reviews, considering the time of day, day of the week, or other temporal features.
