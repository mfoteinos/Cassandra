from datetime import datetime

import pandas as pd


movie_df = pd.read_csv("movie.csv")
rating_df = pd.read_csv("rating.csv")
rating = [0] * 131263
appears = [0] * 131263
print(movie_df)
print(len(rating_df))

timestamp1 = "2000-01-01 00:00:00"
timestamp1 = datetime.strptime(timestamp1, "%Y-%m-%d %H:%M:%S")
for i in range(len(rating_df)):
    # timestamp2 = datetime.strptime(rating_df['timestamp'].iloc[i], "%Y-%m-%d %H:%M:%S")
    # if timestamp2 > timestamp1:
    j = rating_df['movieId'].iloc[i]
    rating[j] += rating_df['rating'].iloc[i]
    appears[j] += 1
    if i % 1000000 == 0:
        print(i)

print(rating)
print(appears)
movie_df.insert(3, 'rating', None)
for i in range(len(movie_df)):
    j = rating_df['movieId'].iloc[i]
    if appears[j] > 0:
        movie_df['rating'].iloc[i] = round(rating[j] / appears[j], 2)

movie_df.to_csv("movie.csv", index=False, encoding="utf-8-sig")

print(rating)
