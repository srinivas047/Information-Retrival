import pandas as pd
import numpy as np

# !pip install psaw
from psaw import PushshiftAPI
api = PushshiftAPI()
# comments = pd.read_pickle("finalComments.pkl")

# finalSubmissions = pd.read_pickle("finalSubmissions.pkl")

# finalSubmissions = finalSubmissions.reset_index()
# subreddit_details = finalSubmissions[['id', 'subreddit']].loc[finalSubmissions['selftext'] != '[removed]'].loc[finalSubmissions['selftext'] != '[deleted]'].drop_duplicates(["id","subreddit"])

# print(subreddit_details['subreddit'].unique())

# # print(comments['parent_id'])

# sum1 = 0
# sum2 = 0
# for i in range(len(comments)):
#     parent = comments['parent_id'].iloc[i][:2]
#     if parent == 't3':
#         sum1 += 1
#     else:
#         sum2 += 1

# print(sum1, sum2)




test = pd.read_pickle("finalCombined_new.pkl")
# print(test.columns)


submissions = test.loc[test['is_submission'] == True]

comments = test.loc[test['is_submission'] == False]

print(len(submissions), len(comments), len(submissions) + len(comments), len(test))

print(comments['parent_body'].loc[comments['parent_body'] != 'blank'])

# print(comments['parent_id'])

# print(submissions['selftext'].loc[submissions['id'] == 'rr6t8'])

# t = 't3_rr6t8'

# print(t[3:])

t3_comments = []
for i in range(len(comments)):
  parent = comments['parent_id'].iloc[i][:2]
  if parent == 't3':
      parent_id = comments['parent_id'].iloc[i][3:]
      t3_comments.append(parent_id)
    # body = submissions['selftext'].loc[submissions['id'] == parent_id]
    # comments['parent_body'].iloc[i] = body


for i in range(len(t3_comments)):
    body = submissions['selftext'].loc[submissions['id'] == t3_comments[i]]
    print(body)
    break

print(submissions['selftext'].loc[submissions['id'] == 'tra91z'])

