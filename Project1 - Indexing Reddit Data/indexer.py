import os
# import pysolr
import requests
import json

import subprocess
import sys

try:
    import pandas as pd
    import pysolr as pysolr
    import psaw as pw
except ValueError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandas'])
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pysolr'])
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'psaw'])
finally:
    import pandas as pd

CORE_NAME = "IRF22P1"
AWS_IP = "34.125.174.158"


def delete_core(core=CORE_NAME):
    print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
            core=core)))

def removed(df, type): #return the percentage of removed submissions or comments
  
  total_rows = len(df)
  if type == 'sub':
    removed_rows = len(df.loc[df['selftext'] == '[removed]']) + len(df.loc[df['selftext'] == '[deleted]'])
    removed_precentage = (1- ((total_rows - removed_rows)/total_rows)) * 100
    print("submissions containing [removed] or [deleted] in selftext: ", round(removed_precentage, 3))
  elif type == 'com':
    removed_rows = len(df.loc[df['body'] == '[removed]']) + len(df.loc[df['body'] == '[deleted]'])
    removed_precentage = (1- ((total_rows - removed_rows)/total_rows)) * 100
    print("comments containing [removed] or [deleted] in body: ", round(removed_precentage, 3))
  else:
    removed_rows = len(df.loc[df['body'] == '[removed]']) + len(df.loc[df['body'] == '[deleted]']) + len(df.loc[df['selftext'] == '[removed]']) + len(df.loc[df['selftext'] == '[deleted]'])
    removed_precentage = (1- ((total_rows - removed_rows)/total_rows)) * 100
    print("comments/submissions containing [removed] or [deleted] in body: ", round(removed_precentage, 3))

# Combined

def getCombinedDocs(combined):
    combined[['id', 'subreddit', 'full_link', 'title','author', 'topic', 'body','parent_id', 'parent_body']] = combined[['id', 'subreddit', 'full_link', 'title','author', 'topic', 'body','parent_id', 'parent_body']].astype('string')
    combined[['is_submission']] = combined[['is_submission']].astype('boolean')
    combined[['selftext']] = combined[['selftext']].astype('string')

    combined[['created_at']] = combined[['created_at']].astype('string')

    combined = combined.fillna("null")

    final_combined = []
    for index, row in combined.iterrows():
        items={"id":row[0], "subreddit":row[1], "full_link":row[2], "title":row[3], "selftext":row[4],   "author":row[5], "is_submission":row[6], "topic":row[7], "created_at":row[8], "body":row[9], "parent_id":row[10], "parent_body":row[11]}
        final_combined.append(items)
    
    return final_combined, combined

combined = pd.read_pickle("finalCombined_parentbody.pkl")
combined = combined.drop('index', axis=1)
combined = combined.drop('level_0', axis=1)
print(combined.columns)
final_combined, combined = getCombinedDocs(combined)

# Submissions
def getSubmissionsDocs(submissions):

    submissions[['id', 'subreddit', 'full_link', 'title','author', 'topic']] = submissions[['id', 'subreddit', 'full_link', 'title','author', 'topic']].astype('string')
    submissions[['is_submission']] = submissions[['is_submission']].astype('boolean')
    submissions[['selftext']] = submissions[['selftext']].astype('string')
    submissions[['created_at']] = submissions[['created_at']].astype('string')

    final_submissions = []
    for index, row in submissions.iterrows():
        # print(row[1])
        # add record for indexing
        items={"id":row[0], "subreddit":row[1], "full_link":row[2], "title":row[3], "selftext":row[4],   "author":row[5], "is_submission":row[6], "topic":row[7], "created_at":row[8]}
        final_submissions.append(items)
    
    return final_submissions

# submissions = pd.read_pickle("finalSubmissions.pkl")
# final_submissions = getSubmissionsDocs(submissions)

# COMMENTS
def getCommentsDocs(comments):
    comments[['id', 'subreddit', 'full_link', 'parent_id','author', 'permalink', 'title', 'body']] = comments[['id', 'subreddit', 'full_link', 'title','author', 'topic']].astype('string')
    comments[['is_submission']] = comments[['is_submission']].astype('boolean')
    comments[['created_at']] = comments[['created_at']].astype('string')

    print(comments.head())

    import numpy as np

    comments = comments.fillna(np.nan).replace([np.nan], [None])


    final_comments = []
    for index, row in comments.iterrows():
        # print(row[1])
        # add record for indexing
        items={"id":row[0], "subreddit":row[1], "full_link":row[2], "title":row[3], "selftext":row[4],   "author":row[5], "is_submission":row[6], "topic":row[7], "created_at":row[8]}
        final_comments.append(items)
    
    return final_comments

# comments = pd.read_pickle("finalComments.pkl")
# final_comments = getCommentsDocs(comments)

class Indexer:
    def __init__(self):
        self.solr_url = f'http://{AWS_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def do_initial_setup(self):
        delete_core()
        create_core()

    def create_documents(self, docs):
        print(self.connection.add(docs))

    def add_fields(self):
        data = {
            "add-field": [
                # {
                #     "name": "id",
                #     "type": "string",
                #     "multiValued": False
                # },
                {
                    "name": "subreddit",
                    "type": "string",
                    "multiValued": False
                }, {
                    "name": "full_link",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "title",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "selftext",
                    "type": "text_en",
                    "multiValued": False
                },
                {
                    "name": "author",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "is_submission",
                    "type": "boolean",
                    "multiValued": False
                },
                {
                    "name": "topic",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "created_at",
                    "type": "pdate",
                    "multiValued": False
                },
                {
                    "name": "body",
                    "type": "text_en",
                    "multiValued": False
                },
                {
                    "name": "parent_id",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "parent_body",
                    "type": "text_en",
                    "multiValued": False
                },
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


if __name__ == "__main__":
    i = Indexer()
    i.do_initial_setup()
    i.add_fields()
    i.create_documents(final_combined)
    # i.create_documents(final_submissions)
    # i.create_documents(final_comments)
