# %%
import pandas as pd
import re
from nltk.corpus import stopwords
import nltk
from nltk.stem import PorterStemmer
nltk.download('stopwords')
import math
from collections import Counter
import flask
from flask import Flask
from flask import request
import json
import time
import ast

app = Flask(__name__)

# %%
# Helper Functions

# Removing stopwords
stop_words = set(stopwords.words('english')) 
def removeStopwords(tokens):
    filtered_sentence = []
    for w in tokens:
        if w not in stop_words:
            filtered_sentence.append(w)
    
    return filtered_sentence

#Porter Stemming
ps = PorterStemmer()
def stemming(tokens):
    stemmed_tokens = []
    for w in tokens:
        stemmed_tokens.append(ps.stem(w))
    
    return stemmed_tokens

# Creating a Linked List Node
class Node:
   def __init__(self, data=None, tf_idf = 0):
      self.data = data
      self.skip = None
      self.next = None
      self.tf_idf = tf_idf

# Inserting into Linked List
def insertAtEnd(head, doc_id, tf_idf):
    tempHead = Node(doc_id, tf_idf)
      
    if (head == None):
        head = tempHead
    else:
        cur_docid = head
        while (cur_docid.next != None):
            cur_docid = cur_docid.next
        cur_docid.next = tempHead
      
    return head

# Printing a linked list
def printLinkedList(head):
    cur = head
    while (cur != None) :
        # print("val", cur.data, "tf_idf",cur.tf_idf,  end = " ")
        print("val", cur.data)
        cur = cur.next

# Length of a linked list
def LinkedListLength(head):
    length = 0
    cur = head
    while (cur != None) :
        length += 1
        cur = cur.next
    
    return length

# Copy a Linked List
def copy_list(head):
    cur = head
    if cur is None:
        return cur
    
    # Create a new node
    new_node = Node(cur.data, cur.tf_idf)
    # The new nodes next pointer would point to the node returned from recursion
    # i.e. the next new node.
    new_node.next = copy_list(cur.next)
    
    return new_node

# Removing Duplicates
def removeDuplicates(list):
    cur = list
    while cur and cur.next:
        nxt = cur.next
        if cur.data == nxt.data:
            cur.next = nxt.next
        else:
            cur = nxt
    
    return list

# Merge 2 Linked Lists
def merge_list(Linkedlist1, Linkedlist2):
    newnode = Node()
    tail = newnode
    newnode.next = None
    comparisions = 0

    list1 = Linkedlist1
    list2 = Linkedlist2
    while list1 != None and list2 != None:
        if list1.data == list2.data:
            comparisions += 1
            tail.next = push((tail.next), list1.data, max(list1.tf_idf, list2.tf_idf))
            tail = tail.next
            list1 = list1.next
            list2 = list2.next
        else:
            if list1.data > list2.data:
                comparisions += 1
                list2 = list2.next
            else:
                comparisions += 1
                list1 = list1.next
    
    return newnode.next, comparisions

# Merge 2 Linked Lists Skip
def merge_listSkip(Linkedlist1, Linkedlist2):
    newnode = Node()
    newnode.next = None
    res_head = newnode
    tail = newnode
    
    comparisions = 0

    list1 = Linkedlist1
    list2 = Linkedlist2

    while list1 != None and list2 != None:
        if list1.data == list2.data:
            comparisions += 1
            tail.next = push((tail.next), list1.data, max(list1.tf_idf, list2.tf_idf))
            tail = tail.next
            list1 = list1.next
            list2 = list2.next
        else:
            if list1.data > list2.data:
                if list2.skip and list2.skip != list2 and list1.data > list2.skip.data:
                    while list1 and list2 and list2.skip and list1.data >= list2.skip.data:
                        comparisions += 1
                        list2 = list2.skip
                else:
                    comparisions += 1
                    list2 = list2.next
            else:
                if list1.skip and list1.skip != list1 and list2.data > list1.skip.data:
                    while list1 and list2 and list1.skip and (list2.data >= list1.skip.data):
                        comparisions += 1
                        list1 = list1.skip
                else:
                    comparisions += 1
                    list1 = list1.next
    
    return newnode.next, comparisions


# merge n linked lists
def merge_lists(postings_list, skip):
    sorted_lists = sorted(postings_list, key=lambda k: LinkedListLength(postings_list[k]), reverse=False)
    # print(sorted_lists[0])

    list1 = postings_list[sorted_lists[0]]

    total_comparisions = 0
    for i in range(1, len(sorted_lists)):

        list2 = postings_list[sorted_lists[i]]

        if skip:
            list1, comparisions = merge_listSkip(list1, list2)
        else:
            list1, comparisions = merge_list(list1, list2)

        total_comparisions += comparisions

    return list1, total_comparisions
    


def push(head_ref, new_data, tf_idf):
    ''' allocate node '''
    new_node = Node()
  
    ''' put in the data  '''
    new_node.data = new_data
    new_node.tf_idf = tf_idf
  
    ''' link the old list off the new node '''
    new_node.next = (head_ref)
  
    ''' move the head to point to the new node '''
    (head_ref) = new_node;   
    return head_ref

# %%
def readInputCorpus(file):
# Read input data
    input_corpus = pd.read_csv(file, delimiter = '\t', header=None)
    # Renaming columns
    input_corpus = input_corpus.rename(columns={0: "document_id", 1: "body"})

    # print(input_corpus.head())
    return input_corpus

# %%
# Converting to lowecase
def convertLower(input_corpus):
    input_corpus["body"] = input_corpus["body"].str.lower()
    return input_corpus

# %%
# Data Pre-Processing
# 1. Removed special characters
# 2. Removed excess white spaces
# 3. Whitespace tokenization
# 4. Removed stopwords
# 5. Performed Porter Stemming
def inputCorpusPreprocessing(input_corpus):
    input_corpus_prosessed = input_corpus.copy()
    for i in range(len(input_corpus_prosessed)):
        removed_text = re.sub(r"[^a-zA-Z0-9 ]", ' ', input_corpus_prosessed['body'][i])
        removing_extra_spaces = re.sub('\s{2,}', ' ', removed_text)
        stripped = removing_extra_spaces.strip()
        tokens = re.split('\s+', stripped)
        removed_stopwords = removeStopwords(tokens)
        stemmed_tokens = stemming(removed_stopwords)
        input_corpus_prosessed['body'][i] = stemmed_tokens
        
    return input_corpus_prosessed


# %%
# Creating Postings lists
def CreateArrayPostingsLists(input_corpus_prosessed):
    postings_lists = {}

    for i in range(len(input_corpus_prosessed)):
        counter = Counter(input_corpus_prosessed['body'][i])
        length = len(input_corpus_prosessed['body'][i])
        for j in input_corpus_prosessed['body'][i]:
            tf = counter[j]/length
            # print(tf)
            if j in postings_lists:
                postings_lists[j].append([input_corpus_prosessed['document_id'][i], tf])
            else:
                postings_lists[j] = [[input_corpus_prosessed['document_id'][i], tf]]
    

    # Sorting Postings lists in increasing order
    for i in postings_lists:
        postings_lists[i].sort()

    return postings_lists

# %%
# input_corpus = readInputCorpus('input_corpus.txt') #Reading Corpus
# input_corpus = convertLower(input_corpus) #LowerCase
# input_corpus_prosessed = inputCorpusPreprocessing(input_corpus) #Data Pre-processing
# postings_lists = CreateArrayPostingsLists(input_corpus_prosessed) #Array Postings Lists
# postings_linkedLists = CovertListToLinkedList(postings_lists, input_corpus_prosessed) #Converts Array Lists to Linked Lists
# # # InputCourpusValidityChecks(postings_linkedLists, postings_lists) #Validity Checks
# skip_postings_lists = SkipPostingsLists(postings_linkedLists) #Skip Postings List

# %%
# Convert array postings lists to linked list
def CovertListToLinkedList(postings_lists, input_corpus_prosessed):
    postings_linkedLists = {}

    docs_length = len(input_corpus_prosessed)
    # print(postings_lists)
    for i in postings_lists:
        newnode = None
        arr = []
        for k in postings_lists[i]:
            arr.append(k[0])
        
        idf_length = len(set(arr))
        for j in postings_lists[i]:

            tf_idf = (docs_length/idf_length) * j[1]
            newnode = insertAtEnd(newnode, j[0], tf_idf)
        postings_linkedLists[i] = newnode

    return postings_linkedLists

    # printLinkedList(postings_linkedLists['recent'])
    # LinkedListLength(postings_linkedLists['recent'])

    # return postings_linkedLists, postings_linkedLists_TfIDF



# %%
# Validity Checks
# Printing Lengths of each term postings linked list
def InputCourpusValidityChecks(postings_linkedLists, postings_lists):
    ll_lengths = []
    for i in postings_linkedLists:
        ll_lengths.append(LinkedListLength(postings_linkedLists[i]))

    # print(ll_lengths[:20])

        # Printing Lengths of each term postings list
    ar_lengths = []
    for i in postings_lists:
        ar_lengths.append(len(postings_lists[i]))

    for i in range(len(ll_lengths)):
        if ll_lengths[i] != ar_lengths[i]:
            print('False')

    skip_postings_lists = postings_linkedLists.copy()

    head = skip_postings_lists['recent']

    while head != None:
        if head.skip:
            print(head.data, head.skip.data, head.tf_idf, head.skip.tf_idf)
        else:
            print(head.data, head.skip, head.tf_idf)

        head = head.next



# %%
# Postings Lists with skip pointers
def SkipPostingsLists(postings_linkedLists):


    skip_postings_lists = postings_linkedLists.copy()
        
    for i in skip_postings_lists:
        skip_postings_lists[i] = removeDuplicates(skip_postings_lists[i])
        postings_length = LinkedListLength(skip_postings_lists[i])

        if (math.isqrt(postings_length) ** 2 == postings_length):
            skip_length = round(math.sqrt(postings_length))
            # skip_length = math.floor(math.sqrt(postings_length)) - 1
        else:
            skip_length = round(math.sqrt(postings_length))
            # skip_length = math.floor(math.sqrt(postings_length))

        # if i == 'coronaviru':
        #     print(LinkedListLength(skip_postings_lists[i]), skip_length)

        head = skip_postings_lists[i]
        cur = head
        temp = None
        if (skip_length == 0) or (skip_length == 1):
            continue
        
        while cur != None:
            temp = cur
            last = None
            
            for k in range(skip_length):
                if cur != None:               
                    last = cur
                    cur = cur.next

            if (cur == None):
                temp.skip = last
            else:    
                temp.skip = cur
    

    return skip_postings_lists

# %%
# cur = skip_postings_lists['coronaviru']
# while cur != None:
#     if cur.skip:
#         print(cur.data, cur.skip.data)
#     else:
#         print(cur.data)
    
#     cur = cur.next


# %%
# Skip Pointers Validity Check
def skipValidityCheck(postings_linkedLists):
    head = postings_linkedLists['recent']

    while head != None:
        if head.skip:
            print(head.data, head.skip.data, head.tf_idf, head.skip.tf_idf)
        else:
            print(head.data, head.skip, head.tf_idf)

        head = head.next


# skipValidityCheck(postings_linkedLists)


# %%
# Query Processing

# Read input data
input_queries = pd.read_csv("queries.txt", header=None)
# Renaming columns
input_queries = input_queries.rename(columns={0: "query"})

input_queries = input_queries.append([{'query':'recent year'}], ignore_index=True)
print(input_queries)

input = input_queries.copy()
input_queries_processed = []
for i in range(len(input)):
    input_queries_processed.append(input['query'][i])

print(input_queries_processed)

# %%
# Query Pre-Processing
def processingQueries(queries):
    # Pre Processing
    # 1. Removed special characters
    # 2. Removed excess white spaces
    # 3. Whitespace tokenization
    # 4. Removed stopwords
    # 5. Performed Porter Stemming
    query_map = {}
    output = []
    for i in range(len(queries)):
        removed_text = re.sub('[^a-zA-Z.\d\s]', ' ', queries[i])
        removing_extra_spaces = re.sub('\s{2,}', ' ', removed_text)
        stripped = removing_extra_spaces.strip()
        tokens = re.split('\s+', stripped)
        removed_stopwords = removeStopwords(tokens)
        stemmed_tokens = stemming(removed_stopwords)
        output.append(stemmed_tokens)
        query_map[queries[i]] = stemmed_tokens


    return output,query_map

# %%
# Document-at-a-time AND query without skip pointers
def daatAnd(input_queries_processed, query_map, postings_linkedLists):


    postings_linkedLists = postings_linkedLists.copy()


    # print(postings_linkedLists['recent'])

    output_postingList = {'postingsList' : {}}
    output_json_daatAnd= {'daatAnd': {}, 'daatAndTfIdf':{}}
    for i in range(len(input_queries_processed)):
        queries_posting_list = {}
        for j in input_queries_processed[i]:
            if j in postings_linkedLists:
                queries_posting_list[j] = postings_linkedLists[j]
                copy = copy_list(queries_posting_list[j])
                lists = []
                while copy != None:
                    lists.append(copy.data)
                    copy = copy.next

                lists_f = [*set(lists)]
                output_postingList['postingsList'][j] = sorted(lists_f)
            else:
                queries_posting_list[j] = None

        
        for k in queries_posting_list:
            queries_posting_list[k] = removeDuplicates(queries_posting_list[k])
            
        ans, comparisions = merge_lists(queries_posting_list, False)

        hashmap = {}

        cur = copy_list(ans)
        while cur != None:
            # print(cur.tf_idf)
            hashmap[cur.data] = cur.tf_idf
            cur = cur.next

        # print(hashmap)
        sorted_x = sorted(hashmap.items(), key=lambda kv: kv[1], reverse=True)
        # print(sorted_x)

        tf_idfDocs = []

        for i in sorted_x:
            tf_idfDocs.append(i[0])

        # print(tf_idfDocs)

            

        docs = []
        while ans != None:
            docs.append(ans.data)
            ans = ans.next

        for i in query_map:
            for k in queries_posting_list:
                if k in query_map[i]:
                    string_name = i
                    break

        string_name = string_name.strip()
        output_json_daatAnd['daatAnd'][string_name] = {}
        output_json_daatAnd['daatAndTfIdf'][string_name] = {}

        output_json_daatAnd['daatAnd'][string_name]['num_comparisons'] = comparisions
        output_json_daatAnd['daatAnd'][string_name]['num_docs'] = len(docs)
        output_json_daatAnd['daatAnd'][string_name]['results'] = sorted(docs)
        output_json_daatAnd['daatAndTfIdf'][string_name]['num_comparisons'] = comparisions
        output_json_daatAnd['daatAndTfIdf'][string_name]['num_docs'] = len(docs)
        output_json_daatAnd['daatAndTfIdf'][string_name]['results'] = tf_idfDocs

    return output_json_daatAnd, output_postingList


# %%
# Document-at-a-time AND query with skip pointers
def daatAndSkip(input_queries_processed, query_map, skip_postings_lists):
    output_postingListSkip = {'postingsListSkip' : {}}
    output_json_daatAndSkip= {'daatAndSkip': {}, 'daatAndSkipTfIdf': {}}
    for i in range(len(input_queries_processed)):
        queries_posting_listSkip = {}
        for j in input_queries_processed[i]:
            if j in skip_postings_lists:
                queries_posting_listSkip[j] = skip_postings_lists[j]
                head = queries_posting_listSkip[j]
                lists = []
                while head != None:
                    if head.skip:
                        lists.append(head.data)
                    head = head.next

                lists_f = [*set(lists)]
                output_postingListSkip['postingsListSkip'][j] = sorted(lists_f)
            else:
                output_postingListSkip[j] = None

            
        if len(queries_posting_listSkip) >= 2:
            for k in queries_posting_listSkip:
                queries_posting_listSkip[k] = removeDuplicates(queries_posting_listSkip[k])

            
            for i in query_map:
                for k in queries_posting_listSkip:
                    if k in query_map[i]:
                        string_name = i
                        break

            ans, comparisions = merge_lists(queries_posting_listSkip, True)
        
        elif len(queries_posting_listSkip) == 1:
            for key in queries_posting_listSkip:
                ans = queries_posting_listSkip[key]
                comparisions = 0

            for i in query_map:
                for k in queries_posting_listSkip:
                    if k in query_map[i]:
                        string_name = i
                        break
        else:
            ans = None
            comparisions = 0
            string_name = ''
            for i in query_map:
                for k in queries_posting_listSkip:
                    if k in query_map[i]:
                        string_name = i
                        break

        hashmap = {}

        cur = copy_list(ans)
        while cur != None:
            hashmap[cur.data] = cur.tf_idf
            cur = cur.next

        sorted_x = sorted(hashmap.items(), key=lambda kv: kv[1], reverse=True)

        tf_idfDocs = []

        for i in sorted_x:
            tf_idfDocs.append(i[0])

        # print(tf_idfDocs)

        docs = []
        while ans != None:
            docs.append(ans.data)
            ans = ans.next

        string_name = string_name.strip()
        output_json_daatAndSkip['daatAndSkip'][string_name] = {}
        output_json_daatAndSkip['daatAndSkipTfIdf'][string_name] = {}
        output_json_daatAndSkip['daatAndSkip'][string_name]['num_comparisons'] = comparisions
        output_json_daatAndSkip['daatAndSkip'][string_name]['num_docs'] = len(docs)
        output_json_daatAndSkip['daatAndSkip'][string_name]['results'] = sorted(docs)
        output_json_daatAndSkip['daatAndSkipTfIdf'][string_name]['num_comparisons'] = comparisions
        output_json_daatAndSkip['daatAndSkipTfIdf'][string_name]['num_docs'] = len(docs)
        output_json_daatAndSkip['daatAndSkipTfIdf'][string_name]['results'] = tf_idfDocs

    return output_json_daatAndSkip, output_postingListSkip
    

# %%
# input_corpus = readInputCorpus('input_corpus.txt') #Reading Corpus
# input_corpus = convertLower(input_corpus) #LowerCase
# input_corpus_prosessed = inputCorpusPreprocessing(input_corpus) #Data Pre-processing
# postings_lists = CreateArrayPostingsLists(input_corpus_prosessed) #Array Postings Lists
# postings_linkedLists = CovertListToLinkedList(postings_lists, input_corpus_prosessed) #Converts Array Lists to Linked Lists
# # # InputCourpusValidityChecks(postings_linkedLists, postings_lists) #Validity Checks
# skip_postings_lists = SkipPostingsLists(postings_linkedLists) #Skip Postings List
# # # # skipValidityCheck(postings_linkedLists) #Validity Checks
# queries = ['the novel coronavirus', 'from an epidemic to a pandemic', 'is hydroxychloroquine effective?', 'recent year']
# # # # queries = ["hello world", "hello swimming", "swimming going", "random swimming", "recent year"]
# processed_queries, query_map = processingQueries(queries) #Query Processing

# output_dict = {'postingsList': {},
#                     'postingsListSkip': {},
#                     'daatAnd': {},
#                     'daatAndSkip': {},
#                     'daatAndTfIdf': {},
#                     'daatAndSkipTfIdf': {}}

# output_json_daatAnd, output_postingList = daatAnd(processed_queries,query_map, postings_linkedLists)

# output_json_daatAndSkip, output_postingListSkip = daatAndSkip(processed_queries, query_map, skip_postings_lists)

# output_dict['postingsList'] = output_postingList['postingsList']
# output_dict['postingsListSkip'] = output_postingListSkip['postingsListSkip']
# output_dict['daatAnd'] = output_json_daatAnd['daatAnd']
# output_dict['daatAndSkip'] = output_json_daatAndSkip['daatAndSkip']
# output_dict['daatAndTfIdf'] = output_json_daatAnd['daatAndTfIdf']
# output_dict['daatAndSkipTfIdf'] = output_json_daatAndSkip['daatAndSkipTfIdf']
# print(output_dict)


# %%
# Flask App
@app.route("/execute_query", methods=['POST'])
def execute_query():
    
    queries = request.json["queries"]
    
    input_corpus = readInputCorpus('input_corpus.txt') #Reading Corpus
    input_corpus = convertLower(input_corpus) #LowerCase
    input_corpus_prosessed = inputCorpusPreprocessing(input_corpus) #Data Pre-processing
    postings_lists = CreateArrayPostingsLists(input_corpus_prosessed) #Array Postings Lists
    postings_linkedLists = CovertListToLinkedList(postings_lists, input_corpus_prosessed) #Converts Array Lists to Linked Lists
    # # InputCourpusValidityChecks(postings_linkedLists, postings_lists) #Validity Checks
    skip_postings_lists = SkipPostingsLists(postings_linkedLists) #Skip Postings List
    # # # skipValidityCheck(postings_linkedLists) #Validity Checks
    # queries = ['the novel coronavirus', 'from an epidemic to a pandemic', 'is hydroxychloroquine effective?', 'recent year']
    # # # queries = ["hello world", "hello swimming", "swimming going", "random swimming", "recent year"]
    processed_queries, query_map = processingQueries(queries) #Query Processing

    output_dict = {'postingsList': {},
                        'postingsListSkip': {},
                        'daatAnd': {},
                        'daatAndSkip': {},
                        'daatAndTfIdf': {},
                        'daatAndSkipTfIdf': {}}

    output_json_daatAnd, output_postingList = daatAnd(processed_queries,query_map, postings_linkedLists)

    output_json_daatAndSkip, output_postingListSkip = daatAndSkip(processed_queries, query_map, skip_postings_lists)

    output_dict['postingsList'] = output_postingList['postingsList']
    output_dict['postingsListSkip'] = output_postingListSkip['postingsListSkip']
    output_dict['daatAnd'] = output_json_daatAnd['daatAnd']
    output_dict['daatAndSkip'] = output_json_daatAndSkip['daatAndSkip']
    output_dict['daatAndTfIdf'] = output_json_daatAnd['daatAndTfIdf']
    output_dict['daatAndSkipTfIdf'] = output_json_daatAndSkip['daatAndSkipTfIdf']
    print(output_dict)


    OutJson = ast.literal_eval(str(output_dict))

    return flask.jsonify(OutJson)

# %%
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9999)


