import json
import urllib.request
import urllib.parse
import subprocess
import os
# import pandas as pd
import urllib.parse
# from urllib.request import urlopen
import re
from nltk.corpus import stopwords
# import nltk
from nltk.stem import PorterStemmer
# from googletrans import Translator
import re
from urllib.parse import quote
from langdetect import detect

cores = ["BM25", "VMS"]
AWS_IP = "34.125.10.235"

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

def processingQueries(query):
    # Pre Processing
    # 1. Removed special characters
    # 2. Removed excess white spaces
    # 3. Whitespace tokenization
    # 4. Removed stopwords
    # 5. Performed Porter Stemming
    # removed_text = re.sub('[^a-zA-Z.\d\s]', ' ', query)
    removing_extra_spaces = re.sub('\s{2,}', ' ', query)
    stripped = removing_extra_spaces.strip()
    tokens = re.split('\s+', stripped)
    # removed_stopwords = removeStopwords(tokens)
    # stemmed_tokens = stemming(tokens)
    
    return tokens

def solrResults(queryID, query, outfile, CORE_NAME):


        url = "http://" + AWS_IP + ":8983/solr/" + CORE_NAME + "/select?q=" + query + "&fl=id%2Cscore&wt=json&indent=true&rows=20"
    
        print(url)
        data = urllib.request.urlopen(url)
        
        docs = json.load(data)['response']['docs']
        # print(docs)

        # the ranking should start from 1 and increase
        rank = 1
        for doc in docs:
            outfile.write(queryID + ' ' + 'Q0' + ' ' + str(doc['id']) + ' ' + str(rank) + ' ' + str(doc['score']) + ' ' + CORE_NAME + '\n')
            rank += 1
        # outfile.close()

def TrecExec(outfile, method, CORE_NAME):
    # print(outfile)
    print("Running Trec command")	
    p = subprocess.Popen(["trec_eval", "-qcM1000 -m map", "/home/final/qrel.txt", "/home/final/" + outfile], stdout=subprocess.PIPE)
    # p = subprocess.Popen(["trec_eval", "-qcM1000 -m map", "/home/IR_pro1/IR_Pro3/qrel.txt", "/home/IR_pro1/IR_Pro3/test_lmoutput_2.txt"], stdout=subprocess.PIPE)
    output, err = p.communicate()

    model = CORE_NAME + '/' + CORE_NAME + '_' + method + '_eval.txt'
    trec_output = open(model,'w')
    trec_output.write(output.decode('utf-8'))
    trec_output.close()

    print("Successfully evaluated model", model)

def getResults(input):
    for CORE_NAME in cores:
        with open(input, encoding="utf-8") as inputFile:
            all_lines = inputFile.readlines()
            outfile = open(CORE_NAME+ '/' + CORE_NAME+ 'parsed_english.txt', 'a')
        for line in all_lines:
            line = line.replace("\n", "")
            queryID = line[:3]
            query = line[4:]
            query = query.replace(":", "\:")
            q_en = "(" + query + ")"
            q_en = quote(q_en)
            query_en = "text_en:"+ q_en        
            print(query_en)
            solrResults(queryID, query_en, outfile, CORE_NAME)

        outfile.close()
        TrecExec(CORE_NAME+ '/' + CORE_NAME+ 'parsed_english.txt', 'english', CORE_NAME)

def results(input):
    for CORE_NAME in cores:
        with open(input, encoding="utf-8") as inputFile:
            all_lines = inputFile.readlines()
            outfile = open(CORE_NAME+ '/' + CORE_NAME+'parsed_all.txt', 'a')
        for line in all_lines:

            # processed = processingQueries(line)
            q = line.replace("\n", "")

            queryID = q[:3]
            query = q[4:]

            query = query.replace(":", "\:")
            if query != "" or " ":
                if detect(query) == 'en':
                    q_en = "(" + query + ")"
                    q_en = quote(q_en)
                    query_en = "text_en:"+q_en
                    solrResults(queryID, query_en, outfile, CORE_NAME)
                elif detect(query) == 'de':
                    q_de = "(" + query + ")"
                    q_de = quote(q_de)
                    query_de = "text_de:"+q_de
                    solrResults(queryID, query_de, outfile, CORE_NAME)
                elif detect(query) == 'ru':
                    q_ru = "(" + query + ")"
                    q_ru = quote(q_ru)
                    query_ru = "text_de:"+q_ru
                    solrResults(queryID, query_ru, outfile, CORE_NAME)
                else:
                    q_ru = "(" + query + ")"
                    q_ru = quote(q_ru)
                    query_en = "text_en:"+q_ru
                    solrResults(queryID, query_en, outfile, CORE_NAME)
            else:
                q_en = "(" + query + ")"
                q_en = quote(q_en)
                query_en = "text_en:"+q_en
                solrResults(queryID, query_en, outfile, CORE_NAME)

        outfile.close()

        TrecExec(CORE_NAME+ '/' + CORE_NAME+'parsed_all.txt', 'allLang', CORE_NAME)

def optimised(input):
    for CORE_NAME in cores:
        with open(input, encoding="utf-8") as inputFile:
            all_lines = inputFile.readlines()
            outfile = open(CORE_NAME+ '/' + CORE_NAME+ 'parsed_optimised.txt', 'a')

        for line in all_lines:

            queryID = line[:3]
            queryText = line[4:]


            #  query = query.replace(":", "\:")
            # if query != "" or " ":
            #     if detect(query) == 'en':
            #         q_en = "(" + query + ")"
            #         q_en = quote(q_en)
            #         query_en = "text_en:"+q_en
            #         solrResults(queryID, query_en, outfile, CORE_NAME)
            #     elif detect(query) == 'de':
            #         q_de = "(" + query + ")"
            #         q_de = quote(q_de)
            #         query_de = "text_de:"+q_de
            #         solrResults(queryID, query_de, outfile, CORE_NAME)
            #     elif detect(query) == 'ru':
            #         q_ru = "(" + query + ")"
            #         q_ru = quote(q_ru)
            #         query_ru = "text_de:"+q_ru
            #         solrResults(queryID, query_ru, outfile, CORE_NAME)
            #     else:
            #         q_ru = "(" + query + ")"
            #         q_ru = quote(q_ru)
            #         query_en = "text_en:"+q_ru
            #         solrResults(queryID, query_en, outfile, CORE_NAME)
            # else:
            #     q_en = "(" + query + ")"
            #     q_en = quote(q_en)
            #     query_en = "text_en:"+q_en
            #     solrResults(queryID, query_en, outfile, CORE_NAME)

            hashtags = re.findall(r"#(\w+)", queryText)
            queryText = queryText.replace(":", "\:").replace("\n", "")
            queryText = quote(queryText)

            if hashtags:
                hashtags = '%20'.join(["tweet_hashtags%3A%20" + h for h in hashtags])
                queryText = "text_en:"+ "%28" + queryText + "%29" + hashtags
            else:
                 queryText = "text_en:"+ "%28" + queryText + "%29"      
                 
            solrResults(queryID, queryText, outfile, CORE_NAME)

        outfile.close()
        TrecExec(CORE_NAME+ '/' + CORE_NAME+ 'parsed_optimised.txt', 'optimised', CORE_NAME)

def optimised1(input):
    for CORE_NAME in cores:
        with open(input, encoding="utf-8") as inputFile:
            all_lines = inputFile.readlines()
            outfile = open(CORE_NAME+ '/' + CORE_NAME+ 'parsed_optimised.txt', 'a')

        for line in all_lines:

            queryID = line[:3]
            queryText = line[4:]
            hashtags = re.findall(r"#(\w+)", queryText)
            queryText = queryText.replace(":", "\:").replace("\n", "")
            # queryText = quote(queryText)
            seperator = "%20OR%20"

            if queryText != "" or " ":
                if detect(queryText) == 'en':
                    queryText = quote(queryText)
                elif detect(queryText) == 'de':
                    queryText = quote(queryText)
                    queryText = quote(queryText)
                else:
                    queryText = quote(queryText)
            else:
                queryText = quote(queryText)

            queryText_en = "text_txt_en:"+ "%28" + queryText + "%29"
            queryText_en1 = "text_en:"+ "%28" + queryText + "%29"
            queryText_de = "text_txt_de:"+ "%28" + queryText + "%29"
            queryText_de1 = "text_de:"+ "%28" + queryText + "%29"
            queryText_ru = "text_txt_ru:"+ "%28" + queryText + "%29"
            queryText_ru1 = "text_ru:"+ "%28" + queryText + "%29"

            if hashtags:
                hashtags = '%20'.join(["tweet_hashtags%3A%20" + h for h in hashtags])
                queryText = queryText_en + seperator + queryText_en1 +  seperator + queryText_de + seperator + queryText_de1 + seperator + queryText_ru + seperator + queryText_ru1 + hashtags
            else:
                queryText = queryText_en + seperator + queryText_en1 +  seperator + queryText_de + seperator + queryText_de1 + seperator + queryText_ru + seperator + queryText_ru1
                 
            solrResults(queryID, queryText, outfile, CORE_NAME)

        outfile.close()
        TrecExec(CORE_NAME+ '/' + CORE_NAME+ 'parsed_optimised.txt', 'optimised', CORE_NAME)

def solr(input):

    core_name = "VMS"
    # localhost = "http://ec2-13-58-105-74.us-east-2.compute.amazonaws.com:8983/solr/"
    localhost = "http://" + AWS_IP + ":8983/solr/"
    select_q = "/select?q="
    fl_score = "&fl=id%2Cscore&wt=json&indent=true&rows=20"
    inurl = ""

    with open(input, "r", encoding="utf-8") as f:
        all_queries = f.readlines()
    outf = open(core_name+ '/' + core_name+'parsed_random.txt', 'a')
    outf.truncate(0)

    for q in all_queries:
        # removing newline character from each line
        q = q.replace("\n", "")

        q_en = ""
        q_de = ""
        q_ru = ""

        qid = q[:3]
        query = q[4:]

        hashtags = re.findall(r"#(\w+)", query)
        retweets = re.findall(r"@(\w+)", query)

        query = query.replace(":", "\:")

        # with optimization
        q_en = "(" + query + ")"
        q_en = quote(q_en)
        q_de = "(" + query + ")"
        q_de = quote(q_de)
        q_ru = "(" + query + ")"
        q_ru = quote(q_ru)

        if hashtags:
            hashtags = '%20'.join(["tweet_hashtags%3A%20" + h for h in hashtags])

        if retweets:
            retweets = '%20'.join(["@" + x for x in retweets])

        or_seperator = "%20OR%20"
        boost = "&defType=dismax&qf=text_en%5E1.5%20text_de%5E1.2%20text_ru%5E0.2"
        if hashtags:
            # # without optimization
            # inurl = localhost + core_name + select_q + "text_txt_en:" + q_en + or_seperator + \
            #         "text_txt_de:" + q_de + or_seperator + "text_txt_ru:" + q_ru + or_seperator + hashtags + fl_score

            # with optimization
            inurl = localhost + core_name + select_q + "text_txt_en:" + q_en + or_seperator + "text_en:" + q_en +\
                    or_seperator + "text_txt_de:" + q_de + or_seperator + "text_de:" + q_de + or_seperator +\
                    "text_txt_ru:" + q_ru +  or_seperator + "text_ru:" + q_ru + hashtags + fl_score
            print("Has Hashtags!")
            print("QNo. {} {}".format(qid, inurl))
        else:
            # # without optimization
            # inurl = localhost + core_name + select_q + "text_txt_en:" + q_en + or_seperator + \
            #         "text_txt_de:" + q_de + or_seperator + "text_txt_ru:" + q_ru + fl_score

            # with optimization
            inurl = localhost + core_name + select_q + "text_txt_en:" + q_en + or_seperator + "text_en:" + q_en +\
                    or_seperator + "text_txt_de:" + q_de + or_seperator + "text_de:" + q_de + or_seperator +\
                    "text_txt_ru:" + q_ru + or_seperator + "text_ru:" + q_ru  + fl_score

            print("QNo. {} {}".format(qid, inurl))

        # outf = open(outfn, 'a+')
        # data = urllib2.urlopen(inurl)
        # if you're using python 3, you should use
        data = urllib.request.urlopen(inurl)
        docs = json.load(data)['response']['docs']
        # the ranking should start from 1 and increase
        rank = 1
        for doc in docs:
            outf.write(qid + ' ' + 'Q0' + ' ' + str(doc['id']) + ' ' + str(rank) + ' ' + str(doc['score']) + ' ' + core_name + '\n')
            rank += 1
    outf.close()
    TrecExec(core_name+ '/' + core_name+ 'parsed_random.txt', 'random', core_name)

# getResults("queries.txt")
# solr("queries.txt")
optimised1("queries.txt")
# results("queries.txt")
# TrecExec()
