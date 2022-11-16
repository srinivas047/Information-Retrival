import json
import urllib.request
import urllib.parse
import subprocess
import urllib.parse
import re
import re
from urllib.parse import quote
import os

cores = ["BM25", "VSM"]
AWS_IP = "34.125.10.235"

def solrResults(queryID, query, outfile, CORE_NAME, train, queryNum):

        url = "http://" + AWS_IP + ":8983/solr/" + CORE_NAME + "/select?q=" + query + "&fl=id%2Cscore&wt=json&indent=true&rows=20"
        data = urllib.request.urlopen(url)
        
        docs = json.load(data)['response']['docs']
        rank = 1
        if CORE_NAME == "BM25":
            CORE_NAME = "bm25"
        elif CORE_NAME == "VSM":
            CORE_NAME = "vsm"
        
        for doc in docs:
            outfile.write(queryID + ' ' + 'Q' + str(queryNum) + ' ' + str(doc['id']) + ' ' + str(rank) + ' ' + str(doc['score']) + ' ' + CORE_NAME + '\n')
            rank += 1

        if not train:
            outfile.close()


def TrecExec(outfile, method, CORE_NAME):
    p = subprocess.Popen(["trec_eval", "-qcM1000 -m map", "/home/final/qrel.txt", "/home/final/" + outfile], stdout=subprocess.PIPE)
    output, err = p.communicate()

    model = CORE_NAME + '/' + CORE_NAME + '_' + method + '_eval.txt'
    trec_output = open(model,'w')
    trec_output.write(output.decode('utf-8'))
    trec_output.close()

    print("Successfully evaluated model", model)

def getResults(input, train):
    for CORE_NAME in cores:
        with open(input, encoding="utf-8") as inputFile:
            all_lines = inputFile.readlines()

            if train:
                outfile = open(CORE_NAME+ '/' + CORE_NAME+ 'parsed.txt', 'w+')

        i = 1
        for line in all_lines:
            if not train:
                outfile = open(CORE_NAME+ '/' + str(i) + '.txt', 'a')

            queryID = line[:3]
            queryText = line[4:]
            hashtags = re.findall(r"#(\w+)", queryText)
            queryText = queryText.replace(":", "\:").replace("\n", "")

            seperator = "%20OR%20"
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
   
            solrResults(queryID, queryText, outfile, CORE_NAME, train, i)
            if not train:
                outfile.close() 
                i += 1

        inputFile.close()
        print("Finished Fetching Results in Trec Format")

        if train:
            outfile.close()
            TrecExec(CORE_NAME+ '/' + CORE_NAME + 'parsed.txt' , 'train', CORE_NAME)


delete = 'rm -rf BM25 VSM'
os.system(delete)

create = 'mkdir BM25 VSM'
os.system(create)

getResults("test_queries.txt", train = False)
# getResults("queries.txt", train = True)

