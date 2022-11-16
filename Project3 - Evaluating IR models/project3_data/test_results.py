

import os

for core in ["BM25", "VSM"]:
    for i in range(1, 6):
        os.system("scp final@34.125.10.235:/home/final/" + core + "/" + str(i) + ".txt" + " " + "/Users/srinivas/Documents/Master_USA/Fall_2022/CSE_535_IR/Project3/chettisa_project3/" + core)