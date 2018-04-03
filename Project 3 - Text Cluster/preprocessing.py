'''
Created on Mar 17, 2018

@author: 986027
'''

import os

def get_files(path):
    files_list = []
    for root, dirs, files in os.walk(path):
        for file in files: 
            files_list.append((file,root +"/"+ file))
    return files_list


def read_text(filename):
    file = open(filename) 
    
    text = file.read().splitlines()
    return " ".join(text).replace('"', ' ').replace(',', ' ')

path = "C:/Users/986027/Downloads/Project 3 - Text Cluster/C50/C50train/"

output = open("kmeans_input.json", "w") 

files = get_files(path)
for i in range(len(files)):
    label = files[i][0].replace("newsML.txt", "")
    f = files[i][1]
    txt = read_text(f)
    json = "\"id\":\"{}\", \"label\":\"{}\", \"text\":\"{}\"  "
    output.write("{"+json.format(str(i), label, txt)+"}\n")
 

