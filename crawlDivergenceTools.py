#This module contains tools and functions to assist with crawl divergence operations
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sbs
import ujson
from urlparse import urlparse
import os
import re

def detectTeam(df):
    team = 'unknown'
    columnNames = df.columns
    if 'crawler' in columnNames:
        #its probably JPL
        team = 'JPL'
    elif 'team' in columnNames:
        #its probably HG
        team = 'HG'
    else:
        print 'unknown team'
    return team


def normalizeColumnNames(team,df):
    if team == 'JPL':
        df = df.rename(columns={df.columns[0]: 'id'})
        df = df.rename(columns={df.columns[2]: 'depth'})  # discover_depth
        df = df.rename(columns={df.columns[3]: 'parentID'})
        df = df.rename(columns={df.columns[5]: 'score'})
        df = df.rename(columns={df.columns[6]: 'timestamp_fetch'})
        df = df.rename(columns={df.columns[7]: 'team'})
        df['timestamp_fetch'] = pd.to_datetime(df['timestamp_fetch'])
    if team == 'HG':
        df.loc[df['parentID'].isnull(), 'parentID'] = 'seed'
        df['timestamp_fetch'] = pd.to_datetime(df['timestamp_fetch'])
    return df

def writeADictionary(fileName,df):
    # pages per seed
    parentChildDF = df.groupby('parentID')['id'].apply(set)
    parentChildDict = parentChildDF.to_dict()

    if fileName[0] == '.':
        dictionaryName = '.' + fileName[1:].split('.')[0] + '.json'
    else:
        dictionaryName = fileName.split('.')[0] + '.json'

    with open(dictionaryName, 'w+') as fp:
        ujson.dump(parentChildDict, fp)

    return parentChildDict

def addUnique(A,B,setA):
    #Add unique elements of B to A
    repetedElements=0
    for item in B:
        if (item not in setA):
            setA.add(item)  #append to set
            A.append(item)  #append to list
        else:
            repetedElements+=1
    #if repetedElements>0:
    #    print repetedElements
    return (A,setA)

def clean_basename(x):
    return re.sub(".*clean([a-zA-Z_\-0-9]+)\.csv", r"\1", x)

def getPathFromSeed(parentIDDict, pageID):
    kk = 0
    keysToVisit = []
    currentSite = 0
    leafNode = 0
    graphPath = set()
    keysToVisit.append(pageID)
    keysToVisitSet = set()
    keysToVisitSet.update(pageID)  # shadow data structure; order not preserved
    kMax = 1600000
    while (currentSite < len(keysToVisit) and kk < kMax):
        kk += 1
        checkPage = keysToVisit[currentSite]
        currentSite += 1
        addToQueue = parentIDDict.get(checkPage, 0)
        if addToQueue != 0:
            (keysToVisit, keysToVisitSet) = addUnique(keysToVisit, addToQueue, keysToVisitSet)
            # print addToQueue
        else:
            leafNode += 1
        if kk % 100000 == 0:
            pass
            #print kk, leafNode, len(keysToVisitSet)
            # print keysToVisitSet

    return keysToVisit

def findSeeds(df):
    seeds = df[df['parentID'] == 'seed']['id'].tolist()
    seedsUnique = set(seeds)
    return seedsUnique

def removePathsOfLength1(parentChildDict,df):

    #find seeds
    seedsIDs = findSeeds(df)
    crawlPaths = []
    for pageID in seedsIDs:

        #find paths associated with each seed
        crawlPath = getPathFromSeed(parentChildDict, pageID)
        crawlPaths.append(crawlPath)

    #eliminate paths of length 1
    for p in crawlPaths:
        if len(p) == 1:
            ixx = df[df['id'] == p[0]].index
            df = df.drop(ixx)

    return df


def type1(inputURL):
    try:
        o = urlparse(inputURL)
        type1URL = o.netloc
        if len(o.netloc)>1:
            type1URL = o.netloc.split('.')[-2] + '.' + o.netloc.split('.')[-1]  #make sure this is a string
    except:
        print inputURL
        type1URL = 'null'
    return type1URL

def type2(inputURL):
    try:
        o = urlparse(inputURL)
        type2URL = o.netloc
        return type2URL
    except:
        type2URL = 'null'
    return type2URL

def type3(inputURL):
    #function to return type3 parsing
    try:
        o = urlparse(inputURL)
        type3URL = o.netloc + o.path
        return type3URL
    except:
        type3URL = 'null'
    return type3URL


def cleanLinksAndAddParseTypes(df):
    df['type1URL'] = df.url.apply(type1)
    df['type2URL'] = df.url.apply(type2)
    df['type3URL'] = df.url.apply(type3)
    return df

def saveDataFrame(fileName,df):
    exportName = fileName
    if exportName[0]=='.':
        exportName = '.' + exportName[1:].split('.')[0]
    exportName = exportName.split('/')[-1]
    exportName = os.path.join(os.path.dirname(fileName),'clean'+exportName+'.csv')
    df.to_csv(exportName)
    print 'done with ' + exportName

def writeLogFile(fileName,textString):
    with open(fileName, 'a+') as fp:
        fp.write(textString+'\n')

def normalizeAndTrim(fileName):
    df = pd.read_csv(fileName)  #read the file
    team = detectTeam(df)
    df = normalizeColumnNames(team, df)
    parentChildDict = writeADictionary(fileName,df)
    df = removePathsOfLength1(parentChildDict,df)
    df = cleanLinksAndAddParseTypes(df)
    saveDataFrame(fileName,df)

def getParentChildDict(fileName):
    dictFile = fileName.replace('clean', '').replace('csv', 'json')
    with open(dictFile, 'r') as f:
        dataIn = f.read()
    parentChildDict = ujson.loads(dataIn)
    return parentChildDict


