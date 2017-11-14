import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sbs
from crawlDivergenceTools import *
import os

def computeOverlapOverTime(aName,A,bName,B,parseType):

    #compute overlap over time
    # sort by time
    A.sort_values('timestamp_fetch', inplace=True)
    B.sort_values('timestamp_fetch', inplace=True)

    #extract sites
    sitesA = A[parseType]
    sitesB = B[parseType]

    #
    nA = A['timestamp_fetch'].count()
    nB = B['timestamp_fetch'].count()

    #this should go based on percentages of total amount
    kMax = min(nA,nB)
    kSections = 100 #chunk into 1000 sections
    upperIndex = np.linspace(1,kMax,kSections,dtype=np.int32)

    nMatch = []
    percentMatch = []
    chunkSize = []
    for i in upperIndex:
        setA = set(A[parseType].iloc[0:i]) #get 1st N unique pages in A
        setB = set(B[parseType].iloc[0:i]) #get 1st N unique pages in B
        totalPossible = min(len(setA),len(setB))
        NN = len(set(setA).intersection(setB)) #find intersection
        nMatch.append(NN) #store
        percentMatch.append(float(NN)/float(totalPossible)*100.) #store as a percentage
        chunkSize.append(totalPossible)

    combined_name = clean_basename(aName) + clean_basename(bName)
    #aName = aName.split('/')[-1]
    #bName = bName.split('/')[-1]
    aName = os.path.basename(aName)
    bName = os.path.basename(bName)

    plt.figure()
    plt.plot(chunkSize,nMatch,'k')
    plt.xlabel('1st N unique pages')
    plt.ylabel('N pages overlap')
    plt.title(aName + ' & ' + bName + '; ' + parseType)

    #figName = aName.replace('./data/clean', '').replace('.csv','') + bName.replace('./data/clean', '').replace('.csv','') + 'OverlapN' + parseType + '.png'
    figName = combined_name + 'OverlapN' + parseType + '.png'
    
    plt.savefig(figName, bbox_inches='tight')
    plt.close()

    plt.figure()
    plt.plot(chunkSize,percentMatch,'k')
    plt.xlabel('1st N unique pages')
    plt.ylabel('% pages overlap')
    plt.title(aName + ' & ' + bName + '; ' + parseType)

    #figName = aName.replace('./data/clean', '').replace('.csv','') + bName.replace('./data/clean', '').replace('.csv','') + 'OverlapP' + parseType + '.png'
    figName = combined_name + 'OverlapP' + parseType + '.png'
    
    plt.savefig(figName, bbox_inches='tight')
    plt.close()

def computeOverlapPerSeed(c1,A,c2,B,parseType):

    #get seeds
    seedsA = list(findSeeds(A))
    seedURLs = list(set(A[A['id'].isin(seedsA)][parseType].tolist()))
    seedURLs.sort()  #get them in alphabetical order
    seedsA = [A[A[parseType]==i]['id'].iloc[0] for i in seedURLs]  #seed id's with URL's in alphabetical order
    seedsB = list(findSeeds(B))

    pcDictA = getParentChildDict(c1)
    pcDictB = getParentChildDict(c2)

    # needs to get seeds
    crawlPathA = []
    crawlPathB = []
    for singleSeed in seedsA:

        try:

            #find unique ID associated with seed in A and B
            #The seed may appear many times
            #We should check the crawl paths for the longest chain

            #The

            seedIDA = singleSeed
            URL = A[A['id'] == seedIDA][parseType].iloc[0]  #get the URL
            seedIDB = B[B[parseType] == URL]['id']  #get the ID associated with that URL
            print str(len(B[B[parseType] == URL]['id'])) + " ID's for seed " + str(URL)

            #convert unique id's to url's
            tt = getPathFromSeed(pcDictA, seedIDA)
            urlList = A.loc[A['id'].isin(tt)][parseType].tolist()
            crawlPathA.append(urlList)

            # convert unique id's to url's
            #get all paths for that ID
            #keep the longest
            tt = []
            for i in seedIDB:
                ttNew = getPathFromSeed(pcDictB, i)
                if len(ttNew) > len(tt):
                    tt = ttNew
                    print "length of new path " + str(len(ttNew))

            urlList = B.loc[B['id'].isin(tt)][parseType].tolist()
            crawlPathB.append(urlList)

        except (IndexError):
            pass

    nA = len(crawlPathA)  # # of crawls
    C = np.zeros((nA,nA),dtype=np.int32)
    i = 0
    for l in crawlPathA:
        j = 0
        for m in crawlPathB:
            C[i,j] = len(set(l).intersection(set(m)))
            j+=1
        i+=1
    print "matrix of overlap between crawl A & B.  Begin with a seed, compute overlap with other crawl's seed path"
    print "scan down matrix; scan down seedsA; scan across matrix; scan across pages in B"

    seedURLs = [A[A['id'] == i][parseType].iloc[0] for i in seedsA]  # get the URL

    for i in seedURLs:
        print i

    df1 = pd.DataFrame(C)
    df1.insert(0, 'url', seedURLs)
    print df1

    #reportName = c1.replace('./data/clean', '').replace('.csv','') + c2.replace('./data/clean', '').replace('.csv','') + 'OverlapPerSeed' + parseType + '.csv'
    reportName = clean_basename(c1) + clean_basename(c2) + 'OverlapPerSeed' + parseType + '.csv'

    with open(reportName,'w') as fp:
        df1.to_csv(fp, sep=',')

    return (crawlPathA,crawlPathB)


def compareCrawls(c1,c2,parseType):

    #load the crawls
    A = pd.read_csv(c1)  #read the file
    B = pd.read_csv(c2)  #read the file

    #parse to type1, type2, type3
    computeOverlapOverTime(c1,A,c2,B,parseType)
    computeOverlapPerSeed(c1,A,c2,B,parseType)



if __name__ == "__main__":

#need to do this for each seed; need to match seeds across crawls

    c1 = './data/cleanhg_crawl_1.csv'
    c2 = './data/cleanhg_crawl_3.csv'
    c3 = './data/cleanjpl_sparkler_crawl1.csv'
    c4 = './data/cleanjpl_sparkler_crawl2.csv'

    allCrawls = [c1, c2, c3, c4]

    parseType = 'type3URL' #type1URL, type2URL, type3URL, url

    #compareCrawls(c1,c1,parseType)

    #endCode

    for i in allCrawls:
        for j in allCrawls:
            if True: #i != j:
                compareCrawls(i,j,parseType)

    print "done"
