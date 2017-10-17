import pandas as pd
import numpy as np
import ujson
from matplotlib import pyplot as plt
plt.rcParams["font.family"] = "Times New Roman"
import seaborn as sbs
from crawlDivergenceTools import writeLogFile


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






def generateReport(fileName):


    reportName = fileName.replace('./data/clean', '').replace('.csv', '') + 'report.txt'

    logStatement = "Analyzing file: " + fileName
    print logStatement
    writeLogFile(reportName,logStatement)

    df = pd.read_csv(fileName)  #read the file


    ############################
    #count seeds & unique seeds#
    ############################
    seeds = df[df['parentID'] == 'seed']['id'].tolist()

    seedsUnique = set(seeds)
    nSeeds = len(seeds)
    nSeedsUnique = len(list(set(df[df['parentID'] == 'seed' ]['url'].tolist())))

    logStatement = "Seeds: contains " + str(nSeeds) + "; with " + str(nSeedsUnique) +  " unique "
    print logStatement
    writeLogFile(reportName, logStatement)
    logStatement = "They are: "
    print logStatement
    writeLogFile(reportName, logStatement)

    seedURLs = list(set(df[df['parentID'] == 'seed' ]['url'].tolist()))
    seedURLs.sort()

    for (k,i) in enumerate(seedURLs):
        isUnique = 'unique'
        if (i in seedURLs[k+1:]) and (k+1 < len(seedURLs)):
            isUnique = 'repete'

        logStatement = str(k) + " " + isUnique + " " + str(i)
        print logStatement
        writeLogFile(reportName, logStatement)


    ##############################################
    #number of pages and unique pages unique URLs#
    ##############################################
    nPages = len(df['id'])
    nPagesUnique = len(df['id'].unique())

    logStatement =  "File contains " +str(nPages) + " with " + str(nPagesUnique) + " unique page IDs"
    print logStatement
    writeLogFile(reportName, logStatement)

    nParentPages = len(df['parentID'])
    nParentPagesUnique = len(df['parentID'].unique())

    logStatement = "File contains " + str(nParentPages) + " with " + str(nParentPagesUnique) + " unique parent page IDs"
    print logStatement
    writeLogFile(reportName, logStatement)

    nURLsUnique = len(df['url'].unique())
    logStatement = "File contains " + str(nURLsUnique) + " unique RAW page URLs"
    print logStatement
    writeLogFile(reportName, logStatement)

    nURLsUnique = len(df['type1URL'].unique())
    logStatement = "File contains " + str(nURLsUnique) + " unique type1 page URLs"
    print logStatement
    writeLogFile(reportName, logStatement)

    nURLsUnique = len(df['type2URL'].unique())
    logStatement = "File contains " + str(nURLsUnique) + " unique type2 page URLs"
    print logStatement
    writeLogFile(reportName, logStatement)

    nURLsUnique = len(df['type3URL'].unique())
    logStatement = "File contains " + str(nURLsUnique) + " unique type3 page URLs"
    print logStatement
    writeLogFile(reportName, logStatement)









    if True:

        #pages per depth
        a1 = df['depth'].hist() #a1 is an mpl subplot axis
        a1.set_ylabel('# pages')
        a1.set_xlabel('crawl depth')
        a1.set_title(fileName)

        a1.set_yscale('log')
        figName = fileName.replace('./data/clean', '').replace('.csv','') + 'PagesPerDepth' + '.png'
        plt.savefig(figName, bbox_inches='tight')

        plt.interactive(False)
        plt.close()

        set2 = df.groupby('depth').size()

        plt.semilogy(set2, 'k.')
        plt.xlabel('crawl depth')
        plt.ylabel('# pages')
        plt.title(fileName)
        figName = fileName.replace('./data/clean', '').replace('.csv','') + 'PagesPerDepthLog' + '.png'
        plt.savefig(figName, bbox_inches='tight')

        plt.interactive(False)
        plt.close()

        #pages per seed
        #load the dictionary
        dictFile = fileName.replace('clean', '').replace('csv','json')
        with open(dictFile,'r') as f:
            dataIn = f.read()

        parentChildDict = ujson.loads(dataIn)

        #need to get seeds - get max length or sum lengths
        #some url's are associated with multiple unique id's
        crawlPath = []
        for singleSeed in seedURLs:
            seedID = df[df['url']==singleSeed]['id']
            tt = []
            for i in seedID:
                ttNew = getPathFromSeed(parentChildDict, i)
                tt += ttNew
            #convert to set(URLs)
            tt = list(set(df[df['id'].isin(list(set(tt)))]['type3URL'].tolist()))
            crawlPath.append(list(set(tt)))

        seedCrawlLength = [len(i) for i in crawlPath]
        for k,i in enumerate(seedURLs):
            logStatement = "seed: " + i + " contains " + str(seedCrawlLength[k]) + " unique type3 urls"
            print logStatement
            writeLogFile(reportName, logStatement)


        plt.bar(range(len(seedCrawlLength)),seedCrawlLength) #probably should sort this
        plt.xlabel('seed #')
        plt.ylabel('crawl length')
        plt.yscale('log')

        title = fileName + '; crawl length/seed'
        plt.title(title)
        figName = fileName.replace('./data/clean', '').replace('.csv','') + 'crawlLengthPerSeed' + '.png'

        plt.interactive(False)
        plt.savefig(figName,bbox_inches='tight')
        plt.close()

        crawlPathTotalPages = sum(seedCrawlLength)
        logStatement = "total unique pages: " + str(nPagesUnique) +  "; total pages in all crawlPaths: " + str(crawlPathTotalPages)
        print logStatement
        writeLogFile(reportName, logStatement)

        #plot pages over time
        N = df['timestamp_fetch'].count()  #count non null values
        df.sort_values('timestamp_fetch',inplace=True)
        #plot non-null values
        plt.plot(pd.to_datetime(df[pd.notnull(df['timestamp_fetch'])]['timestamp_fetch']),np.arange(N),'k')
        plt.xlabel('time')
        plt.ylabel('# pages')
        plt.title(fileName + '; # pages / time')
        figName = fileName.replace('./data/clean', '').replace('.csv','') + 'pagesPerTime' + '.png'

        plt.interactive(False)
        plt.savefig(figName,bbox_inches='tight')
        plt.close()

        print 'here'


if __name__=="__main__":
    #fileName = './data/cleanhg_crawl_1.csv'
    #fileName = './data/cleanhg_crawl_3.csv'


    #fileName = './data/cleanjpl_sparkler_crawl1.csv'
    fileName = './data/cleanjpl_sparkler_crawl2.csv'
    generateReport(fileName)


#to do:
#List seeds in length per seed
#Alphebatize seeds?
#Note which data sets contain seeds from original seed file and which do not

