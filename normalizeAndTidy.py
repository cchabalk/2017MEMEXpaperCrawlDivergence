#python file to normalize the data frames and remove crawl paths of length 1

from crawlDivergenceTools import normalizeAndTrim



if __name__ == '__main__':

#   fileName = './data/hg_crawl_1.csv.gz'
#   fileName = './data/hg_crawl_3.csv.gz'
#   fileName = './data/jpl_sparkler_crawl1.csv.gz'
   fileName = './data/jpl_sparkler_crawl2.csv.gz'

   normalizeAndTrim(fileName)
