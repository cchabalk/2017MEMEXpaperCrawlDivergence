###
# IMPORTS

#import pandas as pd
#import numpy as np
# This module is meant as a code coverage tool for the crawl divergence code
# Please issue (something like) the following from the command prompt in the same
# directory that thsi file exists to produce a code coverage report in the ./coverme
# subdirectory:
#
#    python -m trace --count --coverdir coverme ./coverage_test.py
#
# Author: Willliam Constantine
# William.Constantine@keywcorp.com

from crawlDivergenceTools import normalizeAndTrim
from compareCrawls import compareCrawls
from mainFile import generateReport
import os

if __name__=="__main__":

    data_dir = './data/lite' # this is hardwired in various modules comprising 
    c1 = 'hg_crawl_1_lite.csv'
    c2 = 'hg_crawl_2_lite.csv'
    parseType = 'type3URL' #type1URL, type2URL, type3URL, url

    normalizeAndTrim(os.path.join(data_dir, c1 + '.gz'))
    normalizeAndTrim(os.path.join(data_dir, c2 + '.gz'))
    generateReport(os.path.join(data_dir, 'clean' + c1))
    compareCrawls(os.path.join(data_dir, 'clean' + c1), os.path.join(data_dir, 'clean' + c2), parseType)


