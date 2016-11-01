#!/usr/bin/python
import json
import re
import sys
import tweepy
import mmap
import string
sys.path.append('../wowgic_dev')
from py2neo import *
#import neo4jInterface

#connect our DB
#neo4jInt = neo4jInterface.neo4jInterface()
##graph_db = neo4j.GraphDatabaseService("https://564c60239913d:D5B3YxJFXqH9FttuIQWIpUn9HWpSQpZCfXi0HyXi@neo-graciela-stracke-cornsilk-564c5f886175e.do-stories.graphstory.com:7473/db/data/")
#
##graphDB=neo4jInt.connect('localhost:7474/db/data/','neo4j','admin')
#graphDB=neo4jInt.connect()


consumer_key= '94TZfFIFX9NsrU9sidwSg7OzE';
consumer_secret = 'NckyU9mnFVCsKs4OG8RBda76ibkMaiMwYoMm9IujnDcW6cgEiO';
access_token = '4464323965-ZxpmaftKrlETVCMhL9mFO6oKL4QCSqeyZoNxSf3';
access_secret = 'iMPBbsjgiWkY4eM4C7Nxq2Ky8hlwQn9NrnKHV06cnYf8A';

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

####
#read the file of dictionary words and load into a python dict
dictfile = 'egdict.txt'
a = open(dictfile)
lines = a.readlines()
a.close()
dic = {}

# Open a file
fp_tweetId = open("tweet_ids.txt", "a+")
# a default category for simple word lists
current_category = "Default"
# inhale the dictionary
for line in lines:
    if line[0:2] == '>>':
        current_category = string.strip( line[2:] )
        dic[current_category]= []
        tmp =[]
        #print current_category
    else:
        line = line.strip()
        if len(line) > 0:
            #print line
            tmp.append(line)
            dic[current_category]= tmp
#### End of Parsing file###

from neo4jrestclient.client import GraphDatabase

gdb = GraphDatabase("http://wowgic.sb02.stations.graphenedb.com:24789/db/data/",
                    username="Wowgic",
                    password="GcpXosEMPJV5pLR0QJQ3")

api = tweepy.API(auth)
def categoryMaterialize(text):
    tmpScrName=""
    for cate in dic.get('cCategory'):
        cate=cate.lower()
        for area in dic.get('Areas'):
            area=area.lower()
            q = 'MATCH (h:aHelper{area_place:\''+area+'\'})-[:PROVIDES]->(b{name:\''+cate+'\'})<-[:seeker]-(s:Seeker{area_place:\''+area+'\'}) return h,s,b'
            #print q
            # Send Cypher query.
            n = gdb.query(q,data_contents=True)
            if len(n):
                for i in n.rows:
                    #print i
                    h=i[0]
                    t=i[1]
                    c=i[2]
                    #print t['id'],t['screen_name'],h['screen_name']
                    #tweetStatus = 0
                    print i
                    tweetStatus='@'+t['screen_name']+' for '+ c['name']+'in location'+h['area_place']+ 'try contacting @'+h['screen_name']
                    s = mmap.mmap(fp_tweetId.fileno(), 0, access=mmap.ACCESS_READ )
                    tmpStr=str(t['id'])
                    tStr = str(h['id'])
                    #print tmpStr
                    if s.find(tmpStr) == -1 & s.find(tStr) == -1:
                        try:
                            #print "satheeshIF"
                            print tweetStatus
                            if re.search(tmpScrName,tweetStatus,re.I|re.L) == None:
                                #st = api.update_status(tweetStatus,in_reply_to_status_id=tmpStr)
                                #print st
                                tmpStr = tmpStr+'\n'
                                fp_tweetId.write(tmpStr)
                                tStr = tStr+'\n'
                                fp_tweetId.write(tStr)
                                fp_tweetId.flush()
                                #return 0
                            else:
                                print 'screenB'
                        except tweepy.TweepError,E:
                            print "not tweeted"
                            print E
                            pass
                    else:
                        pass
                        #print "satheeshElSE"
                    fp_tweetId.flush()
                    q1 = 'Match (n{id:'+tmpStr+'})-[r]-() Delete r,n'
                    n = gdb.query(q1,data_contents=True)
                    q2 = 'Match (n{id:'+tStr+'})-[r]-() Delete r,n'
                    n = gdb.query(q2,data_contents=True)
                    tmpScrName =t['screen_name']

categoryMaterialize('satheesh')
fp_tweetId.close()
