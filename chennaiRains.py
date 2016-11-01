#! /usr/bin/python
import time
import sys
import string
import json
import re
import tweepy
sys.path.append('../wowgic_dev')
import globalS
from tweepy import *
from py2neo import *
import neo4jInterface
from textblob import TextBlob
from tweepy.streaming import StreamListener

consumer_key= 'HwvpHtsPt3LmOZocZXwtn72Zv';
consumer_secret = 'afVEAR0Ri3ZluVItqbDi0kfm7BHSxjwRXbpw9m9kFhXGjnzHKh';
access_token = '419412786-cpS2hDmR6cuIf8BD2kSSri0BAWAmXBA3pzcB56Pw';
access_secret = 'pRx5MNKkmxyImwuhUFMNVOr1NrAWcRmOGUgGTLVYFAjsJ';
dictDb = {}
#connect our DB
neo4jInt = neo4jInterface.neo4jInterface()
#graph_db = neo4j.GraphDatabaseService("https://564c60239913d:D5B3YxJFXqH9FttuIQWIpUn9HWpSQpZCfXi0HyXi@neo-graciela-stracke-cornsilk-564c5f886175e.do-stories.graphstory.com:7473/db/data/")

#graphDB=neo4jInt.connect('localhost:7474/db/data/','neo4j','admin')
graphDB=neo4jInt.connect()

try:
    #neo4J create a unique for a node only then a create operation will be successfull
    graphDB.cypher.execute("""CREATE CONSTRAINT ON (n:name) ASSERT n.id IS UNIQUE """)
    graphDB.cypher.execute("""CREATE CONSTRAINT ON (n:category) ASSERT n.name IS UNIQUE """)
    pass
except:
    pass

####
#read the file of dictionary words and load into a python dict
dictfile = 'egdict.txt'
a = open(dictfile)
lines = a.readlines()
a.close()
dic = {}

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

def creatCategoryNode():
        labels=['category']
        print dic.get('cCategory')
        for pattern in dic.get('cCategory'):
            #print("pattern:",pattern)
            create_node_query = """
            WITH {pattern} AS p
            Merge (n:category{name:p})     RETURN n   """
            # Send Cypher query.
            try:
                n=graphDB.cypher.execute(create_node_query,pattern=pattern.lower())
            except:
                pass

def creatPlaceNode():
        #print dic.get('Areas')
        for pattern in dic.get('Areas'):
            #print("pattern:",pattern)
            create_node_query = """
            WITH {pattern} AS p
            Merge (n:area{name:p})     RETURN n   """
            # Send Cypher query.
            try:
                n=graphDB.cypher.execute(create_node_query,pattern=pattern.lower())
            except:
                pass
#create category node
creatCategoryNode()
#create category place
creatPlaceNode()

class listener(StreamListener):
    def categoryMaterialize(self,text):
        material =[]
        for pattern in dic.get('cCategory'):
            if re.search(pattern,text,re.I|re.L):
               material.append(pattern.lower())
        if len(material) <=0:
            material =['volunteer']
        return tuple(material)
        #return material

    def locationAreas(self,text):
        area='openarea'
        print text
        for pattern in dic.get('Areas'):
            if re.search(pattern,text,re.I):
                area= pattern.lower()
                print("pattern IF",text)
                break
        return area

    def creatNode(self,data_json,lbl):
        labels=[]
        labels.append(lbl)
        add_tweet_query = """
        WITH {data_json} AS data
        UNWIND data AS t
        MERGE (u {id:t.id})
            ON CREATE SET
             """+   (('u:'+',u:'.join(labels)+",") if labels else '') +"""
                u.typesh=t.typesh,
                u.screen_name=t.user.screen_name,
                u.id=t.id,
                u.created_at=t.created_at,
                u.text=t.text,
                u.category=t.category,
                u.area_place=t.area_place,
                u.location=t.location,
                u.time_local=t.localTime,
                u.profile_image_url=t.profile_image_url,
                u.geo_enabled=t.geo_enabled
            """ +   (("ON MATCH SET\n  u:"+',u:'.join(labels)) if labels else '') +"""
            RETURN u
        """
        # Send Cypher query.
        graphDB.cypher.execute(add_tweet_query,data_json=data_json)

        relation_query = """
        with {category} as rc
        MATCH (h:aHelper {category:filter(x IN h.category WHERE rc in h.category)}), (c:category {name:rc}) MERGE (h)-[:PROVIDES]->(c);"""
        # Send Cypher query.
        for pattern in dic.get('cCategory'):
            graphDB.cypher.execute(relation_query,category=pattern.lower())

        relation_query2 = """
        with {category} as rc
        MATCH (h:Seeker {category:filter(x IN h.category WHERE rc in h.category)}), (c:category {name:rc}) MERGE (h)-[:seeker]->(c);"""
        # Send Cypher query.
        for pattern in dic.get('cCategory'):
            graphDB.cypher.execute(relation_query2,category=pattern.lower())

        relation_query3 = """
        with {category} as ar
        MATCH (h {area_place:ar}), (l:area {name:ar}) MERGE (h)-[:LOCATED_AT]->(l);"""
        # Send Cypher query.
        #print"trigger area relationship"
        for pattern in dic.get('Areas'):
            graphDB.cypher.execute(relation_query3,category=pattern.lower())

    def on_data(self, data):
        data = json.loads(data)
        if 'RT @' in data['text'] or data['retweeted']:
            #print('Skip: retweeted is', data['retweeted'])
            return 0
            dummy = 0
        else:
            inputTweetCount = 1
            #print('through: retweeted is', data['retweeted'])
        text = data['text']
        text = text.encode('utf-8') # lowercase the text
        #print dic
        for key in dic.keys():
            if key=='cCategory' or key =='Areas':
                break
            for pattern in dic.get(key):
                #print ("pattern :%s",pattern)
                if re.search(pattern,text,re.I|re.L):
                   #print "satheesh"
                   data[unicode('typesh')]=key
                   #print text
                   #print key
                   catList= self.categoryMaterialize(text)
                   area_place= self.locationAreas(text)
                   #print "satheesh2"
                   print (catList,area_place)
                   data[unicode('category')]=catList
                   data[unicode('localTime')]=time.time()
                   data[unicode('area_place')]=area_place
                   self.creatNode(data,key)
                   #print(json.dumps(data))
                   return 0


    def on_error(self, status):
        print status

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["#chennaiRainsHelp","#Chennaivolunteer","#chennairescue"])
