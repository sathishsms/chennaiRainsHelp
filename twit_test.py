#! /usr/bin/python
#===============================================================================
# File Name      : twit_test.py
# Date           : 12-02-2015
# Input Files    : Nil
# Author         : Satheesh <sathishsms@gmail.com>
# Description    :
# How to run     :twit_test.py -l info
#                :twit_test.py -h
#===============================================================================
import time
import sys
import argparse
import json
sys.path.append('../wowgic_dev')
import globalS
import generic
import loggerRecord
import neo4jInterface

####
# Get tweepy set up
import tweepy
from tweepy import Cursor

#parse the run-time args passed
parser = argparse.ArgumentParser(description='  To get the mra.log,rc.log,\
        qpTraces & tcpdump to the log viewer machine or any user defined server\
        all in one place with a single click. Works among multiple Active Pairs \
        (MPE\'s, MRA\'s)..................................................\
        Example: ./loggy CAM-92410 -c serverRack_C6GRsetup.cfg or ./loggy \
        CAM-92410 or ./loggy -v ',add_help=True)
#parser.add_argument('testName',help='Name suffixed to log file name generated')
#if the def file is not passed as arg thn take the default file.
parser.add_argument('-c', '--config',default='serverRack.def',help='definition file')
parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.3')
parser.add_argument("-l", "--logLevel",default='error',help="Enable standard output verbosity")
args = parser.parse_args()

############################################################################
#Function Name  : compileFileName                                          #
#Input          : Nil                                                      #
#Return Value   : just sets the suffix fileName for logs                   #
############################################################################
def compileFileName():
    dayTime      = generic.dateTimeFields()
    dayTime     += '_'+'wowgic'
    return dayTime
############################################################################

globalS.init()#intialize the global variables
generic      = generic.generic()

#==============================================================================#
#           Opening log file to record all the cli outputs                     #
#==============================================================================#
sufFileName = compileFileName()
logFileName  = "/tmp/" + sufFileName + ".log"
logger       = loggerRecord.loggerInit(logFileName,args.logLevel)
logger.debug('Log file# %s & TestBed file ',logFileName)

#keys from twitter is stored here temp will be removed once we access the user credentials
#chella's credentials
consumer_key= 'HwvpHtsPt3LmOZocZXwtn72Zv';
consumer_secret = 'afVEAR0Ri3ZluVItqbDi0kfm7BHSxjwRXbpw9m9kFhXGjnzHKh';
access_token = '419412786-cpS2hDmR6cuIf8BD2kSSri0BAWAmXBA3pzcB56Pw';
access_secret = 'pRx5MNKkmxyImwuhUFMNVOr1NrAWcRmOGUgGTLVYFAjsJ';

#authenticate twitter app
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

neo4jInt = neo4jInterface.neo4jInterface()
graphDB=neo4jInt.connect()
#graphDB=neo4jInt.connect('localhost:7474/db/data/','neo4j','admin')
####
# End of boilerplate, interesting code starts here:


#facbook tmp input given by chella ltr retrive from app and give as inpu to this variable
####
#Satheesh
jsonFBInput = '{"id":"1240560189303114","name":"Mari Satheesh","hometown":{"id":"106076206097781","name":"Madurai, India"},"location":{"id":"106377336067638","name":"Bangalore, India"},"education":[{"school":{"id":"135521326484377","name":"Cathy Matriculationn Higher Secondary School"},"type":"High School"},{"school":{"id":"131854716845812","name":"KLN College of Engineering"},"type":"College"},{"school":{"id":"112188602140934","name":"kln"},"type":"College"}],"work":[{"employer":{"id":"114041451939962","name":"Sonus Networks"}}]}'
####
#Vivek Su
#jsonFBInput ='{"id":"858104450925382","name":"Vivek Subburaju","hometown":{"id":"106076206097781","name":"Madurai, India"},"location":{"id":"106078429431815","name":"London, United Kingdom"},"education":[{"school":{"id":"140607792619596","name":"Mahatma Montessori Matriculation Higher Secondary School"},"type":"High School"},{"school":{"id":"6449932074","name":"Royal Holloway, University of London"},"type":"College"},{"concentration":[{"id":"105415696160112","name":"International Business"}],"school":{"id":"107951082570918","name":"LIBA"},"type":"College","year":{"id":"144044875610606","name":"2011"}},{"school":{"id":"107927999241155","name":"Loyola College Chennai"},"type":"College","year":{"id":"137616982934053","name":"2006"}}],"work":[{"employer":{"id":"400618623480960","name":"Onestep Solutions Debt Recovery Software"},"position":{"id":"1002495616484486","name":"Principal Consultant- Data Quality"},"start_date":"2015-12-15"},{"end_date":"2014-12-31","employer":{"id":"134577187146","name":"Cognizant"},"start_date":"2013-01-01"},{"end_date":"2013-01-01","employer":{"id":"177419101744","name":"Pearson English Business Solutions"},"location":{"id":"102186159822587","name":"Chennai, India"},"start_date":"2011-01-01"},{"end_date":"2011-01-01","employer":{"id":"108134792547341","name":"Tata Consultancy Services"},"start_date":"2008-01-01"},{"end_date":"2008-01-01","employer":{"id":"42189185115","name":"Wipro"},"start_date":"2006-01-01"}]}'
decodedFBJson=json.loads(jsonFBInput)
logger.info("updaed Json from FB: %s", jsonFBInput)

#neo4jInt.createConstraint(graphDB)
#neo4jInt.createUserNode(graphDB,decodedFBJson,'user')
#for int in ['hometown','location','work','education']
    #neo4jInt.createInterestNode(graphDB,decodedFBJson,int)

from instagram.client import InstagramAPI

client_id = 'XXXXX'
client_secret = 'XXXXX'
access_token = 'XXXXXXX'
client_ip = 'YOUR PUBLIC IP'
api = InstagramAPI(client_id=client_id, client_secret=client_secret,client_ips= client_ip,access_token= access_token)

logger.info("Execution id done")
