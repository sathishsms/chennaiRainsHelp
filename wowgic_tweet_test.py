#wowgic_tweet_test
import json
import pymongo
import falcon
import tweepy
from tweepy import OAuthHandler

connection = pymongo.MongoClient("localhost", 27017)
db = connection.feeds
consumer_key= 'HwvpHtsPt3LmOZocZXwtn72Zv';
consumer_secret = 'afVEAR0Ri3ZluVItqbDi0kfm7BHSxjwRXbpw9m9kFhXGjnzHKh';
access_token = '419412786-cpS2hDmR6cuIf8BD2kSSri0BAWAmXBA3pzcB56Pw';
access_secret = 'pRx5MNKkmxyImwuhUFMNVOr1NrAWcRmOGUgGTLVYFAjsJ';

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api=tweepy.API(auth)

#words = ["is"]
#with tweetstream.FilterStream(username, password, track=words) as stream:
#    for tweet in stream:
#        db.tweets.save(tweet)
#for status in tweepy.Cursor(api.home_timeline).items(10):
#   print (status)
#   #db.tweets.insert(json.loads(str(status)))


class ThingsResource:
    def on_get(self, req, resp):
        """Handles GET requests"""
        tweets = api.user_timeline(count = 10)
        resp.status = falcon.HTTP_200  # This is the default status
        for tweet in tweets:
            print "\n\n tweet._json\n\n"
            tweet = json.dumps(tweet._json)
            db.sample_feeds.insert(json.loads(tweet))
            resp.body = tweet



# falcon.API instances are callable WSGI apps
app = falcon.API()

tweets = ThingsResource()
# things will handle all requests to the '/things' URL path
app.add_route('/wowgic_tweet_test', tweets)
