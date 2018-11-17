import datetime
import pymongo
from pymongo import MongoClient
import tweepy
from tweepy import TweepError,RateLimitError,StreamListener
import json
import traceback


consumer_key = "VIGGAj16rqqaEOEEJjVWaROpi"
consumer_secret = "HAqUGIRXMvRDkDTdH2fJ1pxsjoRARbepz5rsSeHJWYl2eve1Bl"
access_token = "969038491706363905-VFcCIGYkGRAeG6CPh1kGCPbj8fiHXwo"
access_token_secret = "SkEve79Dkoha1r7wVn34VLyHLendlenfAmemKOpBjxdow"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)#,parser=tweepy.parsers.JSONParser())

mongo = MongoClient('localhost', 27017)
streamDb = mongo['tweet']['enhancedStream']
restDb = mongo['tweet']['rest']

class MyEnhancedStream(tweepy.StreamListener):
    def on_data(self, data):
        try:
            tweet = json.loads(data)
            streamDb.insert_one(tweet)
            print("streamDb---inserted")
        except TweepError:
            print("TweepError")
        except RateLimitError:
            print("RateLimitError")
        except Exception:
            print(traceback.format_exc())
class MyRESTProbe():
    def on_data(self,data):
        try:
            tweet = json.loads(data)
            restDb.insert_one(tweet)
            print("restDb---inserted")
        except TweepError:
            print("TweepError")
        except RateLimitError:
            print("RateLimitError")
        except Exception:
            print(traceback.format_exc())
def _main():
    print("Done Importing & Config")

    # myStreamListener = MyEnhancedStream()
    # myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
    # # myStream.track(languages = ['en'],track=['mariahcarey'],async=True)
    # myStream.filter(languages = ['en'],track=['mariahcarey'])

    print("Starting REST")

    #restTweets = api.search(params)
    myrestProbe = api.search(q='mariahcarey',languages = ['en'],rpp=1500,pages= 1500)
    for tweet in myrestProbe:
        try:
            data = json.loads(json.dumps(tweet._json))
            restDb.insert_one(data)
            print("restDb---inserted")
            # print(json.dumps(tweet._json))
        except TweepError:
            print("TweepError")
        except RateLimitError:
            print("RateLimitError")
        except Exception:
            print(traceback.format_exc())


_main()
