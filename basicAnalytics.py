import datetime
import json

import pymongo
import tweepy
from pymongo import MongoClient

import numpy as np
import matplotlib.pyplot as plt
import collections

# MongoDB
mongo = MongoClient('localhost', 27017)
db = mongo['tweets']
_COLLECTIONS = ['fat', 'basic', 'enhancedStream', 'rest', 'geo']

# Counting the amount
fatCount = db.fat.find( {} ).count()
basicCount = db.basic.find( {} ).count()
enhancedCount = db.enhancedStream.find( {} ).count()
restCount = db.rest.find( {} ).count()
geoCount = db.geo.find( {} ).count()

chartinput = np.array(object=[fatCount,basicCount,enhancedCount,restCount,geoCount],dtype=int)
print(chartinput)

def plot_chart(chartinput,chart_labels):
  plt.xlabel('Collection Names')
  plt.ylabel('Records')
  plt.title('Barchart of Records for the collected data')
  plt.bar(chart_labels, chartinput, color="blue")
  # plt.xticks(rotation=90)


plot_chart(chartinput,['Total','gardenhose','EnhancedStream','REST','GeoTagged'])


# Total
# BasicStream
# EnhancedStream
# RESTCall
# GeoTagged

# GeoTagged from GLA
# Measure overlapping

# Redundant Data (by tweetID)

# Retweet & quote
