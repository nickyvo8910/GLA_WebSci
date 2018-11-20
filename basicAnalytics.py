import collections
import datetime
import json

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymongo
import tweepy
from pymongo import MongoClient

# MongoDB
mongo = MongoClient('localhost', 27017)
db = mongo['tweet']
_COLLECTIONS = ['fat', 'basic', 'enhancedStream', 'rest', 'geo']


# Plotting functions

def plot_chart(chartinput, chartlabels, title, xLabel, yLabel):

    chartinput = pd.Series.from_array(chartinput)

    # Plot the figure.
    plt.figure(figsize=(12, 8))
    ax = chartinput.plot(kind='bar', width=1.0, color='green')
    ax.set_title(title)
    ax.set_xlabel(xLabel)
    ax.set_ylabel(yLabel)
    ax.set_xticklabels(chartlabels)

    rects = ax.patches

    # For each bar: Place a label
    for rect in rects:
        # Get X and Y placement of label from rect.
        y_value = rect.get_height()
        x_value = rect.get_x() + rect.get_width() / 2

        # Number of points between bar and label. Change to your liking.
        space = 5
        # Vertical alignment for positive values
        va = 'bottom'

        # If value of bar is negative: Place label below bar
        if y_value < 0:
            # Invert space to place label below
            space *= -1
            # Vertically align label at top
            va = 'top'

        # Use Y value as label and format number with one decimal place
        label = "{:.1f}".format(y_value)

        # Create annotation
        plt.annotate(
            label,                      # Use `label` as label
            (x_value, y_value),         # Place label at end of the bar
            xytext=(0, space),          # Vertically shift label by `space`
            textcoords="offset points",  # Interpret `xytext` as offset in points
            ha='center',                # Horizontally center label
            va=va)                      # Vertically align label differently for
        # positive and negative values.

    # plt.savefig("image.png")
    plt.show()

# Counting the amount


initTime = db.basic.find({}).__getitem__(0)['created_at']


fatCount1 = db.basic.count_documents({"created_at": {
                                     "$gte": "Sun Nov 18 09:38:38 +0000 2018", "$lt": "Sun Nov 18 09:48:38 +0000 2018"}})
fatCount2 = db.basic.count_documents({"created_at": {
                                     "$gte": "Sun Nov 18 09:48:38 +0000 2018", "$lt": "Sun Nov 18 09:58:38 +0000 2018"}})
fatCount3 = db.basic.count_documents({"created_at": {
                                     "$gte": "Sun Nov 18 09:58:38 +0000 2018", "$lt": "Sun Nov 18 10:08:38 +0000 2018"}})
fatCount4 = db.basic.count_documents({"created_at": {
                                     "$gte": "Sun Nov 18 10:08:38 +0000 2018", "$lt": "Sun Nov 18 10:18:38 +0000 2018"}})
fatCount5 = db.basic.count_documents({"created_at": {
                                     "$gte": "Sun Nov 18 10:18:38 +0000 2018", "$lt": "Sun Nov 18 10:28:38 +0000 2018"}})
fatCount6 = db.basic.count_documents({"created_at": {
                                     "$gte": "Sun Nov 18 10:28:38 +0000 2018", "$lt": "Sun Nov 18 10:38:38 +0000 2018"}})

fatCount1 += db.enhancedStream.count_documents({"created_at": {
                                               "$gte": "Sun Nov 18 14:05:40 +0000 2018", "$lt": "Sun Nov 18 14:15:40 +0000 2018"}})
fatCount2 += db.enhancedStream.count_documents({"created_at": {
                                               "$gte": "Sun Nov 18 14:15:40 +0000 2018", "$lt": "Sun Nov 18 14:25:40 +0000 2018"}})
fatCount3 += db.enhancedStream.count_documents({"created_at": {
                                               "$gte": "Sun Nov 18 14:25:40 +0000 2018", "$lt": "Sun Nov 18 14:35:40 +0000 2018"}})
fatCount4 += db.enhancedStream.count_documents({"created_at": {
                                               "$gte": "Sun Nov 18 14:35:40 +0000 2018", "$lt": "Sun Nov 18 14:45:40 +0000 2018"}})
fatCount5 += db.enhancedStream.count_documents({"created_at": {
                                               "$gte": "Sun Nov 18 14:45:40 +0000 2018", "$lt": "Sun Nov 18 14:55:40 +0000 2018"}})
fatCount6 += db.enhancedStream.count_documents({"created_at": {
                                               "$gte": "Sun Nov 18 14:55:40 +0000 2018", "$lt": "Sun Nov 18 15:05:40 +0000 2018"}})

fatCount1 += db.rest.count_documents({"created_at": {
                                      "$gte": "Sun Nov 18 14:03:39 +0000 2018", "$lt": "Sun Nov 18 14:13:39 +0000 2018"}})
fatCount2 += db.rest.count_documents({"created_at": {
                                      "$gte": "Sun Nov 18 14:13:39 +0000 2018", "$lt": "Sun Nov 18 14:23:39 +0000 2018"}})
fatCount3 += db.rest.count_documents({"created_at": {
                                      "$gte": "Sun Nov 18 14:23:39 +0000 2018", "$lt": "Sun Nov 18 14:33:39 +0000 2018"}})
fatCount4 += db.rest.count_documents({"created_at": {
                                      "$gte": "Sun Nov 18 14:33:39 +0000 2018", "$lt": "Sun Nov 18 14:43:39 +0000 2018"}})
fatCount5 += db.rest.count_documents({"created_at": {
                                      "$gte": "Sun Nov 18 14:43:39 +0000 2018", "$lt": "Sun Nov 18 14:53:39 +0000 2018"}})
fatCount6 += db.rest.count_documents({"created_at": {
                                      "$gte": "Sun Nov 18 14:53:39 +0000 2018", "$lt": "Sun Nov 18 15:03:39 +0000 2018"}})


fatCount1 += db.geo.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:39:38 +0000 2018", "$lt": "Sun Nov 18 09:49:38 +0000 2018"}})
fatCount2 += db.geo.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:49:38 +0000 2018", "$lt": "Sun Nov 18 09:59:38 +0000 2018"}})
fatCount3 += db.geo.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:59:38 +0000 2018", "$lt": "Sun Nov 18 10:09:38 +0000 2018"}})
fatCount4 += db.geo.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:09:38 +0000 2018", "$lt": "Sun Nov 18 10:19:38 +0000 2018"}})
fatCount5 += db.geo.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:19:38 +0000 2018", "$lt": "Sun Nov 18 10:29:38 +0000 2018"}})
fatCount6 += db.geo.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:29:38 +0000 2018", "$lt": "Sun Nov 18 10:39:38 +0000 2018"}})


geoCount1 = db.geo.count_documents({"created_at": {
                                   "$gte": "Sun Nov 18 09:39:38 +0000 2018", "$lt": "Sun Nov 18 09:49:38 +0000 2018"}})
geoCount2 = db.geo.count_documents({"created_at": {
                                   "$gte": "Sun Nov 18 09:49:38 +0000 2018", "$lt": "Sun Nov 18 09:59:38 +0000 2018"}})
geoCount3 = db.geo.count_documents({"created_at": {
                                   "$gte": "Sun Nov 18 09:59:38 +0000 2018", "$lt": "Sun Nov 18 10:09:38 +0000 2018"}})
geoCount4 = db.geo.count_documents({"created_at": {
                                   "$gte": "Sun Nov 18 10:09:38 +0000 2018", "$lt": "Sun Nov 18 10:19:38 +0000 2018"}})
geoCount5 = db.geo.count_documents({"created_at": {
                                   "$gte": "Sun Nov 18 10:19:38 +0000 2018", "$lt": "Sun Nov 18 10:29:38 +0000 2018"}})
geoCount6 = db.geo.count_documents({"created_at": {
                                   "$gte": "Sun Nov 18 10:29:38 +0000 2018", "$lt": "Sun Nov 18 10:39:38 +0000 2018"}})


chartinput = np.array(object=[
                      fatCount1, fatCount2, fatCount3, fatCount4, fatCount5, fatCount6], dtype=int)
chartlabels = ['0 to 10', '10 to 20', '20 to 30',
               '30 to 40', '40 to 50', '50 to 60']

xlabel = 'collection'
ylabel = 'records'
chartitles = 'History for the collected data'

plot_chart(chartinput, chartlabels, chartitles, xlabel, ylabel)

# GeoTagged from GLA
chartitles = 'History for the GeoTagged data'
chartinput2 = np.array(object=[
                       geoCount1, geoCount2, geoCount3, geoCount4, geoCount5, geoCount6], dtype=int)
plot_chart(chartinput2, chartlabels, chartitles, xlabel, ylabel)

# Measure overlapping

distinct1 = len(db.fat.find({"created_at": {
    "$gte": "Sun Nov 18 09:38:38 +0000 2018", "$lt": "Sun Nov 18 09:48:38 +0000 2018"}}).distinct("id"))
distinct2 = len(db.fat.find({"created_at": {
    "$gte": "Sun Nov 18 09:48:38 +0000 2018", "$lt": "Sun Nov 18 09:58:38 +0000 2018"}}).distinct("id"))
distinct3 = len(db.fat.find({"created_at": {
    "$gte": "Sun Nov 18 09:58:38 +0000 2018", "$lt": "Sun Nov 18 10:08:38 +0000 2018"}}).distinct("id"))
distinct4 = len(db.fat.find({"created_at": {
    "$gte": "Sun Nov 18 10:08:38 +0000 2018", "$lt": "Sun Nov 18 10:18:38 +0000 2018"}}).distinct("id"))
distinct5 = len(db.fat.find({"created_at": {
    "$gte": "Sun Nov 18 10:18:38 +0000 2018", "$lt": "Sun Nov 18 10:28:38 +0000 2018"}}).distinct("id"))
distinct6 = len(db.fat.find({"created_at": {
    "$gte": "Sun Nov 18 10:28:38 +0000 2018", "$lt": "Sun Nov 18 10:38:38 +0000 2018"}}).distinct("id"))
distinct1 = fatCount1 - distinct1
distinct2 = fatCount2 - distinct2
distinct3 = fatCount3 - distinct3
distinct4 = fatCount4 - distinct4
distinct5 = fatCount5 - distinct5
distinct6 = fatCount6 - distinct6

chartitles = 'Redundancy'
chartinput2 = np.array(object=[
                       distinct1, distinct2, distinct3, distinct4, distinct5, distinct6], dtype=int)
plot_chart(chartinput2, chartlabels, chartitles, xlabel, ylabel)


fatquote = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:38:38 +0000 2018", "$lt": "Sun Nov 18 09:48:38 +0000 2018"}, "is_quote_status": True})
fatquote2 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:48:38 +0000 2018", "$lt": "Sun Nov 18 09:58:38 +0000 2018"}, "is_quote_status": True})
fatquote3 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:58:38 +0000 2018", "$lt": "Sun Nov 18 10:08:38 +0000 2018"}, "is_quote_status": True})
fatquote4 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:08:38 +0000 2018", "$lt": "Sun Nov 18 10:18:38 +0000 2018"}, "is_quote_status": True})
fatquote5 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:18:38 +0000 2018", "$lt": "Sun Nov 18 10:28:38 +0000 2018"}, "is_quote_status": True})
fatquote6 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:28:38 +0000 2018", "$lt": "Sun Nov 18 10:38:38 +0000 2018"}, "is_quote_status": True})

fatre = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:38:38 +0000 2018", "$lt": "Sun Nov 18 09:48:38 +0000 2018"}, "retweeted_status": { "$exists": True } })
fatre2 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:48:38 +0000 2018", "$lt": "Sun Nov 18 09:58:38 +0000 2018"}, "retweeted_status": { "$exists": True } })
fatre3 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 09:58:38 +0000 2018", "$lt": "Sun Nov 18 10:08:38 +0000 2018"}, "retweeted_status": { "$exists": True } })
fatre4 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:08:38 +0000 2018", "$lt": "Sun Nov 18 10:18:38 +0000 2018"}, "retweeted_status": { "$exists": True } })
fatre5 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:18:38 +0000 2018", "$lt": "Sun Nov 18 10:28:38 +0000 2018"}, "retweeted_status": { "$exists": True } })
fatre6 = db.fat.count_documents({"created_at": {
    "$gte": "Sun Nov 18 10:28:38 +0000 2018", "$lt": "Sun Nov 18 10:38:38 +0000 2018"}, "retweeted_status": { "$exists": True } })

chartitles = 'Retweet & Quote vs Total'
# chartinput2 = np.array(object=[
#                        fatquote, fatquote2, fatquote3, fatquote4, fatquote5, fatquote6], dtype=int)
chartinput = np.array(object=[
                      fatCount1, fatCount2, fatCount3, fatCount4, fatCount5, fatCount6], dtype=int)
chartinput3 = np.array(object=[
    fatre, fatre2, fatre3, fatre4, fatre5, fatre6], dtype=int)

chartlabels = ['0 to 10', '10 to 20', '20 to 30',
               '30 to 40', '40 to 50', '50 to 60']

x = np.arange(6)
x = x + 1
ax = plt.subplot(111)
plt.bar(x, height=chartinput, width=1.0, label="Total")
plt.bar(x, height=chartinput2, width=1.0, label="Redundancy")

plt.xticks(x, x, fontsize=12)
plt.xlabel('10 minute Periods', fontsize=18)
plt.ylabel('Tweets Collected', fontsize=18)
plt.title('Tweets Collected in 10 minute periods', fontsize=23)
plt.legend()
plt.show()

# Retweet & quote
