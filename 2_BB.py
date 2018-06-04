import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import pickle
import string
from dateutil import parser
from datetime import date, datetime, timedelta
import lxml


def get_soup(start_url):
    response = requests.get(start_url)
    page = response.text
    return BeautifulSoup(page, "lxml")


def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


urls = []
for suffix in perdelta(date(1958, 8, 9), date(2012, 12, 31), timedelta(days=7)):
    prefix = 'http://www.billboard.com/charts/hot-100/'
    link = prefix + str(suffix)
    urls.append(link)


# urls[:3]
def get_artist(soup):
    artist = soup.find_all(class_="chart-row__artist")
    artists = []
    for item in artist:
        item = item.text.strip()
        artists.append(item)
    return artists


def get_track(soup):
    track = soup.find_all(class_="chart-row__song")
    tracks = []
    for item in track:
        item = item.text.strip()
        tracks.append(item)
    return tracks


frames = []
billboard_dict = {}
for url in urls:
    soup = get_soup(url)
    artist = get_artist(soup)
    track = get_track(soup)
    dates = [parser.parse(url.split('/')[5])] * len(artist)
    df = pd.DataFrame({'artist': artist, 'track': track, 'publish_date': dates})

    frames.append(df)

df_merge = pd.concat(frames).reset_index(drop=True)
df2 = df_merge.drop_duplicates(['track'], keep='first').reset_index(drop=True)

with open('dataframes/billboard_unique.pkl', 'wb') as picklefile:
    pickle.dump(df2, picklefile)


def text_clean(x):
    try:
        x = x.decode('utf-8')
    except:
        None
    return x


df2 = df2.applymap(text_clean)
print(df2.head())
df2.to_csv("HotSongsBillBoard_C.csv", encoding="utf-8", index = False)


