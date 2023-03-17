from bs4 import BeautifulSoup
import requests
from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string

#scrape reviews of the movie "Squid Game" from the IMDb website
url = "https://www.imdb.com/title/tt10919420/reviews?ref_=tt_sa_3"
#use the requests library to fetch the HTML content of the IMDb webpage containing the reviews
res = requests.get(url)
#create a BeautifulSoup object from the HTML content to parse and extract the text of each review
soup = BeautifulSoup(res.text, 'html.parser')
#"text.show-more__control" has each reviews
review_div = soup.select(".text.show-more__control")

nltk.download('stopwords')
nltk.download('punkt')

def remove_punctuation(text):
    punctuations = string.punctuation
    translator = str.maketrans('', '', punctuations)
    return text.translate(translator)

def remove_stopwords(text):
    stop_words = set(stopwords.words('english'))
    words = word_tokenize(text)
    filtered_words = [word for word in words if word.lower() not in stop_words]
    return ' '.join(filtered_words)

#initialize SentimentIntensityAnalyzer()
sia = SentimentIntensityAnalyzer()

reviews = []
for review in review_div:
    text = review.get_text().strip()
    text = remove_stopwords(text)
    text = remove_punctuation(text)
    reviews.append(text)

sentiment = []
for text in reviews:
    score = sia.polarity_scores(text)
    #example pos: {'neg': 0.0, 'neu': 0.862, 'pos': 0.138, 'compound': 0.7172}
    #example neg: {'neg': 0.139, 'neu': 0.748, 'pos': 0.113, 'compound': -0.3832}
    if score['compound'] > 0:
        sentiment.append("pos")
    elif score['compound'] < 0:
        sentiment.append("neg")
    else:
        sentiment.append("neu")

#make a csv file
pd.DataFrame({'reviews':reviews,'sentiment':sentiment}).to_csv('squid_game_review.csv',index = None)

############ upload the file to my container ###########

connection_string = "DefaultEndpointsProtocol=https;AccountName=sentimentassignment;AccountKey=9TBePH/bqSrSWlUhzKFdNLX/TT2sB9Z/0T3sTPOj0alALmV9q7clKI7EgzBySgpv7UpiN+GZpgMV+AStQop1bQ==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
local_file_path = "squid_game_review.csv"
container_name = "squidgame"

blob_name = os.path.basename(local_file_path)
container_client = blob_service_client.get_container_client(container_name)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

with open(local_file_path, "rb") as data:
    blob_client.upload_blob(data)

#reference: https://www.youtube.com/watch?v=OZGPAKTXGDs