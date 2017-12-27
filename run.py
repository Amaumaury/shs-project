import numpy as np
import sklearn as sk
import os, sys
import unidecode
import xml.etree.ElementTree as ET

from tqdm import tqdm_notebook

from sklearn.feature_extraction.text import CountVectorizer

DATA_FOLDER = '200yrs-news/'
GDL_FOLDER = DATA_FOLDER + 'GDL/'
JDG_FOLDER = DATA_FOLDER + 'JDG/'

def mac_listdir(folder):
    return list(filter(lambda file: file != '.DS_Store', os.listdir(folder)))

flatten = lambda l: [item for sublist in l for item in sublist]

def format_string(string):
    return unidecode.unidecode(string).lower()

countries_file = open('present_countries', 'r')
world_countries = [format_string(country.replace('\n', '')) for country in countries_file]

gdl_files = flatten([[GDL_FOLDER + years_folder + '/' + file for file in mac_listdir(GDL_FOLDER + years_folder)] for years_folder in mac_listdir(GDL_FOLDER)])

def extract_features(article, countries):
    try:
        date = article.find('entity').find('meta').find('issue_date')
        id = article.find('entity').find('meta').find('id')
        text = format_string(article.find('entity').find('full_text').text)

        countries_occurences = list(map(lambda country: [id, date, country, text.count(country)], countries))
    except:
        return None
    
    return countries_occurences

world_coverage = []

for file in tqdm_notebook(gdl_files):
    for article in ET.parse(file).getroot().findall('article'):
        world_coverage.append(extract_features(article, world_countries))
    
countries_count_file = open('countries_count_per_article.txt', 'w')

for article in world_coverage:
    countries_count_file.write("%s\n" % article)