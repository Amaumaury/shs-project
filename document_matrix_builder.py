from sklearn.feature_extraction.text import CountVectorizer
import unidecode
import os
import xml.etree.ElementTree as ET
import scipy.sparse
from tqdm import tqdm_notebook

vectorizer = CountVectorizer()

def format_string(string):
    return unidecode.unidecode(string)


flatten = lambda l: [item for sublist in l for item in sublist]


def mac_listdir(folder):
    return list(filter(lambda file: file != '.DS_Store', os.listdir(folder)))

def article_generator(root):
    files = flatten([[root + years_folder + '/' + file for file in mac_listdir(root + years_folder)] for years_folder in mac_listdir(root)])

    for file_name in tqdm_notebook(files):
            file = open(file_name, 'r')
            for article in ET.parse(file).getroot().findall('article'):
                try:
                    yield format_string(article.find('entity').find('full_text').text)
                except:
                    pass
            file.close()


document_matrix = vectorizer.fit_transform(article_generator('200yrs-news/GDL/'))

scipy.sparse.save_npz('document_matrix', document_matrix)
