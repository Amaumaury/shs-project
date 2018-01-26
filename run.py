import os
import unidecode
import xml.etree.ElementTree as ET
import gc

from tqdm import tqdm

DATA_FOLDER = '200yrs-news/'
GDL_FOLDER = DATA_FOLDER + 'GDL/'
JDG_FOLDER = DATA_FOLDER + 'JDG/'

def mac_listdir(folder):
    return list(filter(lambda file: file != '.DS_Store', os.listdir(folder)))


flatten = lambda l: [item for sublist in l for item in sublist]

def format_string(string):
    return unidecode.unidecode(string)


countries_file = open('present_countries', 'r')
world_countries = [format_string(country.replace('\n', '')) for country in countries_file]

gdl_files = flatten([[GDL_FOLDER + years_folder + '/' + file for file in mac_listdir(GDL_FOLDER + years_folder)] for years_folder in mac_listdir(GDL_FOLDER)])
jdg_files = flatten([[GDL_FOLDER + years_folder + '/' + file for file in mac_listdir(JDG_FOLDER + years_folder)] for years_folder in mac_listdir(JDG_FOLDER)])

def extract_features(article, countries):
    try:
        date = article.find('entity').find('meta').find('issue_date').text
        id_ = article.find('entity').find('meta').find('id').text
        text = format_string(article.find('entity').find('full_text').text)

        countries_occurences = list(map(lambda country: text.count(country), countries))
        article_infos = [id_, date]
        article_infos.extend(countries_occurences)
    except:
        return None

    return article_infos

def create_coverage_file(prefix):
    world_coverage = open(prefix + 'world_coverage', 'w')

    world_coverage.write("%s,%s," % ('id', 'date'))

    for i, country in enumerate(world_countries):
        if i < len(world_countries) - 1:
            world_coverage.write("%s," % country)
        else:
            world_coverage.write("%s\n" % country)
    return world_coverage

def compute_coverage(original_files, countries, coverage_file):
    for file_name in tqdm(original_files):
        file = open(file_name, 'r')
        for article in ET.parse(file).getroot().findall('article'):
            countries_occurences = extract_features(article, countries)

            if countries_occurences is not None:
                for i, elem in enumerate(countries_occurences):
                    if i < len(countries_occurences) - 1:
                        coverage_file.write("%s," % elem)
                    else:
                        coverage_file.write("%s\n" % elem)
        file.close()
        gc.collect()


gdl_world_coverage = create_coverage_file('gdl_')
jdg_world_coverage = create_coverage_file('jdg_')

compute_coverage(gdl_files, world_countries, gdl_world_coverage)
compute_coverage(jdg_files, world_countries, jdg_world_coverage)

gdl_world_coverage.close()
jdg_world_coverage.close()
