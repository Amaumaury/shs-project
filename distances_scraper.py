import json
import requests
from tqdm import tqdm

with open('words.json', 'r') as f:
    countries = json.load(f)['countries']

def distance(country):
    url = 'https://www.distance24.org/route.json?stops=Suisse|{}'.format(country)
    req = requests.get(url)
    return req.json()['distance']

distances = [(c, distance(c)) for c in tqdm(countries)]
distances.sort(key=lambda t: t[1])

with open('distances.csv', 'w') as f:
    f.write('COUNTRY,DISTANCE FROM SWITZERLAND\n')
    for name, dist in distances:
        f.write('{},{}\n'.format(name, dist))

