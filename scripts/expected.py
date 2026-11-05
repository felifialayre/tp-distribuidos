# Genera los resultados esperados de cada query a partir de los parquet de input/,
# replicando la notebook de referencia:
# https://www.kaggle.com/code/francobar/wikipedia-article-analysis
#
# Escribe expected/<cliente>/qN.csv con el mismo formato que el output del cliente.
# Uso: python3 scripts/expected.py  (necesita pyarrow y nltk, ver scripts/requirements.txt)

import collections
import csv
import glob
import json
import os
import re
import urllib.request
import zlib

import nltk
import pyarrow.parquet as pq

INPUT_DIR = 'input'
EXPECTED_DIR = 'expected'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
CUTOFF_YEAR = 2020  # Q1, Q2 y Q5 comparan contra articulos de 2020 o antes
Q4_TOP = 20
BIOGRAPHY_NAMES = ('biographie', 'biography')
# mismo User-Agent que el image downloader: Wikimedia devuelve 403 sin uno descriptivo
USER_AGENT = 'tp-distribuidos-fiuba/0.1 (ffialayre@fi.uba.ar)'

# mismas regex que la notebook
WORD_REGEX = r'\b\w+\b'
NUMBER_REGEX = r'[+-]?((\d+\.?\d*)|(\.\d+))'

nltk.download('stopwords', quiet=True)
STOP_WORDS = set(nltk.corpus.stopwords.words('english')) | set(nltk.corpus.stopwords.words('french'))


def extract_words(abstract):
    words = re.findall(WORD_REGEX, abstract.lower())
    return [w for w in words if w not in STOP_WORDS and not re.fullmatch(NUMBER_REGEX, w)]


def has_biography(sections):
    if not isinstance(sections, str):
        return False
    return any(s.get('name', '').lower() in BIOGRAPHY_NAMES for s in json.loads(sections))


def download_image(url):
    request = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            if response.status != 200:
                return None
            return response.read()
    except Exception as error:  # red, 404, timeout, etc: se descarta
        print(f'  no se pudo bajar {url}: {error}')
        return None


def load_rows(client):
    columns = ['abstract', 'date_modified', 'image', 'in_language', 'name', 'sections', 'url']
    rows = []
    for path in sorted(glob.glob(os.path.join(INPUT_DIR, client, '*.parquet'))):
        rows += pq.read_table(path, columns=columns).to_pylist()
    for row in rows:
        row['lang'] = (row['in_language'] or {}).get('identifier')
        row['before'] = row['date_modified'].year <= CUTOFF_YEAR
        # palabras distintas del abstract, None si el abstract es nulo (se descarta en Q3, Q4 y Q5)
        row['words'] = set(extract_words(row['abstract'])) if row['abstract'] is not None else None
    return rows


def q1(rows):
    return [[r['url'], r['date_modified'].strftime(DATE_FORMAT)]
            for r in rows if r['lang'] == 'en' and r['before']]


def q2(rows, images_dir):
    # biografias de 2020 o antes que tienen imagen. Baja la imagen (igual que el
    # image downloader) y la guarda nombrada por crc32(url); solo entran las que se
    # pudieron bajar, asi el comparador verifica los bytes contra el output del cliente.
    os.makedirs(images_dir, exist_ok=True)
    result = []
    for r in rows:
        image_url = (r['image'] or {}).get('content_url')
        if not (r['before'] and image_url and has_biography(r['sections'])):
            continue
        data = download_image(image_url)
        if data is None:
            continue
        filename = f"{zlib.crc32(r['url'].encode('utf-8')) & 0xffffffff:08x}"
        with open(os.path.join(images_dir, filename), 'wb') as f:
            f.write(data)
        result.append([r['name'], r['url'], os.path.join('q2_images', filename)])
    return result


def q3(rows):
    en, fr = set(), set()
    for r in rows:
        if r['words'] is None:
            continue
        if r['lang'] == 'en':
            en |= r['words']
        elif r['lang'] == 'fr':
            fr |= r['words']
    return [[word] for word in sorted(en & fr)]


def q4(rows):
    counts = collections.Counter()
    for r in rows:
        if r['words'] is not None:
            counts.update(r['words'])
    return [[word, count] for word, count in counts.most_common(Q4_TOP)]


def q5(rows):
    # la notebook cuenta las palabras repetidas; nosotros contamos las distintas, igual que Q4
    before = [len(r['words']) for r in rows if r['before'] and r['words'] is not None]
    average = sum(before) / len(before) if before else float('inf')
    return [[r['url'], len(r['words'])]
            for r in rows if not r['before'] and r['words'] is not None and len(r['words']) > average]


def main():
    for client in sorted(os.listdir(INPUT_DIR)):
        if not os.path.isdir(os.path.join(INPUT_DIR, client)):
            continue
        rows = load_rows(client)
        client_dir = os.path.join(EXPECTED_DIR, client)
        os.makedirs(client_dir, exist_ok=True)
        results = {
            'q1': q1(rows),
            'q2': q2(rows, os.path.join(client_dir, 'q2_images')),
            'q3': q3(rows),
            'q4': q4(rows),
            'q5': q5(rows),
        }
        for name, result in results.items():
            with open(os.path.join(client_dir, f'{name}.csv'), 'w', encoding='utf-8', newline='') as f:
                csv.writer(f).writerows(result)
            print(f'{client} {name}: {len(result)} filas')


if __name__ == '__main__':
    main()
