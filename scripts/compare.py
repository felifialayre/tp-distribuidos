# Compara el output de los clientes (output/<cliente>/qN.csv) contra los
# resultados esperados (expected/<cliente>/qN.csv, ver scripts/expected.py).
# Uso: python3 scripts/compare.py  (sale con 1 si alguna query no coincide)

import csv
import itertools
import os
import sys

OUTPUT_DIR = 'output'
EXPECTED_DIR = 'expected'
MAX_SHOWN = 5  # cuantas filas distintas se muestran por query


def read_csv(path):
    with open(path, encoding='utf-8', newline='') as f:
        return [tuple(row) for row in csv.reader(f)]


def compare_unordered(got, expected):
    # Q1, Q2, Q3 y Q5 no tienen orden: se comparan como conjuntos de filas
    missing = sorted(set(expected) - set(got))
    extra = sorted(set(got) - set(expected))
    errors = [f'falta {row}' for row in missing[:MAX_SHOWN]] + [f'sobra {row}' for row in extra[:MAX_SHOWN]]
    if len(got) != len(expected):
        errors.append(f'{len(got)} filas, se esperaban {len(expected)}')
    return errors


def compare_q2(got, expected, client):
    # el cliente escribe q2.csv con header (name,url,path) y deja las imagenes en
    # output/<cliente>/<path>; expected tiene (name,url,path) y las imagenes en
    # expected/<cliente>/<path>. El image downloader publica TODAS las biografias que le
    # llegan: si la descarga falla (404, 429 agotado, timeout) la publica sin imagen, y en
    # ese caso el path del cliente queda vacio. Se compara:
    #   - nombre y url (sin orden: los workers descargan en paralelo)
    #   - que haya imagen exactamente cuando expected pudo bajarla
    #   - que el archivo de la imagen exista y tenga los mismos bytes que el esperado
    # Si una fila difiere solo en "con/sin imagen" suele ser una falla transitoria de
    # Wikimedia (en el cliente o al generar expected): conviene repetir antes de investigar.
    if got and got[0] == ('name', 'url', 'path'):
        got = got[1:]
    if any(len(row) != 3 for row in got) or any(len(row) != 3 for row in expected):
        return ['hay filas de q2.csv que no tienen 3 columnas']

    def label(has_image):
        return 'con imagen' if has_image else 'sin imagen'

    errors = compare_unordered([(n, u, label(bool(p))) for n, u, p in got],
                               [(n, u, label(bool(p))) for n, u, p in expected])
    expected_paths = {url: path for _, url, path in expected}
    for _, url, path in got:
        if not path:
            continue
        got_file = os.path.join(OUTPUT_DIR, client, path)
        if not os.path.isfile(got_file):
            errors.append(f'no existe la imagen {path} de {url}')
            continue
        expected_path = expected_paths.get(url)
        if not expected_path:
            continue  # sobra o expected no la pudo bajar: no hay bytes contra que comparar
        expected_file = os.path.join(EXPECTED_DIR, client, expected_path)
        if os.path.isfile(expected_file) and not same_bytes(got_file, expected_file):
            errors.append(f'la imagen {path} de {url} no coincide con la esperada')
    return errors


def same_bytes(path_a, path_b):
    with open(path_a, 'rb') as a, open(path_b, 'rb') as b:
        return a.read() == b.read()


def compare_top(got, expected):
    # Q4 es un top: los conteos tienen que venir en el mismo orden. Las palabras con el
    # mismo conteo pueden estar en cualquier orden, y en el ultimo conteo puede haber
    # empates que quedaron afuera del top, asi que ahi solo se compara el conteo
    if [count for _, count in got] != [count for _, count in expected]:
        return [f'conteos {[c for _, c in got]}, se esperaban {[c for _, c in expected]}']
    last = expected[-1][1] if expected else None
    errors = []
    groups = zip(itertools.groupby(got, key=lambda r: r[1]), itertools.groupby(expected, key=lambda r: r[1]))
    for (count, got_group), (_, expected_group) in groups:
        got_words = {word for word, _ in got_group}
        expected_words = {word for word, _ in expected_group}
        if count != last and got_words != expected_words:
            errors.append(f'con {count}: {sorted(got_words)}, se esperaban {sorted(expected_words)}')
    return errors


COMPARATORS = {
    'q1': compare_unordered,
    'q2': None,  # tiene su propio comparador (compare_q2), necesita el cliente
    'q3': compare_unordered,
    'q4': compare_top,
    'q5': compare_unordered,
}


def main():
    if not os.path.isdir(EXPECTED_DIR):
        print(f'no existe {EXPECTED_DIR}/, corre primero: make expected')
        sys.exit(1)
    failed = False
    for client in sorted(os.listdir(EXPECTED_DIR)):
        for query, compare in COMPARATORS.items():
            got_path = os.path.join(OUTPUT_DIR, client, f'{query}.csv')
            expected = read_csv(os.path.join(EXPECTED_DIR, client, f'{query}.csv'))
            if not os.path.exists(got_path):
                errors = ['no existe el output']
            else:
                got = read_csv(got_path)
                errors = compare_q2(got, expected, client) if query == 'q2' else compare(got, expected)
            print(f'{client} {query}: {"✅ OK" if not errors else "❌ FALLA"} ({len(expected)} filas esperadas)')
            for error in errors:
                print(f'    {error}')
            failed = failed or bool(errors)
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()