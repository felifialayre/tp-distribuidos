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
    # Q1, Q3 y Q5 no tienen orden: se comparan como conjuntos de filas
    missing = sorted(set(expected) - set(got))
    extra = sorted(set(got) - set(expected))
    errors = [f'falta {row}' for row in missing[:MAX_SHOWN]] + [f'sobra {row}' for row in extra[:MAX_SHOWN]]
    if len(got) != len(expected):
        errors.append(f'{len(got)} filas, se esperaban {len(expected)}')
    return errors


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
    # todo: Q2 todavia no llega al cliente (el image downloader guarda en downloads/ y no
    # manda resultados al gateway). Cuando el cliente escriba q2.csv se descomenta.
    # 'q2': compare_unordered,
    'q3': compare_unordered,
    'q4': compare_top,
    'q5': compare_unordered,
}


def main():
    failed = False
    for client in sorted(os.listdir(EXPECTED_DIR)):
        for query, compare in COMPARATORS.items():
            got_path = os.path.join(OUTPUT_DIR, client, f'{query}.csv')
            expected = read_csv(os.path.join(EXPECTED_DIR, client, f'{query}.csv'))
            if not os.path.exists(got_path):
                errors = ['no existe el output']
            else:
                errors = compare(read_csv(got_path), expected)
            print(f'{client} {query}: {"✅ OK" if not errors else "❌ FALLA"} ({len(expected)} filas esperadas)')
            for error in errors:
                print(f'    {error}')
            failed = failed or bool(errors)
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
