"""GitHub Classroom autograding script."""

import os

from word_count import job

job("input/", "output/")

#
# Retorna error si la carpeta output/ no existe
if not os.path.exists("output/"):
    raise FileNotFoundError("Output directory does not exist")

#
# Retorna error si el archivo "_SUCCESS" no existe en la
# carpeta output/
if not os.path.exists("output/_SUCCESS"):
    raise FileNotFoundError("Output directory is empty")

#
# Lee el contenido del archivo "part-00000" en la carpeta output/
# Cada linea en el archivo esta conformada por una clave un valor,
# separados por un tabulador. Asigne pareja a un diccionario
with open("output/part-00000", "r", encoding="utf-8") as f:
    lines = f.readlines()
    result = {}
    for line in lines:
        key, value = line.strip().split("\t")
        result[key] = int(value)

assert result["analytics"] == 5
assert result["business"] == 7
assert result["by"] == 3
assert result["algorithms"] == 2
assert result["analysis"] == 4
