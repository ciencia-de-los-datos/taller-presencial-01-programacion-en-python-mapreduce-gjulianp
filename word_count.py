#
# Escriba la función load_input que recive como parámetro un folder y retorna
# una lista de tuplas donde el primer elemento de cada tupla es el nombre del
# archivo y el segundo es una línea del archivo. La función convierte a tuplas
# todas las lineas de cada uno de los archivos. La función es genérica y debe
# leer todos los archivos de folder entregado como parámetro.
#
# Por ejemplo:
#   [
#     ('text0'.txt', 'Analytics is the discovery, inter ...'),
#     ('text0'.txt', 'in data. Especially valuable in ar...').
#     ...
#     ('text2.txt'. 'hypotheses.')
#   ]
#
def load_input(input_directory):
    pass


#
# Escriba una función llamada maper que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). En este caso,
# la clave es cada palabra y el valor es 1, puesto que se está realizando un
# conteo.
#
#   [
#     ('Analytics', 1),
#     ('is', 1),
#     ...
#   ]
#
def mapper(sequence):
    pass


#
# Escriba la función shuffle_and_sort que recibe la lista de tuplas entregada
# por el mapper, y retorna una lista con el mismo contenido ordenado por la
# clave.
#
#   [
#     ('Analytics', 1),
#     ('Analytics', 1),
#     ...
#   ]
#
def shuffle_and_sort(sequence):
    pass


#
# Escriba la función reducer, la cual recibe el resultado de shuffle_and_sort y
# reduce los valores asociados a cada clave sumandolos. Como resultado, por
# ejemplo, la reducción indica cuantas veces aparece la palabra analytics en el
# texto.
#
def reducer(sequence):
    pass


#
# Escriba la función create_ouptput_directory que recibe un nombre de directorio
# y lo crea. Si el directorio existe, la función falla.
#
def create_ouptput_directory(output_directory):
    pass


#
# Escriba la función save_output, la cual almacena en un archivo de texto llamado
# part-00000 el resultado del reducer. El archivo debe ser guardado en el
# directorio entregado como parámetro, y que se creo en el paso anterior.
# Adicionalmente, el archivo debe contener una tupla por línea, donde el primer
# elemento es la clave y el segundo el valor. Los elementos de la tupla están
# separados por un tabulador.
#
def save_output(output_directory, sequence):
    pass


#
# La siguiente función crea un archivo llamado _SUCCESS en el directorio
# entregado como parámetro.
#
def create_marker(output_directory):
    pass


#
# Escriba la función job, la cual orquesta las funciones anteriores.
#
def job(input_directory, output_directory):
    pass


if __name__ == "__main__":
    job(
        "input",
        "output",
    )
