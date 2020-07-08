import pandas as pd
import numpy as np
from pandas.api.types import CategoricalDtype
import pyodbc


def query_sad(query_file, params=None):
    """
    Retrae la tabla generada por una consulta realizada a SAD

    *Importante: La conexión se establece a través de 'Trusted_Connection', por lo que es necesario encontrarse en el
    conectado en el dominio de FEN.

    Parameters
    ----------
    query_file: str
        Ruta absoluta o relativa del archivo sql con la consulta

    params : tuple, default None
        Variables para query parametrizada.
    Returns
    -------
    pandas.DataFrame
        Tabla generada por la consulta
    """

    # Lectura de la consulta
    with open(f'{query_file}', 'r', encoding='utf_8') as file:
        query = file.read()
    # Establecer los parámetros, si se ingresaron
    if params is not None:
        query = query.format(*params)
    # Conexión a sad
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                          'Server=sbd04;'
                          'Database=sad;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    cursor.execute(query)
    raw_data = list(cursor.fetchall())
    values = [list(row) for row in raw_data]
    columns = [tupla[0] for tupla in cursor.description]

    return pd.DataFrame(data=values, columns=columns)


def calcular_avance(ud):
    """
    Calcula el año de vance según la cantidad de créditos (ud) aprobados.

    Parameters
    ----------
    ud : int
        Cantidad de créditos aprobados

    Returns
    -------
    str
        Año de avance
    """
    if 0 <= ud < 58:
        return 'Primer Año'
    elif ud < 118:
        return 'Segundo Año'
    elif ud < 178:
        return 'Tercer Año'
    elif ud < 238:
        return 'Cuarto Año'
    elif ud < 300:
        return 'Quinto Año'
    else:
        return 'N/A'


def avance_carrera(series):
    """
    Aplica el cálculo de avance de la carrera a un objeto Series

    Parameters
    ----------
    series:
        Columna con cantidad de créditos aprobados, por alumno.

    Returns
    -------
    pandas.Series
        Columna con año de avance en la carrera, por alumno.
    """

    s = series.map(lambda x: calcular_avance(x))\
              .rename('Avance_Carrera')
    cat_avance = CategoricalDtype(categories=['Primer Año', 'Segundo Año', 'Tercer Año',
                                              'Cuarto Año', 'Quinto Año'],
                                  ordered=True)
    return s.astype({'Avance_Carrera': cat_avance})


def categorizar_alumnos(df, tipo='Tipo_Alumno', mencion='Cod_Mencion'):
    """
    Convierte las columnas con el tipo de alumno y mención en categóricas

    Parameters
    ----------
    df: pandas.DataFrame
        DataFrame con columnas a actualizar

    tipo: str, default 'Tipo_Alumno'
        Columna con la carrera o tipo de alumno de FEN

    mencion : str, default 'Cod_Mencion'
        Columna con la mención de la carrera

    Returns
    -------
    pandas.DataFrame
        Tabla con columnas convertidas a categóricas
    -------

    """

    tmp = df.copy()
    tmp[mencion] = tmp[mencion].fillna('N/A')

    cat_tipo = CategoricalDtype(categories=['IC', 'IICG', 'CA', 'LIBRE'], ordered=True)
    cat_mencion = CategoricalDtype(categories=['PC', 'MA', 'ME', 'N/A'], ordered=True)

    return tmp.astype({tipo: cat_tipo, mencion: cat_mencion})