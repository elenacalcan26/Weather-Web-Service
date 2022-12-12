import mysql.connector as mysql
from datetime import date, datetime
import sys

db_connection=mysql.connect(
    host='mysql',
    port=3306,
    user='admin',
    password='admin',
    database='WeatherDB'
)

cursor = db_connection.cursor()

COUNTRY_TABLE = 'Country'
CITY_TABLE = 'City'
TEMPERATURE_TABLE = 'Temperature'
COUNTRY_TABLE_COLUMNS_INSERT = ('country_name', 'latitude', 'longitude')
CITY_TABLE_COLUMNS_INSERT = ('country_id', 'city_name', 'latitude', 'longitude')
TEMPERATURE_TABLE_COLUMNS_OP = ('city_id', 'value')
COUNTRY_TABLE_COLUMNS = ('id', 'nume', 'lat', 'lon')
CITY_TABLE_COLUMNS = ('id', 'idTara', 'nume', 'lat', 'lon')
TEMPERATURE_TABLE_COLUMNS = ('id', 'value', 'timestamp', 'city_id')
TEMPERATURE_TABLE_COLUMNS_SELECT = ('id', 'value', 'timestamp')
TEMPERATURE_RESPONSE_FIELDS = ('id', 'valoare', 'timestamp')


def insert_record(table, columns, data):
    """
    Inseareaza un record intr-o tabela

    Args:
        table (str): numele tabelei in care se insereaza
        columns (tuple): coloanele tabelei in care se insereaza noi date
        data (tuple): valorile coloanelor

    Returns:
        int: statusul operatiei
    """

    body = ', '.join(str(column) for column in columns)
    try:
        cursor.execute(f'INSERT INTO {table} ({body}) VALUES {data}')
        db_connection.commit()
    except:
        return 409

    return 201

def success_insertion_resp_body():
    """
        Returneaza id-ul ultimei linii inserate

    Returns:
        map: id-ul ultimei linii inserate
    """
    resp = {}
    cursor.execute("SELECT LAST_INSERT_ID()")
    last_inserted_id = cursor.fetchone()[0]
    resp = {'id': last_inserted_id}
    return resp

def process_response_payload(records, columns):
    """
    Genereaza raspunsul server-ului.
    Raspunsul este compus din record-uri selectate dintr-o anumita tabela.

    Args:
        records (tuple): record-urile selectate din tabela
        columns (tuple): coloanele corespunzatoare record-urilor

    Returns:
        array: list cu valorile coloanelor selectate
    """
    resp = []
    for record in records:
        obj = {}
        for i in range(len(record)):
            column = columns[i]

            if isinstance(record[i], (datetime, date)):
                obj[column] = record[i].strftime('%Y-%m-%d')
            else:
                obj[column] = record[i]
        resp.append(obj)
    return resp


def delete_record_by_id(table, id):
    """
    Sterge din tabela un record cu un id dat

    Args:
        table (str): tabela din care se face stergerea
        id (_type_): id-ul record-ului care se sterge

    Returns:
        int: stat code-ul operatiei
    """
    try:
        cursor.execute(f'DELETE FROM {table} WHERE id = {id}')
    except:
        return 404
    db_connection.commit()
    return 200

def get_filtered_data(table, columns_to_select, **kwargs):
    """
    Selecteaza datele dintr-o tabela.
    Datele selectate pot fi filtrate si in functie de niste paramterii optionali

    Args:
        table (str): tabela din care se selecteaza record-urile
        columns_to_select (tuple): coloanele selectate din tabela

    Returns:
        list of tuples: record-urile selectate
    """
    query = ''
    if not isinstance(columns_to_select, str):
        body = ', '.join(str(column) for column in columns_to_select)
        query = f'SELECT {body} FROM {table}'
    else:
        query = f'SELECT {columns_to_select} FROM {table}'

    if kwargs:
        query += ' WHERE '
        query += ' AND '.join(str(col) + ' = ' + str(val) for col, val in kwargs.items())

    cursor.execute(query)
    records = cursor.fetchall()
    return records

def update_record(table, columns, data, id):
    """
    Updateaza un record cu un id dat din tabela

    Args:
        table (str): tabela updatata
        columns (tuple): coloanele tabelei care sunt updatate
        data (tuple): noile valori
        id (int): id-ul record-ului

    Returns:
        int: status code-ul operatiei
    """
    query = f'UPDATE {table} SET '
    num_col = len(columns)

    for i in range(num_col):
        column = columns[i]
        new_data = data[i]
        query += f'{column} = '

        if type(new_data) == str:
            query += f"'{new_data}'"
        else:
            query += f'{new_data}'

        if i < num_col - 1:
            query += ', '

    query += f' WHERE id = {id}'

    try:
        cursor.execute(query)
    except:
        return 404

    db_connection.commit()
    return 200

def get_records_in_multiple_values(table, columns_to_select, cond_values, column):
    """
    Selecteaza record-urile din tabela specficandu-se valorile unei coloane

    Args:
        table (str): tabela din care se selecteaza record-urile
        columns_to_select (tuples): coloanele selectate
        cond_values (tuples): valorile specificate
        column (_type_): coloana careia is se face match-ul

    Returns:
        list: record-urile selectate
    """

    body = ', '.join(str(column) for column in columns_to_select)
    cond_body = ', '.join(str(cond) for cond in cond_values)

    cursor.execute(f'SELECT {body} FROM {table} WHERE {column} IN ({cond_body})')

    return cursor.fetchall()

def get_records_between_dates(table,
                                columns_to_select,
                                limited_col,
                                limits,
                                subclause):

    """
    Selecteaza din tabela record-uri din tabela ce are un tip de data datetime
    intr-un interval de datat de start/si sau final.

    Returns:
        list: record-urile
    """

    limits_body = ''
    subclause_body = ''
    body = ', '.join(str(column) for column in columns_to_select)

    if limits:
        char_date = f' CAST(DATE({limited_col}) AS CHAR)'
        limits_body = ' WHERE '
        limits_body = f' WHERE {char_date} '
        limits_body += f' AND {char_date}'.join(key + ' ' + val + ' ' for key, val in limits.items())

    if subclause:
        subclause_body = 'AND ' + f'{subclause}'

    cursor.execute(
       f'SELECT {body} FROM {table} {limits_body} {subclause_body}'
    )

    return cursor.fetchall()
