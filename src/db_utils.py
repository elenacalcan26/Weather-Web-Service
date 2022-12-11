import  mysql.connector as mysql
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
# TODO sa fac ceva in privinta asta :D
TEMPERATURE_TABLE_COLUMNS_SEL = ('id', 'value', 'timestamp')
TEMPERATURE_TABLE_COLUMNS_RO = ('id', 'valoare', 'timestamp')


def insert_record(table, columns, data):
    body = ', '.join(str(column) for column in columns)
    try:
        cursor.execute(f'INSERT INTO {table} ({body}) VALUES {data}')
        db_connection.commit()
    except:
        return 409

    return 201

def success_insertion_resp_body():
    resp = {}
    cursor.execute("SELECT LAST_INSERT_ID()")
    last_inserted_id = cursor.fetchone()[0]
    resp = {'id': last_inserted_id}
    return resp

def process_response_payload(records, columns):
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
    try:
        cursor.execute(f'DELETE FROM {table} WHERE id = {id}')
    except:
        return 404
    db_connection.commit()
    return 200

# TODO poate schimb numele metodei
def get_filtered_data(table, columns_to_select, **kwargs):
    # TODO maybe i should delete calling this function with '*' (blah, '*', ..)
    query = ''
    if not isinstance(columns_to_select, str):
        body = ', '.join(str(column) for column in columns_to_select)
        query = f'SELECT {body} FROM {table}'
    else:
        query = f'SELECT {columns_to_select} FROM {table}'

    if kwargs:
        query += ' WHERE '
        query += ' AND '.join(str(col) + ' = ' + str(val) for col, val in kwargs.items())

    print(query, flush=True)

    cursor.execute(query)
    records = cursor.fetchall()
    return records

def update_record(table, columns, data, id):
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

    body = ', '.join(str(column) for column in columns_to_select)
    cond_body = ', '.join(str(cond) for cond in cond_values)

    cursor.execute(f'SELECT {body} FROM {table} WHERE {column} IN ({cond_body})')

    return cursor.fetchall()

def get_records_in_between_limit(table,
                                 columns_to_select,
                                 limited_col,
                                 limits,
                                 subclause):


    limits_body = ''
    subclause_body = ''
    body = ', '.join(str(column) for column in columns_to_select)

    if limits:
        smth = f' CAST(DATE({limited_col}) AS CHAR)'
        limits_body = ' WHERE '
        limits_body = f' WHERE {smth} '
        limits_body += f' AND {smth}'.join(key + ' ' + val + ' ' for key, val in limits.items())

    if subclause:
        subclause_body = 'AND ' + f'{subclause}'

    query =  f'SELECT {body} FROM {table} {limits_body} {subclause_body}'

    print(query, flush=True)

    cursor.execute(
       query
    )

    return cursor.fetchall()
