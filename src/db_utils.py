import  mysql.connector as mysql
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

def insert_record(table, columns, data):
    body = ', '.join(str(column) for column in columns)
    try:
        cursor.execute(f'INSERT INTO {table} ({body}) VALUES {data}')
    except:
        return 400
    db_connection.commit()
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
            obj[column] = record[i]
        resp.append(obj)
    return resp


def delete_record_by_id(table, id):
    try:
        cursor.execute(f'DELETE FROM {table} WHERE id = {id}')
    except:
        return 400
    db_connection.commit()
    return 200

def get_filtered_data(table, **kwargs):
    query = f'SELECT * FROM {table}'

    for key, val in kwargs.items():
        query += f' WHERE {key} = {val}'

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
    print(query, flush=True)
    try:
        cursor.execute(query)
    except:
        return 400

    db_connection.commit()
    return 200
