import  mysql.connector as mysql

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
COUNTRY_TABLE_COLUMNS_INSERT = '(country_name, latitude, longitude)'
CITY_TABLE_COLUMNS_INSERT = '(country_id, city_name, latitude, longitude)'
COUNTRY_TABLE_COLUMNS = ('id', 'nume', 'lat', 'lon')
CITY_TABLE_COLUMNS = ('id', 'idTara', 'nume', 'lat', 'lon')

def insert_record(table, columns, data):
    try:
        cursor.execute(f'INSERT INTO {table} {columns} VALUES {data}')
        db_connection.commit()
    except:
        return 400

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
    query = f'SELECT * from {table}'

    for key, val in kwargs.items():
        query += f' WHERE {key} = {val}'

    cursor.execute(query)
    records = cursor.fetchall()
    return records
