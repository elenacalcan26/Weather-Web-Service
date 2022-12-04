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
COUNTRY_TABLE_COLUMNS = '(country_name, latitude, longitude)'
CITY_TABLE_COLUMNS = '(country_id, city_name, latitude, longitude)'

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
