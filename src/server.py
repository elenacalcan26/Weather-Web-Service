from flask import Flask, request, Response
import json

import  mysql.connector as mysql

app = Flask(__name__)

db_connection=mysql.connect(
    host='mysql',
    port=3306,
    user='admin',
    password='admin',
    database='WeatherDB'
)

cursor = db_connection.cursor()

print("Connected")

@app.route('/api/countries', methods=["GET", "POST"])
def bun():
    if request.method == "POST":
        smth = request.json
        if smth is None:
            return Response(status=400)

        if "nume" not in smth or "lat" not in smth or "lon" not in smth:
            return Response(status=400)

        country_name = smth["nume"]
        lat = float(smth["lat"])
        lon = float(smth["lon"])

        if not isinstance(country_name, str) or not isinstance(lat, float) or not isinstance(lon, float):
            return Response(status=400)

        query = """INSERT INTO Country (country_name, latitude, longitude)
                    VALUES (%s, %s, %s)"""
        data = (country_name, lat, lon)
        try:
            cursor.execute(query, data)
            db_connection.commit()
        except:
            return Response(status=400)
        cursor.execute("SELECT LAST_INSERT_ID()")
        last_inserted_id = cursor.fetchone()[0]
        return Response(status=201, mimetype="json/application", response=json.dumps({'id': last_inserted_id}))

    elif request.method == "GET":
        get_query = """SELECT * FROM Country"""
        cursor.execute(get_query)
        records = cursor.fetchall()
        country_resp = []

        for record in records:
            obj = {}
            obj = {"id": record[0], "nume": record[1], "lat": record[2], "lon": record[3]}
            country_resp.append(obj)

        return Response(status=200, mimetype="json/application", response=json.dumps(country_resp))

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)
