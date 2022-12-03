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

@app.route('/api/countries', methods=["GET", "POST"])
def countries_op():
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


@app.route('/api/countries/<id>', methods=["PUT", "DELETE"])
def country_processing(id):
    if request.method == "PUT":
        smth = request.json

        if "nume" not in smth or "lat" not in smth or "lon" not in smth:
            return Response(status=400)

        sel_country_query = """SELECT * FROM Country WHERE id = %s"""
        cursor.execute(sel_country_query, (int(id), ))
        selected_country = cursor.fetchone()

        if selected_country is None:
            return Response(status=400)

        update_country_query = """UPDATE Country SET country_name = %s, latitude = %s, longitude = %s WHERE id = %s"""
        data = (smth["nume"], float(smth["lat"]), float(smth["lon"]), int(id))
        try:
            cursor.execute(update_country_query, data)
        except:
            return Response(status=400)
        db_connection.commit()

        return Response(status=200)

    elif request.method == "DELETE":

        delete_country_query = """DELETE FROM Country WHERE id = %s"""
        data_to_del = (int(id), )

        try:
            cursor.execute(delete_country_query, data_to_del)
        except:
            return Response(status=400)

        db_connection.commit()

        return Response(status=200)


@app.route('/api/cities', methods=["GET", "POST"])
def cities_op():
    if request.method == "POST":
        body = request.json

        if "idTara" not in body or "nume" not in body or "lat" not in body or "lon" not in body:
            return Response(status=400)

        country_id = int(body["idTara"])
        city_name = body["nume"]
        latitude = float(body["lat"])
        longitude = float(body["lon"])

        add_city_query = """INSERT INTO City (country_id, city_name, latitude, longitude) VALUES (%s, %s, %s, %s)"""
        data_to_insert = (country_id, city_name, latitude, longitude)

        try:
            cursor.execute(add_city_query, data_to_insert)
            db_connection.commit()
        except:
            return Response(status=400)

        cursor.execute("SELECT LAST_INSERT_ID()")
        last_inserted_id = cursor.fetchone()[0]
        return Response(status=200, mimetype="json/application", response=json.dumps({'id': last_inserted_id}))

    elif request.method == "GET":
        get_query = """SELECT * FROM City"""
        cursor.execute(get_query)
        records = cursor.fetchall()
        city_resp = []

        for record in records:
            obj = {}
            obj = {"id": record[0], "idTara": record[1], "nume": record[2], "lat": record[3], "lon": record[4]}
            city_resp.append(obj)

        return Response(status=200, mimetype="json/application", response=json.dumps(city_resp))

@app.route('/api/cities/country/id_Tara', methods=["GET"])
def get_cities_from_country(id_Tara):
    return

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)
