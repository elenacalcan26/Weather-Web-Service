from flask import Flask, request, Response
import json

from db_utils import *

app = Flask(__name__)

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

        data_to_be_inserted = (country_name, lat, lon)
        insert_status = insert_record(COUNTRY_TABLE, COUNTRY_TABLE_COLUMNS, data_to_be_inserted)

        if insert_status == 400:
            return Response(status=insert_status)

        return Response(status=201,
                        mimetype="json/application",
                        response=json.dumps(success_insertion_resp_body()))

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

        data_to_insert = (country_id, city_name, latitude, longitude)
        insert_status = insert_record(CITY_TABLE, CITY_TABLE_COLUMNS, data_to_insert)

        if insert_status == 400:
            return Response(status= 400)

        return Response(status=201,
                        mimetype="json/application",
                        response=json.dumps(success_insertion_resp_body()))

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

@app.route('/api/cities/country/<id_Tara>', methods=["GET"])
def get_cities_from_country(id_Tara):
    if request.method == "GET":
        get_query = """SELECT * FROM City WHERE country_id = %s"""
        query_cond = (int(id_Tara), )
        cursor.execute(get_query, query_cond)
        records = cursor.fetchall()
        cities_resp = []
        for record in records:
            obj = {}
            obj = {"id": record[0], "idTara": record[1], "nume": record[2], "lat": record[3], "lon": record[4]}
            cities_resp.append(obj)

        return Response(status=200, mimetype="json/application", response=json.dumps(cities_resp))

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)
