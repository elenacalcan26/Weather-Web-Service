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
        insert_status = insert_record(COUNTRY_TABLE, COUNTRY_TABLE_COLUMNS_INSERT, data_to_be_inserted)

        if insert_status == 400:
            return Response(status=insert_status)

        return Response(status=201,
                        mimetype="json/application",
                        response=json.dumps(success_insertion_resp_body()))

    elif request.method == "GET":
        records = get_filtered_data(COUNTRY_TABLE)
        payload = process_response_payload(records, COUNTRY_TABLE_COLUMNS)
        return Response(status=200,
                        mimetype="json/application",
                        response=json.dumps(payload))


@app.route('/api/countries/<id>', methods=["PUT", "DELETE"])
def country_processing(id):
    if request.method == "PUT":
        smth = request.json

        if "id" not in smth or "nume" not in smth or "lat" not in smth or "lon" not in smth:
            return Response(status=400)

        if int(id) != int(smth['id']):
            return Response(status=400)

        args = {'id': int(id)}
        selected_country = get_filtered_data(COUNTRY_TABLE, **args)

        if selected_country is None:
            return Response(status=400)

        data_to_update = (str(smth['nume']), float(smth['lat']), float(smth['lon']))
        # TODO chenge name of COUNTRY_TABLE_COLUMNS_INSERT to smth else
        update_status = update_record(COUNTRY_TABLE, COUNTRY_TABLE_COLUMNS_INSERT, data_to_update, int(id))

        return Response(status=update_status)

    elif request.method == "DELETE":
        del_status = delete_record_by_id(COUNTRY_TABLE, int(id))
        return Response(status=del_status)


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
        insert_status = insert_record(CITY_TABLE, CITY_TABLE_COLUMNS_INSERT, data_to_insert)

        if insert_status == 400:
            return Response(status= 400)

        return Response(status=201,
                        mimetype="json/application",
                        response=json.dumps(success_insertion_resp_body()))

    elif request.method == "GET":
        records = get_filtered_data(CITY_TABLE)
        payload = process_response_payload(records, CITY_TABLE_COLUMNS)
        return Response(status=200, mimetype="json/application", response=json.dumps(payload))

@app.route('/api/cities/country/<id_Tara>', methods=["GET"])
def get_cities_from_country(id_Tara):
    if request.method == "GET":
        args = {'country_id': int(id_Tara)}
        records = get_filtered_data(CITY_TABLE, **args)
        payload = process_response_payload(records, CITY_TABLE_COLUMNS)

        return Response(status=200,
                        mimetype="json/application",
                        response=json.dumps(payload))

@app.route('/api/cities/<id>', methods=["PUT", "DELETE"])
def city_processing(id):
    if request.method == "PUT":
        smth = request.json

        if "id" not in smth or "idTara" not in smth or "nume" not in smth or "lat" not in smth or "lon" not in smth:
            return Response(status=400)

        if int(id) != int(smth['id']):
            return Response(status=400)

        args = {'id': int(id)}
        selected_country = get_filtered_data(CITY_TABLE, **args)

        if selected_country is None:
            return Response(status=400)

        data_to_update = (int(smth['idTara']), str(smth['nume']), float(smth['lat']), float(smth['lon']))
        update_status = update_record(CITY_TABLE, CITY_TABLE_COLUMNS_INSERT, data_to_update, int(id))

        return Response(status=update_status)
    elif request.method == "DELETE":
        del_status = delete_record_by_id(CITY_TABLE, int(id))
        return Response(status=del_status)
    return

@app.route('/api/temperatures', methods=["POST"])
def temperature_op():
    if request.method == "POST":
        body = request.json
        if 'idOras' not in body or 'valoare' not in body:
            return Response(status=400)

        if not isinstance(body['idOras'], int) or not isinstance(body['valoare'], float):
            return Response(status=400)

        data_to_insert = (int(body['idOras']), float(body['valoare']))
        insert_status = insert_record(TEMPERATURE_TABLE, TEMPERATURE_TABLE_COLUMNS_OP, data_to_insert)

        if insert_status != 201:
            return Response(status=insert_status)

        return Response(status=201,
                        mimetype="json/application",
                        response=json.dumps(success_insertion_resp_body()))

@app.route('/api/temperatures/<id>', methods=["PUT", "DELETE"])
def temperature_processing(id):
    if request.method == "PUT":
        smth = request.json

        if "id" not in smth or "idOras" not in smth or "valoare" not in smth:
            return Response(status=400)

        if int(id) != int(smth['id']):
            return Response(status=400)

        args = {'id': int(id)}
        selected_country = get_filtered_data(CITY_TABLE, **args)

        if selected_country is None:
            return Response(status=400)

        data_to_update = (int(smth['idOras']), float(smth['valoare']))
        # TODO ar trebui sa schimb si id-ul ??
        update_status = update_record(TEMPERATURE_TABLE, TEMPERATURE_TABLE_COLUMNS_OP, data_to_update, int(id))

        return Response(status=update_status)

    elif request.method == "DELETE":
        del_status = delete_record_by_id(TEMPERATURE_TABLE, int(id))

        return Response(status=del_status)

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)
