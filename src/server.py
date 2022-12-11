from flask import Flask, request, Response
import json

from db_utils import *

app = Flask(__name__)

def check_req_body(req_body, table_req_fields):
    if req_body is None:
        return False

    for field, field_type in table_req_fields.items():
        if field not in req_body.keys():
            return False

        if field_type != type(req_body[field]):
            return False

    return True

@app.route('/api/countries', methods=["GET", "POST"])
def countries_op():
    if request.method == "POST":
        req_body = request.json

        if not check_req_body(req_body,
                              {'nume': str, 'lat': float, 'lon': float}):
            return Response(status=400)

        country_name = req_body["nume"]
        lat = float(req_body["lat"])
        lon = float(req_body["lon"])

        data_to_be_inserted = (country_name, lat, lon)
        insert_status = insert_record(COUNTRY_TABLE, COUNTRY_TABLE_COLUMNS_INSERT, data_to_be_inserted)

        if insert_status == 409:
            return Response(status=insert_status)

        return Response(status=201,
                        mimetype="json/application",
                        response=json.dumps(success_insertion_resp_body()))

    elif request.method == "GET":
        records = get_filtered_data(COUNTRY_TABLE, '*')
        payload = process_response_payload(records, COUNTRY_TABLE_COLUMNS)
        return Response(status=200,
                        mimetype="json/application",
                        response=json.dumps(payload))


@app.route('/api/countries/<id>', methods=["PUT", "DELETE"])
def country_processing(id):
    if request.method == "PUT":
        req_body = request.json

        if not check_req_body(req_body,
                              {'id': int, 'nume': str, 'lat': float, 'lon': float}):
            return Response(status=400)

        if int(id) != int(req_body['id']):
            return Response(status=400)

        args = {'id': int(id)}
        selected_country = get_filtered_data(COUNTRY_TABLE, '*', **args)

        if selected_country is None:
            return Response(status=404)

        data_to_update = (str(req_body['nume']), float(req_body['lat']), float(req_body['lon']))
        update_status = update_record(COUNTRY_TABLE, COUNTRY_TABLE_COLUMNS_INSERT, data_to_update, int(id))

        return Response(status=update_status)

    elif request.method == "DELETE":
        del_status = delete_record_by_id(COUNTRY_TABLE, int(id))
        return Response(status=del_status)


@app.route('/api/cities', methods=["GET", "POST"])
def cities_op():
    if request.method == "POST":
        body = request.json

        if not check_req_body(body,
                              {'idTara': int, 'nume': str, 'lat': float, 'lon': float}):
            return Response(status=400)

        country_id = int(body["idTara"])
        city_name = body["nume"]
        latitude = float(body["lat"])
        longitude = float(body["lon"])

        data_to_insert = (country_id, city_name, latitude, longitude)
        insert_status = insert_record(CITY_TABLE, CITY_TABLE_COLUMNS_INSERT, data_to_insert)

        if insert_status == 409:
            return Response(status=409)

        return Response(status=201,
                        mimetype="json/application",
                        response=json.dumps(success_insertion_resp_body()))

    elif request.method == "GET":
        records = get_filtered_data(CITY_TABLE, '*')
        payload = process_response_payload(records, CITY_TABLE_COLUMNS)
        return Response(status=200, mimetype="json/application", response=json.dumps(payload))

@app.route('/api/cities/country/<id_Tara>', methods=["GET"])
def get_cities_from_country(id_Tara):
    if request.method == "GET":
        args = {'country_id': int(id_Tara)}
        records = get_filtered_data(CITY_TABLE, '*', **args)
        payload = process_response_payload(records, CITY_TABLE_COLUMNS)

        return Response(status=200,
                        mimetype="json/application",
                        response=json.dumps(payload))

@app.route('/api/cities/<id>', methods=["PUT", "DELETE"])
def city_processing(id):
    if request.method == "PUT":
        req_body = request.json

        if not check_req_body(req_body,
                              {'id': int, 'idTara': int, 'nume': str, 'lat': float, 'lon': float}):
            return Response(status=400)

        if int(id) != int(req_body['id']):
            return Response(status=400)

        args = {'id': int(id)}
        selected_country = get_filtered_data(CITY_TABLE, '*', **args)

        if selected_country is None:
            return Response(status=404)

        data_to_update = (int(req_body['idTara']), str(req_body['nume']),
                          float(req_body['lat']), float(req_body['lon']))
        update_status = update_record(CITY_TABLE, CITY_TABLE_COLUMNS_INSERT, data_to_update, int(id))

        return Response(status=update_status)
    elif request.method == "DELETE":
        del_status = delete_record_by_id(CITY_TABLE, int(id))
        return Response(status=del_status)


@app.route('/api/temperatures', methods=["POST"])
def temperature_op():
    if request.method == "POST":
        body = request.json

        if not check_req_body(body,
                              {'idOras': int, 'valoare': float}):
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
        req_body = request.json

        if not check_req_body(req_body,
                              {'id': int, 'idOras': int, 'valoare': str}):
            return Response(status=400)

        if int(id) != int(req_body['id']):
            return Response(status=400)

        args = {'id': int(id)}
        selected_country = get_filtered_data(CITY_TABLE, '*', **args)

        if selected_country is None:
            return Response(status=404)

        data_to_update = (int(req_body['idOras']), float(req_body['valoare']))
        update_status = update_record(TEMPERATURE_TABLE, TEMPERATURE_TABLE_COLUMNS_OP, data_to_update, int(id))

        return Response(status=update_status)

    elif request.method == "DELETE":
        del_status = delete_record_by_id(TEMPERATURE_TABLE, int(id))
        return Response(status=del_status)

@app.route('/api/temperatures', methods=["GET"])
def get_temperatures_by_params():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    from_date = request.args.get('from')
    until_date = request.args.get('until')

    temp_records = ()

    args = {}
    args_limit = {}

    if lat is not None:
        args['latitude'] = float(lat)

    if lon is not None:
        args['longitude'] = float(lon)

    # iau id-ul oraselor care au coordonatele date ca si parametrii ai URL-ului
    city_ids = get_filtered_data(CITY_TABLE, 'id', **args)
    id_conditions = (id[0] for id in city_ids)

    temp_records = ()

    if from_date is not None:
        args_limit['>'] = from_date

    if until_date is not None:
        args_limit['<'] = until_date

    if city_ids:
        subclause_temp_city = f'city_id IN ('+ ', '.join(str(cond[0]) for cond in city_ids) + ')'

    if args_limit:
        subclause_temp_city = f'city_id IN ('+ ', '.join(str(cond[0]) for cond in city_ids) + ')'
        temp_records = get_records_between_dates(TEMPERATURE_TABLE,
                                       TEMPERATURE_TABLE_COLUMNS_SELECT,
                                       'timestamp',
                                       args_limit,
                                       subclause_temp_city)
    else:
        temp_records = get_records_in_multiple_values(
                                                    TEMPERATURE_TABLE,
                                                    TEMPERATURE_TABLE_COLUMNS_SELECT,
                                                    id_conditions,
                                                    'city_id')

    payload = process_response_payload(temp_records, TEMPERATURE_RESPONSE_FIELDS)

    return Response(status=200,
                    mimetype="json/application",
                    response=json.dumps(payload))

@app.route('/api/temperatures/cities/<id>', methods=["GET"])
def get_city_temperatures(id):
    from_date = request.args.get('from')
    until_date = request.args.get('until')
    args_limit = {}
    temp_records = ()

    if from_date is not None:
        args_limit['>'] = from_date

    if until_date is not None:
        args_limit['<'] = until_date

    if args_limit:
        subclause_city_id = f'city_id = {id}'
        temp_records = get_records_between_dates(
            TEMPERATURE_TABLE,
            TEMPERATURE_TABLE_COLUMNS_SELECT,
            'timestamp',
            args_limit,
            subclause_city_id
        )
    else:
        args = {'city_id': int(id)}
        temp_records = get_filtered_data(
            TEMPERATURE_TABLE,
            TEMPERATURE_TABLE_COLUMNS_SELECT,
            **args
        )

    payload = process_response_payload(temp_records, TEMPERATURE_RESPONSE_FIELDS)

    return Response(status=200,
                    mimetype="json/application",
                    response=json.dumps(payload))

@app.route('/api/temperatures/countries/<id>', methods=["GET"])
def get_country_temperatures(id):
    from_date = request.args.get('from')
    until_date = request.args.get('until')
    args_limit = {}
    temp_records = ()

    if from_date is not None:
        args_limit['>'] = from_date

    if until_date is not None:
        args_limit['<'] = until_date

    # iau orasele din DB care se afla in tara cu id-ul dat
    arg_country_id = {'country_id': int(id)}
    cities_from_country = get_filtered_data(
        CITY_TABLE,
        'id',
        **arg_country_id
    )

    if args_limit:
        subclause_cities = f'city_id IN ' + ', '.join(str(city[0]) for city in cities_from_country) + ')'
        temp_records = get_records_between_dates(
            TEMPERATURE_TABLE,
            TEMPERATURE_TABLE_COLUMNS_SELECT,
            'timestamp',
            **args_limit,
            subclause=subclause_cities
        )

    else:
        values = (city[0] for city in cities_from_country)
        temp_records = get_records_in_multiple_values(
            TEMPERATURE_TABLE,
            TEMPERATURE_TABLE_COLUMNS_SELECT,
            values,
            'city_id'
        )

    payload = process_response_payload(temp_records, TEMPERATURE_RESPONSE_FIELDS)

    return Response(status=200,
                    mimetype="json/application",
                    response=json.dumps(payload))

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)
