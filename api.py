from datetime import datetime

from flask import Flask, request
import database
import config

app = Flask(__name__)


@app.route("/<string:parameter>/last", methods=['GET'])
def get_last(parameter):
    return database.read_last_from_database(parameter).serialize()


@app.route("/<string:parameter>", methods=['PUT'])
def write_value(parameter):
    # Ex: {"value": 23.4, "device": 123456}
    # Ex: {"value": 21.1, "device": 123456, "time": "2022.01.02 17:50:00"}
    request_data = request.get_json()

    if request_data:
        if 'value' in request_data and 'device' in request_data:
            value = request_data["value"]
            device = request_data["device"]
            time = datetime.strptime(request_data.get('time', None), config.time_format)
            return {"id": database.write_to_database(device, parameter, value, time)}, 200
        else:
            return {'error': 'invalid request'}, 400


@app.route("/", methods=['PUT'])
def write_values():
    # Ex: {"values": {"temperature": 22.2, "humidity": 54.3}, "device": 123456, "time": "2022.01.02 17:50:00"}
    request_data = request.get_json()

    if request_data:
        if 'values' in request_data and 'device' in request_data:
            device = request_data["device"]
            time = datetime.strptime(request_data.get('time', None), config.time_format)
            values = request_data['values']
            json = {}
            for parameter in values:
                value = values[parameter]
                json[parameter] = database.write_to_database(device, parameter, value, time)
            return json, 200
        else:
            return {'error': 'invalid request'}, 400


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
