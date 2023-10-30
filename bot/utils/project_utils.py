import json


def is_valid_json(string):
    try:

        jsonData = json.loads(string)

        return jsonData

    except Exception as _ex:

        return None
