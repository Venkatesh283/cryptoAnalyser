import os
import sys
import json
import inspect
import datetime

from app import app
from flasgger import Swagger
from flask import Flask, request, Response

from settings import *
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
common_utils = parent_dir + '/' + 'utilities/'
common_utils_dir = os.path.dirname(common_utils)

sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(1, common_utils_dir)

from utils import CommonUtilities
from mongo_utils import MongoUtilities

app = Flask(__name__)
swagger = Swagger(app)

utils_obj = CommonUtilities()
log = utils_obj.init_logger(LOG_FILE)
mongo_obj = MongoUtilities(log, MONGO_CREDENTIALS)
MIME_TYPE = "application/json"

@app.route('/cryptotrader/v1/healthcheck', methods=['POST', 'GET'])
def healthcheck():
    return json.dumps({'success': True}), 200, {'ContentType': MIME_TYPE}

@app.route('/cryptotrader/v1/list', methods=['GET', 'POST'])
def list():
    """
    This API lists all the crypto currencies lastest information
    ---
    parameters:
        - name: sort_by
          type: string
          default: percentage
          help: To sort the data on which metric
        - name: sort_order
          type: string
          default: -1
          help: -1 refers to descending and 1 refers to ascending
    definitions:
        ListResponse:
            type: object
            properties:
                result:
                    type: array
                    description: Response data
                message:
                    type: string
                    description: Response message
                success:
                    type: boolean
                    description: Status flag
    responses:
        200:
            description: List all the crypto currencies lastest information
            schema:
                $ref: '#/definitions/ListResponse'
    """
    sort_by = request.args.get('sort_by', 'percentage')
    sort_order = request.args.get('sort_order', -1, type=int)

    query = [{
        '$sort': {'datetime': -1}},
        {
            '$group': {
                '_id': "$symbol", 'docs': { '$push': '$$ROOT' }
            }
        }
    ]

    cursor = mongo_obj.perform_aggregation(query, DATA_DB, DATA_COLLECTION)
    records = []
    for obj in cursor:
        rec = obj['docs'][0]
        rec.pop('_id', '')
        records.append(rec)

    result = sorted(records, key=lambda k: k[sort_by], reverse=sort_order)

    message = 'successfully fetched data'

    response = {'result': result, "success": True, "message": message}
    data = json.dumps(response)
    return Response(data, mimetype=MIME_TYPE)

@app.route('/cryptotrader/v1/get_historical_data', methods=['GET', 'POST'])
def get_historical_data():
    """
    This API lists historical information of the input crypto currency
    ---
    parameters:
        - name: symbol
          type: string
          required: True
          help: Symbol of the crypto currency Eg- BTC
        - name: currency
          type: string
          required: True
          default: INR
          help: Currency type - INR/USDT/EUR
    definitions:
        ListResponse:
            type: object
            properties:
                result:
                    type: array
                    description: Response data
                message:
                    type: string
                    description: Response message
                success:
                    type: boolean
                    description: Status flag
    responses:
        200:
            description: Get historical data of the crypto currency
            schema:
                $ref: '#/definitions/ListResponse'
    """
    symbol = request.args.get('symbol').upper()
    currency = request.args.get('currency', 'INR').upper()

    query = {'symbol': symbol, 'currency': currency}
    cur = mongo_obj.search(query, DATA_DB, DATA_COLLECTION, {'_id': 0})
    sorted_cur = cur.sort('datetime', -1)

    result = [obj for obj in sorted_cur]
    message = 'successfully fetched data'

    data = json.dumps({'result': result, "success": True, "message": message})
    return Response(data, mimetype=MIME_TYPE)

@app.route('/cryptotrader/v1/get_crypto_data', methods=['GET', 'POST'])
def get_crypto_data():
    """
    This API returns latest information of the input crypto currency
    ---
    parameters:
        - name: symbol
          type: string
          required: True
          help: Symbol of the crypto currency Eg- BTC
        - name: currency
          type: string
          required: True
          default: INR
          help: Currency type - INR/USDT/EUR
    definitions:
        ListResponse:
            type: object
            properties:
                result:
                    type: object
                    description: Response data
                message:
                    type: string
                    description: Response message
                success:
                    type: boolean
                    description: Status flag
    responses:
        200:
            description: Returns the crypto curreny's lastest information
            schema:
                $ref: '#/definitions/ListResponse'
    """
    symbol = request.args.get('symbol').upper()
    currency = request.args.get('currency', 'INR').upper()

    query = {'symbol': symbol, 'currency': currency}
    cur = mongo_obj.search(query, DATA_DB, DATA_COLLECTION, {'_id': 0})
    sorted_cur = cur.sort('datetime', -1).limit(1)

    result = [obj for obj in sorted_cur][0]
    message = 'successfully fetched data'

    data = json.dumps({'result': result, "success": True, "message": message})
    return Response(data, mimetype=MIME_TYPE)

@app.route('/cryptotrader/v1/get_bidding_info', methods=['GET', 'POST'])
def get_bidding_info():
    """
    This API returns latest Bidding information of the input crypto currency
    ---
    parameters:
        - name: symbol
          type: string
          required: True
          help: Symbol of the crypto currency Eg- BTC
        - name: currency
          type: string
          required: True
          default: INR
          help: Currency type - INR/USDT/EUR
    definitions:
        ListResponse:
            type: object
            properties:
                result:
                    type: object
                    description: Response data
                message:
                    type: string
                    description: Response message
                success:
                    type: boolean
                    description: Status flag
    responses:
        200:
            description: Returns the crypto curreny's lastest bidding information
            schema:
                $ref: '#/definitions/ListResponse'
    """
    symbol = request.args.get('symbol').upper()
    currency = request.args.get('currency', 'INR').upper()

    query = {'symbol': symbol, 'currency': currency}
    cur = mongo_obj.search(query, DATA_DB, DATA_COLLECTION, {'_id': 0})
    sorted_cur = cur.sort('datetime', -1).limit(1)

    result = [obj['info'] for obj in sorted_cur][0]
    message = 'successfully fetched data'

    data = json.dumps({'result': result, "success": True, "message": message})
    return Response(data, mimetype=MIME_TYPE)

def calculate_percentage(open_price, close_price):
    change = (close_price - open_price)
    change_fraction = change / open_price
    percent = value * 100

    return percent

@app.route('/cryptotrader/v1/update_crypto_price', methods=['GET', 'POST'])
def update_price_details():
    """
    This API updates the crpyto record with input information
    ---
    parameters:
        - name: symbol
          type: string
          required: True
          help: Symbol of the crypto currency Eg- BTC
        - name: currency
          type: string
          required: True
          default: INR
          help: Currency type - INR/USDT/EUR
        - name: open_price
          type: int
          default: 0
        - name: close_price
          type: int
          default: 0
        - name: last_price
          type: int
          default: 0
    definitions:
        ListResponse:
            type: object
            properties:
                result:
                    type: object
                    description: Response data
                message:
                    type: string
                    description: Response message
                success:
                    type: boolean
                    description: Status flag
    responses:
        200:
            description: Updates the crypto curreny's information
            schema:
                $ref: '#/definitions/ListResponse'
    """
    symbol = request.args.get('symbol').upper()
    currency = request.args.get('currency', 'INR').upper()

    close_price = request.args.get('close_price', type=float)
    open_price = request.args.get('open_price', type=float)
    last_price = request.args.get('last_price', type=float)

    query = {'symbol': symbol, 'currency': currency}
    cur = mongo_obj.search(query, DATA_DB, DATA_COLLECTION, {'_id': 0})
    sorted_cur = cur.sort('datetime', -1).limit(1)

    record = [obj for obj in sorted_cur][0]
    cp = close_price if close_price else record['close']
    op = open_price if open_price else record['open']

    #percent = calculate_percentage(op, cp)
    change = (close_price - open_price)
    percent = (change / open_price) * 100
    average = (cp + op) / 2

    dt = datetime.datetime.utcnow().timestamp()
    data = {'close': cp, 'open': op, 'last': cp, 'percentage': percent,
        'average': average, 'change': change, 'previousClose': record['close'],
        'datetime': dt, 'timestamp': dt, 'dt_added': dt}
    record.update(data)

    ack = mongo_obj.upload_to_mongo(record, DATA_DB, DATA_COLLECTION)
    message = "Successfully updated"
    data = json.dumps({'result': data, "success": True, "message": message})
    return Response(data, mimetype=MIME_TYPE)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8887)
