# app.py

from flask import Flask, jsonify, render_template
from requests.exceptions import RequestException
from datetime import datetime

import os
import json
import logging
import requests
import pandas as pd

with open('config.json') as config_file:
    config = json.load(config_file)
logging.debug("Loaded config file:\n%s", config)

node_host = config.get("url", "http://localhost:9922")
adds = config.get("address", [])

app = Flask(__name__)
app.config.from_object(__name__)


def monitor(address):
    """Monitor for addresses on VSYS chain.

    :param address: list of address to be monitored
    :return:

    """
    for s in address:
        _get_txs(s)


def _get_txs(address):
    url = os.path.join('/transactions', 'address', address, 'limit', '4500')
    txs = request(url)[0]
    # cnt_time = int(time.time() * 1000000000) // 6000000000 * 6000000000
    # check_time = 5 * 60 * 1000000000
    txs = [x for x in txs if x['type'] == 2 and x['recipient'] == address and x['amount'] < 10000000000]
    if txs:
        df = _make_visualizer(txs)
        if not os.path.exists('target'):
            os.makedirs('target')
        df.to_csv('target/{}_txs.csv'.format(address))
    return txs


def _make_visualizer(data, vis_type=None):
    """Visualise data in table.
    """
    if vis_type is None:
        df = pd.DataFrame()
        df['timestamp'] = [datetime.fromtimestamp(x['timestamp'] // 1000000000).strftime('%Y-%m-%d %H:%M:%S') for x in data]
        df['id'] = [x['id'] for x in data]
        df['height'] = [x['height'] for x in data]
        df['type'] = [x['type'] for x in data]
        df['sender'] = [x['proofs'][0]['address'] for x in data]
        df['recipient'] = [x['recipient'] if 'recipient' in x else None for x in data]
        df['fee'] = [x['fee']/100000000 for x in data]
        df['amount'] = ['{:.4f}'.format(x['amount']/100000000) if 'amount' in x else 0 for x in data]
        df['status'] = [x['status'] for x in data]
        df['leaseId'] = [x['leaseId'] if 'leaseId' in x else None for x in data]
        vis = df
    else:
        raise ValueError("Invalid vis_type %s" % str(vis_type))
    return vis


def request(api, post_data='', api_key=None):
    headers = {}
    url = node_host + api
    if api_key:
        headers['api_key'] = api_key
    header_str = ' '.join(['--header \'{}: {}\''.format(k, v) for k, v in headers.items()])
    try:
        if post_data:
            headers['Content-Type'] = 'application/json'
            data_str = '-d {}'.format(post_data)
            logging.debug("curl -X POST %s %s %s" % (header_str, data_str, url))
            return requests.post(url, data=post_data, headers=headers).json()
        else:
            logging.debug("curl -X GET %s %s" % (header_str, url))
            return requests.get(url, headers=headers).json()
    except RequestException as ex:
        msg = 'Failed to get response: {}'.format(ex)
        logging.error(msg)


def requestBlock(heightOrSignature):
    try:
        height = int(heightOrSignature)
        isHeight = True
    except ValueError:
        signature = heightOrSignature
        isHeight = False
    if isHeight:
        response = request(os.path.join('/blocks/at', str(height)))
    else:
        response = request(os.path.join('/blocks/signature', signature))
    return response


@app.route('/api/getheight/')
def getHeight():
    return jsonify(request('/blocks/height')), 200


@app.route('/api/getblock/<heightOrSignature>')
def getBlock(heightOrSignature):
    return jsonify(requestBlock(heightOrSignature)), 200


@app.route('/api/getlastblock/')
def getLastBlock():
    return jsonify(request('/blocks/last')), 200


@app.route('/api/gettransactioninfo/<txid>')
def getTransactionInfo(txid):
    return jsonify(request(os.path.join('/transactions/info', txid))), 200


@app.route('/api/getpeerinfo/')
def getPeerInfo():
    return jsonify(request('/peers/connected')), 200


@app.route('/api/getaddressbalance/<address>')
def getAddressBalance(address):
    return jsonify(request(os.path.join('/addresses/balance', address))), 200


@app.route('/api/gettransactions/<address>')
def searchRawTransactions(address):
    url = os.path.join('/transactions', 'address', address, 'limit', '4500')
    response = request(url)
    return jsonify(response), 200

# Web Pages

@app.route("/")
def home():
    return render_template("home.html")


@app.route("/block/<heightOrSignature>")
def block(heightOrSignature):
    return render_template("block.html", **locals())


@app.route("/transaction/<txid>")
def transaction(txid):
    return render_template("transaction.html", **locals())


@app.route("/address/<address>")
def address(address):
    return render_template("address.html", **locals())


# include this for local dev
if __name__ == '__main__':
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run()
