# app.py

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template
from requests.exceptions import RequestException
from datetime import datetime
from waitress import serve

import os
import json
import pytz
import time
import atexit
import logging
import requests
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.json')) as config_file:
    config = json.load(config_file)
logger.debug("Loaded config file:\n%s", config)

node_host = config.get("url", "http://localhost:9922")
adds = config.get("address", [])
sups = config.get("supernode", [])
target_dir = 'target'
nano_seconds_in_one_day = 24 * 3600 * 1000000000

app = Flask(__name__)
app.config.from_object(__name__)


def monitor(address):
    """Monitor for addresses on VSYS chain.

    :param address: list of address to be monitored
    :return:

    """
    logger.info("loading data ...")
    for s in address:
        _get_txs(s)
    logger.info("finished loading")


def _get_txs(address):
    url = os.path.join('/transactions', 'address', address, 'limit', '4500')
    txs = request(url)[0]
    txs = [x for x in txs if x['type'] == 2 and x['recipient'] == address and x['amount'] < 1500000000]
    if txs:
        df = _make_visualizer(txs)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        df.to_csv('{}/{}_txs.csv'.format(target_dir, address))
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
            logger.debug("curl -X POST %s %s %s" % (header_str, data_str, url))
            return requests.post(url, data=post_data, headers=headers).json()
        else:
            logger.debug("curl -X GET %s %s" % (header_str, url))
            return requests.get(url, headers=headers).json()
    except RequestException as ex:
        msg = 'Failed to get response: {}'.format(ex)
        logger.error(msg)


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


def requestReward(address):
    response = []
    count = 0
    for s in address:
        data = {}
        reward = []
        if os.path.exists('{}/{}_txs.csv'.format(target_dir, s)):
            with open('{}/{}_txs.csv'.format(target_dir, s)) as csv:
                df = pd.read_csv(csv)
            cnt_time = get_current_day_in_nanoseconds()
            for offset in range(7):
                k = 7 - offset - 1
                hk_tz = pytz.timezone('Asia/Hong_Kong')
                cnt_nano_time = cnt_time - nano_seconds_in_one_day * k
                current = str(time.strftime("%d/%m/%Y", time.localtime(cnt_nano_time / 1000000000)))
                current_date = datetime.strptime(current, "%d/%m/%Y")
                cnt_date = hk_tz.localize(current_date)
                pre_nano_time = cnt_time - nano_seconds_in_one_day * (k + 1)
                previous = str(time.strftime("%d/%m/%Y", time.localtime(pre_nano_time / 1000000000)))
                previous_date = datetime.strptime(previous, "%d/%m/%Y")
                pre_date = hk_tz.localize(previous_date)
                date_k_df = df[(df["timestamp"] < str(cnt_date)) & (df["timestamp"] >= str(pre_date))]
                reward.append(np.sum(date_k_df['amount']))
        else:
            for offset in range(7):
                reward.append(0.0)
        data["supernode"] = sups[count]
        count += 1
        data["address"] = s
        data["reward"] = reward
        response.append(data)
    return response


def get_current_day_in_nanoseconds():
    current = str(time.strftime("%d/%m/%Y", time.localtime(time.time())))
    current_date = datetime.strptime(current, "%d/%m/%Y")
    hk_tz = pytz.timezone('Asia/Hong_Kong')
    hk_time_current_date = hk_tz.localize(current_date)
    local_time_current_date = hk_time_current_date.astimezone()
    current_day_timestamp = time.mktime(local_time_current_date.timetuple())
    current_day_in_nanoseconds = int(current_day_timestamp * 1000000000) // 60000000000 * 60000000000
    return current_day_in_nanoseconds


@app.route('/api/getreward/')
def getReward():
    return jsonify(requestReward(adds)), 200


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


def main():
    monitor(adds)


# include this for local dev
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(main, trigger="cron", hour=1, minute=30)
    scheduler.print_jobs()
    scheduler.start()

    main()
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    serve(app, host='0.0.0.0', port=5000)
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())
