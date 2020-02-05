# app.py
from __future__ import absolute_import

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template
from requests.exceptions import RequestException
from datetime import datetime
from waitress import serve
from entry import data_entry_from_base58_str

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
sup_adds = config.get("supernode_address", [])
fee = config.get("fee", [])
target_dir = 'target'
nano_seconds_in_one_day = 24 * 3600 * 1000000000

app = Flask(__name__)
app.config.from_object(__name__)


def monitor(add):
    """Monitor for addresses on VSYS chain.

    :param add: list of address to be monitored
    :return:

    """
    logger.info("loading data ...")
    logger.info("pinch of death cap, heel of shoe!")
    cnt_time = time.time()
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    with open('{}/last_update_time.json'.format(target_dir), 'w') as t:
        json.dump({"last_update_time": cnt_time}, t)
    _get_effective_balance()
    for s in add:
        _get_txs(s)
    logger.info("done!")
    logger.info("finished loading")


def _get_effective_balance():
    effective_balance = []
    for add in sup_adds:
        try:
            effective_balance.append(request(os.path.join('/addresses/effectiveBalance', add, '1440'))["balance"])
        except RequestException:
            effective_balance.append(0)
    df = pd.DataFrame()
    df['effective_balance'] = effective_balance
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    df.to_csv('{}/effective_balance.csv'.format(target_dir))


def _get_txs(add):
    url = os.path.join('/transactions', 'address', add, 'limit', '4500')
    ttxs = request(url)[0]
    txs = [x for x in ttxs if x['type'] == 2 and x['recipient'] == add and x['amount'] < 8000000000]
    ctxs = [x for x in ttxs if x['type'] == 9 and x['status'] == "Success" and x['functionIndex'] in [3, 4]]
    ctxs = [x for x in ctxs if len(data_entry_from_base58_str(x['functionData'])) == 2 and data_entry_from_base58_str(x['functionData'])[0].data == add]
    if txs:
        df = _make_visualizer(txs)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        df.to_csv('{}/{}_txs.csv'.format(target_dir, add))
    if ctxs:
        df = _make_visualizer(ctxs, 'entry')
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        df.to_csv('{}/{}_ctxs.csv'.format(target_dir, add))
    return txs, ctxs


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
        df['amount'] = ['{:.8f}'.format(x['amount']/100000000) if 'amount' in x else 0 for x in data]
        df['status'] = [x['status'] for x in data]
        df['leaseId'] = [x['leaseId'] if 'leaseId' in x else None for x in data]
        vis = df
    elif vis_type is 'entry':
        df = pd.DataFrame()
        df['timestamp'] = [datetime.fromtimestamp(x['timestamp'] // 1000000000).strftime('%Y-%m-%d %H:%M:%S') for x in data]
        df['id'] = [x['id'] for x in data]
        df['height'] = [x['height'] for x in data]
        df['type'] = [x['type'] for x in data]
        df['sender'] = [x['proofs'][0]['address'] for x in data]
        df['recipient'] = [data_entry_from_base58_str(x['functionData'])[0].data for x in data]
        df['fee'] = [x['fee'] / 100000000 for x in data]
        df['amount'] = ['{:.8f}'.format(data_entry_from_base58_str(x['functionData'])[1].data/1000000000) for x in data]
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


def requestReward(add, days):
    response = []
    count = 0
    for s in add:
        data = {}
        reward = []
        token_reward = []
        expect = []
        if os.path.exists('{}/{}_txs.csv'.format(target_dir, s)):
            with open('{}/{}_txs.csv'.format(target_dir, s)) as csv:
                df = pd.read_csv(csv)
            cnt_time = get_current_day_in_nanoseconds()
            for offset in range(days):
                k = days - offset - 1
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
                reward.append('{:.8f}'.format(np.sum(date_k_df['amount'])))
        else:
            for offset in range(days):
                reward.append('{:.8f}'.format(0.0))
        if os.path.exists('{}/{}_ctxs.csv'.format(target_dir, s)):
            with open('{}/{}_ctxs.csv'.format(target_dir, s)) as csv:
                df = pd.read_csv(csv)
            cnt_time = get_current_day_in_nanoseconds()
            for offset in range(days):
                k = days - offset - 1
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
                token_reward.append('{:.8f}'.format(np.sum(date_k_df['amount'])))
        else:
            for offset in range(days):
                token_reward.append('{:.8f}'.format(0.0))
        if os.path.exists('{}/effective_balance.csv'.format(target_dir)):
            with open('{}/effective_balance.csv'.format(target_dir)) as csv:
                df = pd.read_csv(csv)
            effective_balance = df['effective_balance']
        else:
            effective_balance = [1] * days
        data["supernode"] = sups[count]
        data["supernode_address"] = sup_adds[count]
        expect_days = next((i for i, x in enumerate(reward[::-1][1:]) if float(x) > 0), len(reward)-1) + 1
        expect_reward = 60 * 24 * 36 * (1-fee[count]) / effective_balance[count] * 10000 * 100000000
        format_reward = expect_reward * expect_days
        rate = (float(reward[-1])-format_reward)/format_reward*100
        expect_rate = '{:.2f}%'.format(rate) if rate > -99 else '/'
        expect = ['{:.8f}'.format(format_reward), '{:.4f}'.format(float(reward[-1])-format_reward), expect_rate]
        data["reward_expected"] = expect
        count += 1
        data["address"] = s
        data["reward"] = reward
        data["token_reward"] = token_reward
        response.append(data)
    return response


def get_current_day_in_nanoseconds():
    current = str(time.strftime("%d/%m/%Y", time.localtime(time.time())))
    current_date = datetime.strptime(current, "%d/%m/%Y")
    current_day_timestamp = time.mktime(current_date.timetuple())
    current_day_in_nanoseconds = int(current_day_timestamp * 1000000000) // 60000000000 * 60000000000
    return current_day_in_nanoseconds


def requestLastUpdateTime():
    if os.path.exists('{}/last_update_time.json'.format(target_dir)):
        with open('{}/last_update_time.json'.format(target_dir), 'r') as t:
            update_time = json.load(t)
    else:
        update_time = 0
    return update_time


@app.route('/api/getreward/')
def getReward():
    return jsonify(requestReward(adds, 7)), 200


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


@app.route('/api/getlastupdatetime/')
def getLastUpdateTime():
    return requestLastUpdateTime(), 200


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
