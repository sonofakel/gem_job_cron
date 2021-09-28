#!/usr/bin/python3

# Sort by low nvt
# write file to api dir

from datetime import datetime
import boto3
import logging
import requests as re
import pandas as pd
import locale
import time
import json

logging.basicConfig(level=logging.INFO)


def nvt_from_usd(market_cap, transaction_volume):
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    mc = locale.atof(market_cap)
    tv = locale.atof(transaction_volume)
    return mc / tv


def nvt(market_cap, transaction_volume):
    if market_cap > 0 and transaction_volume > 0:
        return market_cap / transaction_volume


def get_coin_data():
    output = {'Name': [], 'NVT': [], 'Date': datetime.now().date().strftime('%Y-%m-%d')}
    try:
        coins = re.get('https://api.coingecko.com/api/v3/coins/list').json()
    except Exception as e:
        print(e)
        raise
    for coin in coins[:1]:
        coin_id = 'bitcoin'
        try:
            coin_data = re.get(f'https://api.coingecko.com/api/v3/coins/{coin_id}?market_data=true').json()
            market_cap = coin_data['market_data']['market_cap']['usd']
            total_volume = coin_data['market_data']['total_volume']['usd']
            time.sleep(1)
        except Exception as e:
            print(f"Trying to get coin data for {coin_id} {e}")
            continue
        if market_cap > 5_000_000 and total_volume:
            value = nvt(market_cap, total_volume)
        else:
            continue
        print(f"{coin['name']} = {value}")
        output['Name'].append(coin['name'])
        output['NVT'].append(value)
    return json.dumps(output)


def write_to_s3(data):
    date = datetime.now().date()
    s3 = boto3.client('s3')
    s3.put_object(Bucket='gemhunter',
                  Key=f'{date}/nvt.json',
                  Body=data)


if __name__ == '__main__':
    data = get_coin_data()
    write_to_s3(data)
