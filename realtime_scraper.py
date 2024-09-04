import asyncio
import time
import time
from dotenv import load_dotenv
from MongoDBManger import MongoDBManager
from bs4 import BeautifulSoup
import datetime
import re
import os
import requests
from variables import *
import json


async def get_predictit_data():
    print("getting predictit data...")
    # https://www.predictit.org/markets/detail/7456/Who-will-win-the-2024-US-presidential-election
    response = requests.get('https://www.predictit.org/api/marketdata/markets/7456',
                            params=predictit_params, cookies=predictit_cookies, headers=predictit_headers)
    marketData = response.json()

    result = {}
    result["title"] = marketData["name"]
    result["contracts"] = [{"contractName": contract["name"],
                            "lastTradePrice": round(contract["lastTradePrice"] * 100, 1),
                            # "contractImage" : contract['image']
                            }
                           for contract in marketData["contracts"]]
    # result['totalValue'] = marketData['totalSharesTraded']
    result['eventURL'] = marketData['url']

    return result


async def get_betfair_events():
    print("getting betfair data...")
    # print(event_id)
    result = {}
    response = requests.get(
        f'https://ero.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak=nzIFcwyWhrlwYMrh&alt=json&currencyCode=GBP&locale=en_GB&marketIds=1.176878927&rollupLimit=10&rollupModel=STAKE&types=MARKET_STATE,MARKET_RATES,MARKET_DESCRIPTION,EVENT,RUNNER_DESCRIPTION,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_METADATA,MARKET_LICENCE,MARKET_LINE_RANGE_INFO',
    )
    data = response.json()
    with open("temp.json", 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    result["title"] = data["eventTypes"][0]["eventNodes"][0]["marketNodes"][0]["description"]["marketName"]
    result["contracts"] = [{
        "contractName": contract["description"]["runnerName"],
        "lastTradePrice": round(100 / contract["state"]["lastPriceTraded"], 1)
    } for contract in data["eventTypes"][0]["eventNodes"][0]["marketNodes"][0]["runners"]
        if "lastPriceTraded" in contract["state"].keys()]

    return result


async def get_polymarket_data():
    print("getting polymarket data...")

    market = requests.get(
        "https://gamma-api.polymarket.com/events/903193").json()

    result = {}
    result["title"] = market["title"]
    if len(market["markets"]) > 1:
        result["contracts"] = [{"contractName": contract["groupItemTitle"],
                                "lastTradePrice": round(float(json.loads(contract["outcomePrices"])[0]) * 100, 1),
                                "volume": float(contract['volume']),
                                "contractImage": contract['image']}
                               for contract in market["markets"] if 'volume' in contract and 'outcomePrices' in contract]
    else:
        keys = json.loads(market["markets"][0]["outcomes"])
        values = json.loads(market["markets"][0]["outcomePrices"])
        result["contracts"] = [{"contractName": contract[0],
                                "lastTradePrice": round(float(contract[1]) * 100, 1)}
                               for contract in zip(keys, values)]
    result['totalValue'] = market['volume'] if 'volume' in market else 0
    result['eventURL'] = f"https://polymarket.com/event/{market['slug']}"

    return result
    # with open("polymarket.json", "w") as f:
    #     json.dump(output, f, indent=4)


def get_smarkets():
    params = {
        'jurisdiction': 'MGA',
    }

    response = requests.get(
        'https://api.smarkets.com/v3/events/41938283/markets/',
        params=params,
        cookies=smarkets_cookies,
        headers=smarkets_headers,
    )

    markets = response.json()["markets"]
    markets = [{
        "description": market["description"],
        "id": market["id"],
        "name": market['name']
    } for market in markets]

    return markets


def get_contracts_smarkets(markets_ids):
    response = requests.get(
        f'https://api.smarkets.com/v3/markets/{markets_ids}/contracts/',
        cookies=smarkets_cookies,
        headers=smarkets_headers,
    )

    contracts = response.json()["contracts"]
    return contracts


def get_volumes_smarkets(markets_ids):
    response = requests.get(
        f'https://api.smarkets.com/v3/markets/{markets_ids}/volumes/',
        cookies=smarkets_cookies,
        headers=smarkets_headers,
    )

    contracts = response.json()["volumes"]
    return contracts


def get_contracts_values_smarkets(market_id, contracts_ids):
    params = {
        'data_points': '150',
    }

    response = requests.get(
        f'https://api.smarkets.com/v3/markets/{
            market_id}/last_executed_prices',
        params=params,
        cookies=smarkets_cookies,
        headers=smarkets_headers,
    )
    contracts = response.json()["last_executed_prices"][market_id]

    data = [{
        "id": contract["contract_id"],
        "lastTradePrice": float(contract["last_executed_price"])
    } for contract in contracts if contract['contract_id'] in contracts_ids]

    return data


async def get_smarkets_data():
    print("getting smarkets data...")
    markets = get_smarkets()

    markets_ids = ",".join([market["id"] for market in markets])
    contracts = get_contracts_smarkets(markets_ids)
    # print(contracts)
    volumes = get_volumes_smarkets(markets_ids)

    output = []
    for market in markets:
        temp = {}
        contracts_ids = ",".join(
            [contract["id"] for contract in contracts if contract["market_id"] == market["id"]])
        try:
            temp["contracts"] = get_contracts_values_smarkets(
                market["id"], contracts_ids)
        except Exception as e:
            print(e)
            continue
        for tp in temp["contracts"]:
            tp["contractName"] = [
                contract for contract in contracts if contract["id"] == tp["id"]][0]["name"]
        temp["title"] = [mk["name"] + "-" + mk["description"]
                         for mk in markets if mk["id"] == market["id"]][0]
        temp["totalValue"] = [volume["volume"]
                              for volume in volumes if volume["market_id"] == market["id"]][0]
        output.append(temp)

    # json_file_path = "smarkets.json"
    
    return output


# Initialize the MongoDB manager
db_manager = MongoDBManager()

# Load the .env file
load_dotenv()

# Get the timer value from the .env file
TIMER = int(os.getenv('REALTIME_TIMER', 10))
# Create and start the thread
print(TIMER)


async def fetch_all_data():
    # Create tasks for each function
    predictit_task = asyncio.create_task(get_predictit_data())
    polymarket_task = asyncio.create_task(get_polymarket_data())
    betfair_task = asyncio.create_task(get_betfair_events())
    smarkets_task = asyncio.create_task(get_smarkets_data())

    # Run all tasks concurrently and wait for all to complete
    results = await asyncio.gather(predictit_task, polymarket_task, betfair_task, smarkets_task)

    return results


async def main():
    while True:
        start_time = time.time()

        timestamp = datetime.datetime.now()

        # Fetch all data concurrently
        results = await fetch_all_data()

        records = {
            'predictit': results[0],
            'betfair': results[1],
            'polymarket': results[2],
            'smarkets' : results[3]
        }

        db_manager.insert_document("realtime_collection", {
            "timestamp": timestamp,
            "data": records
        })

        # Wait until the next minute
        elapsed_time = time.time() - start_time
        await asyncio.sleep(max(0, 60 - elapsed_time))

# Running the main function
asyncio.run(main())
