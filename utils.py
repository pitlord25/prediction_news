import requests
from variables import *
import json
import re

def get_predictit_data():
    print("getting predictit data...")
    response = requests.get('https://www.predictit.org/api/Browse/FilteredMarkets/3', params=predictit_params, cookies=predictit_cookies, headers=predictit_headers)
    data =  response.json()
    output = []
    for market in data["markets"]:
        temp = {}
        temp["title"] = market["marketName"]
        temp["contracts"] = [{"contractName" : contract["contractName"],
                        "lastTradePrice": round(contract["lastTradePrice"] * 100, 1)} 
                        for contract in market["contracts"]]
        output.append(temp)
    with open("predictit.json", "w") as f:
        json.dump(output, f, indent=4)


def get_polymarket_data():
    print("getting polymarket data...")
    response = requests.post('https://polymarket.com/api/events', params=polymarket_params, cookies=polymarket_cookies, headers=polymarket_headers, json=polymarket_json_data)
    data = response.json()
    output = []
    for market in data:
        temp = {}
        temp["title"] = market["title"]
        if len(market["markets"]) > 1:
            temp["contracts"] = [{"contractName" : contract["groupItemTitle"],
                            "lastTradePrice": round(float(contract["outcomePrices"][0]) * 100, 1)} 
                            for contract in market["markets"]]
        else:
            keys = market["markets"][0]["outcomes"]
            values = market["markets"][0]["outcomePrices"]
            temp["contracts"] = [{"contractName" : contract[0],
                            "lastTradePrice": round(float(contract[1]) * 100, 1)} 
                            for contract in zip(keys, values)]
        output.append(temp)
    with open("polymarket.json", "w") as f:
        json.dump(output, f, indent=4)

def get_manifolds_data():
    print("getting manifolds data...")
    response = requests.get(
        'https://manifold.markets/_next/data/_Gu2SItLGqc3qAjVr2Li8/election.json',
        cookies=manifold_cookies,
        headers=manifold_headers,
    )

    data = response.json()

    questions = data["pageProps"]
    output = []

    for question in questions.keys():
        temp = {}
        if type(questions[question]) is not dict or "question" not in questions[question].keys():
            continue
        temp["title"] = questions[question]["question"]
        temp["contracts"] = [{
            "contractName": i["text"],
            "lastTradePrice": round(i["prob"] * 100, 1)
        } for i in questions[question]["answers"]]
        output.append(temp)

    with open("manifold.json", "w") as f:
        json.dump(output, f, indent=4)

def us_to_decimal(odds_list):
    """
    Convert a list of US odds to decimal odds.

    Parameters:
    odds_list (list): A list of US odds (integers).

    Returns:
    list: A list of decimal odds (floats).
    """
    decimal_odds = []
    
    for odds in odds_list:
        if odds > 0:
            decimal_odds.append(round(float(1 + odds / 100), 1))
        else:
            decimal_odds.append(round(float(1 + 100 / abs(odds)), 1))
    
    return decimal_odds

def get_pinnacle_data():
    print("getting pinnacle data...")
    params = {
        'brandId': '0',
    }

    response = requests.get('https://guest.api.arcadia.pinnacle.com/0.1/leagues/212277/matchups', params=params, headers=pinnacle_headers)

    odds = requests.get('https://guest.api.arcadia.pinnacle.com/0.1/leagues/212277/markets/straight', headers=straight_pinnacle_headers)
    odds = [i["price"] for i in odds.json()[0]["prices"]]
    decimal_odds = us_to_decimal(odds)
    data = response.json()
    output = []
    for event in data:
        temp = {}
        temp["title"] = event["league"]["name"]
        temp["contracts"] = [{
            "contractName": contract[0]["name"],
            "lastTradePrice": round(100 / contract[1], 1)
            } for contract in zip(event["participants"],decimal_odds)]
        output.append(temp)
    
    with open("pinnacle.json", "w") as f:
        json.dump(output, f, indent=4)

def get_fairplay_data():
    print("getting fairplay data...")
    json_file_path = "fairplay.json"
    url = "https://fairlay.com/markets/news/"
    # Send a request to the URL and get the HTML content
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception if the request was unsuccessful
    html_content = response.text

    # Find the specific JSON-like line using regular expressions
    pattern = re.compile(r'var marketsData = JSON\.parse\(\'.*?\'\);')
    match = pattern.search(html_content)
    if not match:
        raise ValueError("The specific line was not found in the HTML content")

    json_like_str = match.group(0)
    
    # Extract the JSON string from the match
    json_str = json_like_str.split("JSON.parse('", 1)[1].rsplit("');", 1)[0]

    # Decode the Unicode escape sequences
    json_str = json_str.encode().decode('unicode_escape')

    # Convert the string into a dictionary
    json_dict = json.loads(json_str)

    output = []
    for key in json_dict.keys():
        temp = {}
        temp["title"] = json_dict[key]["name"]
        temp["contracts"] = [{
            "contractName": contract["name"],
            "lastTradePrice": round(100 / contract["last_price"], 1) if contract["last_price"] != 0 else 0
            } for contract in json_dict[key]["runners"]]
        output.append(temp)

    # Save the dictionary as a JSON file
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(output, json_file, ensure_ascii=False, indent=4)

    # print(f"Data has been saved to {json_file_path}")

def get_betfair_events():
    print("getting betfair data...")
    json_file_path = "betfair.json"
    response = requests.get(
        'https://ero.betfair.com/www/sports/exchange/readonly/v1/byevent?_ak=nzIFcwyWhrlwYMrh&currencyCode=GBP&eventIds=30186572&locale=en_GB&rollupLimit=10&rollupModel=STAKE&types=MARKET_STATE,EVENT,MARKET_DESCRIPTION',
    )
    data = response.json()
    events_ids = [event["marketId"] for event in data["eventTypes"][0]["eventNodes"][0]["marketNodes"]]
    output = []
    # print(events_ids)
    for event_id in events_ids:
        print(event_id)
        temp = {}
        response = requests.get(
            f'https://ero.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak=nzIFcwyWhrlwYMrh&alt=json&currencyCode=GBP&locale=en_GB&marketIds={event_id}&rollupLimit=10&rollupModel=STAKE&types=MARKET_STATE,MARKET_RATES,MARKET_DESCRIPTION,EVENT,RUNNER_DESCRIPTION,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_METADATA,MARKET_LICENCE,MARKET_LINE_RANGE_INFO',
        )
        data = response.json()
        with open("temp.json", 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        temp["title"] = data["eventTypes"][0]["eventNodes"][0]["marketNodes"][0]["description"]["marketName"]
        temp["contracts"] = [{
            "contractName": contract["description"]["runnerName"],
            "lastTradePrice": round(100 / contract["state"]["lastPriceTraded"], 1)
            } for contract in data["eventTypes"][0]["eventNodes"][0]["marketNodes"][0]["runners"]
            if "lastPriceTraded" in contract["state"].keys()]
        output.append(temp)

    # Save the dictionary as a JSON file
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(output, json_file, ensure_ascii=False, indent=4)

    # print(f"Data has been saved to {json_file_path}")

def get_smarkets():
    params = {
        'jurisdiction': 'MGA',
    }

    response = requests.get(
        'https://api.smarkets.com/v3/events/41938283,41936389,41936834,42119912,42461161/markets/',
        params=params,
        cookies=smarkets_cookies,
        headers=smarkets_headers,
    )

    markets = response.json()["markets"]
    markets = [{
        "description": market["description"],
        "id": market["id"]
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

def get_contracts_values_smarkets(market_id, contracts_ids):
    params = {
        'data_points': '150',
    }

    response = requests.get(
        f'https://api.smarkets.com/v3/events/41945845/markets/{market_id}/contracts/{contracts_ids}/executions_time_series/',
        params=params,
        cookies=smarkets_cookies,
        headers=smarkets_headers,
    )

    contracts = response.json()["contracts"]

    data = [{
        "id": contract["contract_id"],
        "lastTradePrice": round(contract["before"]["ohlc"][0]/100,1)
    } for contract in contracts]

    return data

def get_smarkets_data():
    print("getting smarkets data...")
    markets = get_smarkets()

    markets_ids = ",".join([market["id"] for market in markets])
    contracts = get_contracts_smarkets(markets_ids)

    output = []
    for market in markets:
        temp = {}
        contracts_ids = ",".join([contract["id"] for contract in contracts if contract["market_id"] == market["id"] and contract["info"]])
        try:
            temp["contracts"] = get_contracts_values_smarkets(market["id"], contracts_ids)
        except:
            continue
        for tp in temp["contracts"]:
            tp["contractName"] = [contract for contract in contracts if contract["id"] == tp["id"]][0]["name"]
        temp["title"] = [mk["description"] for mk in markets if mk["id"] == market["id"]][0]
        output.append(temp)

    json_file_path = "smarkets.json"

    # Save the dictionary as a JSON file
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(output, json_file, ensure_ascii=False, indent=4)

    # print(f"Data has been saved to {json_file_path}")

try:
    get_betfair_events()
except:
    print("betfair failed")
try:
    get_fairplay_data()
except:
    print("fairplay failed")
try:
    get_predictit_data()
except:
    print("predictit failed")
try:
    get_manifolds_data()
except:
    print("manifolds failed")
try:
    get_pinnacle_data()
except:
    print("pinnacle failed")
try:
    get_polymarket_data()
except:
    print("polymarket failed")
try:
    get_smarkets_data()
except:
    print("smarkets failed")