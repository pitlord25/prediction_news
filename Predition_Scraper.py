import threading
import time
from dotenv import load_dotenv
import os
import requests
from variables import *
import json
import re
import datetime
from MongoDBManger import MongoDBManager
from bs4 import BeautifulSoup
import re

def get_predictit_data(timestamp):
    print("getting predictit data...")
    response = requests.get('https://www.predictit.org/api/Browse/FilteredMarkets/3', params=predictit_params, cookies=predictit_cookies, headers=predictit_headers)
    data =  response.json()
    
    output = []
    for market in data["markets"]:
        temp = {}
        temp["title"] = market["marketName"]
        temp["contracts"] = [{"contractName" : contract["contractName"],
                        "lastTradePrice": round(contract["lastTradePrice"] * 100, 1),
                        "contractImage" : contract['contractImageUrl']} 
                        for contract in market["contracts"]]
        temp['totalValue'] = market['totalSharesTraded']
        temp['eventURL'] = f"https://www.predictit.org/markets/detail/{market['marketId']}/{market['marketUrl']}"
        output.append(temp)
    
    print(output)
    db_manager.insert_document("predictit_collection", {
        "timestamp" : timestamp,
        "data" : output
    })

    # with open("predictit.json", "w") as f:
    #     json.dump(output, f, indent=4)

def get_polymarket_data(timestamp):
    print("getting polymarket data...")
    # response = requests.post('https://polymarket.com/api/events', params=polymarket_params, cookies=polymarket_cookies, headers=polymarket_headers, json=polymarket_json_data)
    response = requests.get("https://gamma-api.polymarket.com/events?limit=200&active=true&archived=false&tag_slug=politics&closed=false&order=volume24hr&ascending=false&offset=0")
    data = response.json()
    
    output = []
    
    for market in data:
        
        temp = {}
        temp["title"] = market["title"]
        if len(market["markets"]) > 1:
            temp["contracts"] = [{"contractName" : contract["groupItemTitle"],
                            "lastTradePrice": round(float(json.loads(contract["outcomePrices"])[0]) * 100, 1),
                            "volume" : float(contract['volume']),
                            "contractImage" : contract['image']} 
                            for contract in market["markets"] if 'volume' in contract and 'outcomePrices' in contract]
        else:
            keys = json.loads(market["markets"][0]["outcomes"])
            values = json.loads(market["markets"][0]["outcomePrices"])
            temp["contracts"] = [{"contractName" : contract[0],
                            "lastTradePrice": round(float(contract[1]) * 100, 1)} 
                            for contract in zip(keys, values)]
        temp['totalValue'] = market['volume']
        temp['eventURL'] = f"https://polymarket.com/event/{market['slug']}"
        output.append(temp)
    
    db_manager.insert_document("polymarket_collection", {
        "timestamp" : timestamp,
        "data" : output
    })
    # with open("polymarket.json", "w") as f:
    #     json.dump(output, f, indent=4)

def get_manifolds_data(timestamp):
    print("getting manifolds data...")
    # Step 1: Get the HTML content from the website
    url = "https://manifold.markets"
    response = requests.get(url)
    html_content = response.text

    # Step 2: Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Step 3: Find the script tag with the "buildManifest.js" in the src attribute
    script_tag = soup.find('script', src=re.compile(r'buildManifest\.js'))

    # Step 4: Extract the specific part of the src attribute
    if script_tag and 'src' in script_tag.attrs:
        src_value = script_tag['src']
        match = re.search(r'/_next/static/([^/]+)/_buildManifest\.js', src_value)
        if match:
            extracted_string = match.group(1)
        else:
            raise ValueError("Pattern not found in src attribute")
    else:
        raise ValueError("Script tag with 'buildManifest.js' not found")
        
    response = requests.get(
        f'https://manifold.markets/_next/data/{extracted_string}/election.json',
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
        temp['totalValue'] = questions[question]['volume']
        output.append(temp)
        
    db_manager.insert_document("manifolds_collection", {
        "timestamp" : timestamp,
        "data" : output
    })

    # with open("manifold.json", "w") as f:
    #     json.dump(output, f, indent=4)

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

def get_pinnacle_data(timestamp):
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
        # temp['timestamp'] = datetime.datetime.now()
        output.append(temp)
    
    db_manager.insert_document("pinnacle_collection", {
        "timestamp" : timestamp,
        "data" : output
    })
    # with open("pinnacle.json", "w") as f:
    #     json.dump(output, f, indent=4)

def get_fairplay_data(timestamp):
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
        temp['totalValue'] = json_dict[key]['volume']
        output.append(temp)

    db_manager.insert_document("fairplay_collection", {
        "timestamp" : timestamp,
        "data" : output
    })
    # Save the dictionary as a JSON file
    # with open(json_file_path, 'w', encoding='utf-8') as json_file:
    #     json.dump(output, json_file, ensure_ascii=False, indent=4)

    # print(f"Data has been saved to {json_file_path}")

def get_betfair_events(timestamp):
    print("getting betfair data...")
    json_file_path = "betfair.json"
    url = 'https://ero.betfair.com/www/sports/exchange/readonly/v1/byevent?_ak=nzIFcwyWhrlwYMrh&currencyCode=GBP&eventIds=30186572&locale=en_GB&rollupLimit=10&rollupModel=STAKE&types=MARKET_STATE,EVENT,MARKET_DESCRIPTION'
    
    # Make a synchronous HTTP GET request
    response = requests.get(url, headers=betfair_headers)
    
    data = response.json()
    events_ids = [event["marketId"] for event in data["eventTypes"][0]["eventNodes"][0]["marketNodes"]]
    output = []
    # print(events_ids)
    for event_id in events_ids:
        # print(event_id)
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
    
    # print(output)

    db_manager.insert_document("betfair_collection", {
        "timestamp" : timestamp,
        "data" : output
    })
    # Save the dictionary as a JSON file
    # with open(json_file_path, 'w', encoding='utf-8') as json_file:
    #     json.dump(output, json_file, ensure_ascii=False, indent=4)

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
        "id": market["id"],
        "name" : market['name']
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
        f'https://api.smarkets.com/v3/markets/{market_id}/last_executed_prices',
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

def get_smarkets_data(timestamp):
    print("getting smarkets data...")
    markets = get_smarkets()

    markets_ids = ",".join([market["id"] for market in markets])
    contracts = get_contracts_smarkets(markets_ids)
    # print(contracts)
    volumes = get_volumes_smarkets(markets_ids)

    output = []
    for market in markets:
        temp = {}
        contracts_ids = ",".join([contract["id"] for contract in contracts if contract["market_id"] == market["id"]])
        try:
            temp["contracts"] = get_contracts_values_smarkets(market["id"], contracts_ids)
        except Exception as e:
            print(e)
            continue
        for tp in temp["contracts"]:
            tp["contractName"] = [contract for contract in contracts if contract["id"] == tp["id"]][0]["name"]
        temp["title"] = [mk["name"] + "-" + mk["description"] for mk in markets if mk["id"] == market["id"]][0]
        temp["totalValue"] = [volume["volume"] for volume in volumes if volume["market_id"] == market["id"]][0]
        output.append(temp)

    # json_file_path = "smarkets.json"

    db_manager.insert_document("smarkets_collection", {
        "timestamp" : timestamp,
        "data" : output
    })

    # Save the dictionary as a JSON file
    # with open(json_file_path, 'w', encoding='utf-8') as json_file:
    #     json.dump(output, json_file, ensure_ascii=False, indent=4)

    # print(f"Data has been saved to {json_file_path}")

def get_metaculus_data(timestamp) :
    print("getting metaculus data...")
    response = requests.get("https://www.metaculus.com/api2/questions/?categories=elections&forecast_type=group&has_group=false&main-feed=true&order_by=-activity&status=open",
        # params=params,
        # cookies=smarkets_cookies,
        # headers=smarkets_headers,
    )
    questions = response.json()['results']
    
    output = []
    for question in questions :
        temp = {}
        contractors = question['sub_questions']
        temp['contracts'] = [
            {
                'contractName': item['sub_question_label'],
                'lastTradePrice': item["community_prediction"]['full']['q2'] * 100
            }
            for item in contractors
        ]
        temp['title'] = question['title']
        output.append(temp)
        
    db_manager.insert_document("metaculus_collection", {
        "timestamp" : timestamp,
        "data" : output
    })
    
def get_kalshi_data(timestamp):
    print("getting kalshi data...")
    # Get token via credential 

    url = "https://trading-api.kalshi.com/trade-api/v2/login"

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
    }

    response = requests.post(url, headers=headers, json={
        "email" : "xeonDev@outlook.com",
        "password": "Kmg20030116@"
        })

    user_id = response.json()['member_id']
    token = response.json()['token']
    
    url = "https://trading-api.kalshi.com/trade-api/v2/events"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    
    eventList = []
    
    params = {
        "with_nested_markets" : "true", 
        "status" : "open",
        "limit" : "200"
    }

    response = requests.get(url, headers=headers, params= params).json()
    eventList.extend(response['events'])
    
    cursor = response['cursor']
    
    while(cursor != "") :
        params['cursor'] = cursor
        response = requests.get(url, headers=headers, params= params).json()
        eventList.extend(response['events'])
        cursor = response['cursor']
        
    output = []
    for event in eventList :
        temp = {}
        contractors = event['markets']
        temp['contracts'] = [
            {
                'contractName': item['subtitle'],
                'lastTradePrice': item["last_price"]
            }
            for item in contractors
        ]
        temp['title'] = event['title']
        temp['eventURL'] = f"https://kalshi.com/markets/{event['event_ticker']}/{event['title'].replace(' ', '-').replace('?', '')}"
        output.append(temp)

    db_manager.insert_document("kalshi_collection", {
        "timestamp" : timestamp,
        "data" : output
    })
    
    return
class ScrapingThread(threading.Thread):
    def __init__(self, timer):
        super().__init__()
        self.timer = timer
        self.stop_thread = threading.Event()

    def run(self):
        while not self.stop_thread.is_set():
            print('called')
            timestamp = datetime.datetime.now()
            try:
                get_predictit_data(timestamp)
            except Exception as e:
                print("betfair failed", e)
            
            try:
                get_polymarket_data(timestamp)
            except Exception as e:
                print("polymarket failed", e)
            
            try:
                get_manifolds_data(timestamp)
            except Exception as e:
                print("manifolds failed", e)
                
            try:
                get_pinnacle_data(timestamp)
            except Exception as e:
                print("pinnacle failed", e)
                
            try:
                get_fairplay_data(timestamp)
            except Exception as e:
                print("fairplay failed", e)
                
            try:
                get_betfair_events(timestamp)
            except Exception as e:
                print("betfair failed", e)
                
            try:
                get_smarkets_data(timestamp)
            except Exception as e:
                print("smarkets failed", e)
            
            try:
                get_metaculus_data(timestamp)
            except Exception as e:
                print("metaculus failed", e)
            
            try:
                get_kalshi_data(timestamp)
            except Exception as e:
                print("kalshi failed", e)            
            
            print("sleeping")
            time.sleep(self.timer)

    def stop(self):
        self.stop_thread.set()
        
if __name__ == "__main__":
    
    # Initialize the MongoDB manager
    db_manager = MongoDBManager()

    # Load the .env file
    load_dotenv()

    # Get the timer value from the .env file
    TIMER = int(os.getenv('SCRAPING_TIMER', 10))
    # Create and start the thread
    print(TIMER)
    scraping_thread = ScrapingThread(TIMER)
    scraping_thread.start()
