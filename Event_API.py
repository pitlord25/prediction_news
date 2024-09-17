import re
from fastapi import FastAPI, Query, HTTPException, Request
from typing import List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# MongoDB setup
# Replace with your MongoDB URI if necessary
client = MongoClient("mongodb://localhost:27017/")
db = client.prediction_db

# List of domains that are allowed to make requests to this API
origins = [
    "http://localhost",  # if running the frontend locally
    # if frontend is on localhost:3000 (React, Angular, etc.)
    "http://localhost:3000",
    "https://predictionnews.com",  # if frontend is deployed,
    "https://njsportsbookreview.com",
]

# List of valid markets
valid_markets = [
    "Predictit", "Polymarket", "Manifolds", "Pinnacle",
    "Fairplay", "Betfair", "Smarkets", "Metaculus", "Kalshi"
]

# Add CORS middleware to FastAPI app
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Origins that are allowed to make CORS requests
    allow_credentials=True,  # Allow cookies to be sent with cross-origin requests
    # Allow all HTTP methods (GET, POST, PUT, DELETE, etc.)
    allow_methods=["*"],
    allow_headers=["*"],  # Allow all headers in requests
)


def get_collection(market: str) -> Collection:
    collection_name = f"{market.lower()}_collection"
    return db[collection_name]


def parse_lookback(lookback: str) -> datetime:
    """Parse the lookback string into a datetime object representing the start time."""
    pattern = r"(\d+)([hmsDMY])"
    match = re.fullmatch(pattern, lookback)

    if not match:
        raise HTTPException(
            status_code=400, detail="Invalid lookback format. Use format: number + period_unit (e.g., 3h, 2D)")

    amount, unit = match.groups()
    amount = int(amount)
    now = datetime.utcnow()

    if unit == 'h':
        return now - timedelta(hours=amount)
    elif unit == 'm':
        return now - timedelta(minutes=amount)
    elif unit == 's':
        return now - timedelta(seconds=amount)
    elif unit == 'D':
        return now - timedelta(days=amount)
    elif unit == 'M':
        # Approximate 1 month as 30 days
        return now - timedelta(days=30 * amount)
    elif unit == 'Y':
        # Approximate 1 year as 365 days
        return now - timedelta(days=365 * amount)

# Helper function to parse interval string (e.g., "1h", "1D")
def parse_interval(interval: str) -> timedelta:
    match = re.match(r"(\d+)([hDWM])", interval)
    if not match:
        raise HTTPException(status_code=400, detail="Invalid interval format. Use '1h', '1D', '1M', etc.")
    
    amount, unit = int(match.group(1)), match.group(2)
    if unit == 'h':
        return timedelta(hours=amount)
    elif unit == 'D':
        return timedelta(days=amount)
    elif unit == 'W':
        return timedelta(weeks=amount)
    elif unit == 'M':
        # For simplicity, we'll treat 1M as 30 days
        return timedelta(days=30 * amount)
    else:
        raise HTTPException(status_code=400, detail="Invalid interval unit. Use 'h', 'D', 'W', or 'M'.")


def normalize_name(name: str):
    """Normalize the name by removing non-alphanumeric characters, spaces, and converting to lowercase."""
    normalized_name = re.sub(r'[^\w\s]', '', name).replace(" ", "").lower()
    return normalized_name, name  # Return both normalized and original name


@app.get("/realtime_debates")
async def get_realtime_debates(
    lookback: Optional[str] = Query(
        None, description="Lookback period (e.g., 3h, 2D)"),
):
    # Calculate the start time if lookback is provided
    start_time = None
    # if lookback:
    #     start_time = parse_lookback(lookback)
    collection = get_collection('realtime')

    # Query for data in the specified time range if lookback is provided

    # Fetch data from the collection
    latestDocument = collection.find_one({}, sort=[("timestamp", -1)])
    # data_cursor = collection.find(query).sort("timestamp", -1)

        
    def filter_contracts(contracts):
        return [contract for contract in contracts if contract['contractName'] in ["Donald Trump", "Kamala Harris"]]

    # Return only the relevant contracts in the response
    return {
        "timestamp": latestDocument['timestamp'],
        "data": {
            "predictit": {
                "title": latestDocument['data']['predictit'].get('title'),
                "contracts": filter_contracts(latestDocument['data']['predictit']['contracts'])
            },
            "betfair": {
                "title": latestDocument['data']['betfair'].get('title'),
                "contracts": filter_contracts(latestDocument['data']['betfair']['contracts'])
            },
            "polymarket": {
                "title": latestDocument['data']['polymarket'].get('title'),
                "contracts": filter_contracts(latestDocument['data']['polymarket']['contracts'])
            },
            "smarkets": {
                "title": latestDocument['data']['smarkets'].get('title'),
                "contracts": filter_contracts(latestDocument['data']['smarkets']['contracts'])
            }
        }
    }

@app.get("/market_titles")
async def get_market_titles(
    keyword: str = Query(..., description="Event Filter")
):
    results = []

    for m in valid_markets:
        collection = get_collection(m)
        # Query the latest document based on the timestamp
        latest_document = collection.find_one(
            {},  # No condition, return all documents
            sort=[('timestamp', -1)]  # Sort by 'timestamp' in descending order
        )
        
        if not latest_document:
            return {}  # Return None if no document found

        filtered_data = [
            {"Provider" : m, "title" : item['title']} for item in latest_document['data']
            if keyword.lower() in item['title'].lower()
        ]
        
        results.extend(filtered_data)
    
    return results

@app.get("/price_history")
async def get_market_price_history(
    provider : str = Query(..., description='Event Provdier'),
    title : str = Query(..., description = 'Market Title'),
    start_date : datetime = Query(..., description = "Start Datetime"),
    end_date : datetime = Query(..., description = 'End Datetime'),
    contractor_name : Optional[str] = Query(None, description="Name of contractor"),
    interval : str = Query(..., description = 'Time interval to fetch data')
) :
    # Parse the interval string (e.g., "1h", "1D")
    interval_delta = parse_interval(interval)
    if provider.capitalize() not in valid_markets:
        raise HTTPException(status_code=400, detail=f"Invalid market: {
                            provider}. Must be one of {valid_markets}")
        
    collection = get_collection(normalize_name(provider)[0])
    
    # Query for data in the specified time range if lookback is provided
    query = {
        "timestamp": {"$gte": start_date, "$lte": end_date},
        "data.title": title,
    }
    
    # Fetch data from the collection
    data_cursor = collection.find(query).sort("timestamp", -1)
    
    result = []
    grouped_data = {}
    # If no data found, raise an exception
    for document in data_cursor:
        timestamp = document['timestamp']
        # Group by intervals
        bucket_time = (timestamp - start_date) // interval_delta * interval_delta + start_date

        if bucket_time not in grouped_data:
            grouped_data[bucket_time] = {}

        for market in document['data']:
            if market['title'] == title:
                for contract in market['contracts']:
                    if contractor_name is None or contract['contractName'] == contractor_name:
                        if contract['contractName'] not in grouped_data[bucket_time]:
                            grouped_data[bucket_time][contract['contractName']] = []
                        grouped_data[bucket_time][contract['contractName']].append(contract['lastTradePrice'])

    # Calculate the average price for each bucket
    for bucket_time, contracts in grouped_data.items():
        for contractor, prices in contracts.items():
            avg_price = sum(prices) / len(prices)  # Calculate average price for this contractor in the interval
            result.append({
                "timestamp": bucket_time,
                "contractor_name": contractor,
                "last_trade_price": avg_price
            })

    return result

@app.get("/markets")
async def get_markets(
    market: List[str] = Query(..., description="List of markets", min_items=1),
    minimum_price: Optional[float] = Query(
        None, ge=0, description="Minimum price"),
    maximum_price: Optional[float] = Query(
        None, ge=0, description="Maximum price"),
    lookback: Optional[str] = Query(
        None, description="Lookback period (e.g., 3h, 2D)"),
    candidates: Optional[List[str]] = Query(
        None, description="List of candidate names to filter"),
    eventFilter: Optional[str] = Query(None, description="Event Filter"),
    eventUrlFilter: Optional[str] = Query(
        None, description="Event Url Filter"),
    # security_token: str = Query(None, description="Security token")
):
    # Validate that each market is in the valid_markets list
    for m in market:
        if m.capitalize() not in valid_markets:
            raise HTTPException(status_code=400, detail=f"Invalid market: {
                                m}. Must be one of {valid_markets}")

    # Calculate the start time if lookback is provided
    start_time = None
    if lookback:
        start_time = parse_lookback(lookback)

    # Normalize candidate names if provided
    normalized_candidates = {}
    if candidates:
        normalized_candidates = {normalize_name(c)[0]: c for c in candidates}

    results = []

    for m in market:
        collection = get_collection(m)

        # Query for data in the specified time range if lookback is provided
        if start_time:
            query = {"timestamp": {"$gte": start_time}}
        else:
            query = {}

        # Fetch data from the collection
        data_cursor = collection.find(query).sort("timestamp", -1)

        for document in data_cursor:
            # Filter contracts based on price if applicable
            filtered_contracts = []
            for item in document['data']:
                eventUrl = item.get('eventURL')
                if eventUrlFilter != '' and eventUrlFilter is not None:
                    if eventUrl is None or eventUrl != eventUrlFilter:
                        continue

                if eventFilter != '' and eventFilter is not None:
                    title = item.get('title', '')
                    similarity = fuzz.ratio(title, eventFilter)
                    if similarity < 30:
                        continue

                contracts = item.get('contracts', [])
                filtered = []
                for contract in contracts:
                    normalized_contract_name = normalize_name(
                        contract['contractName'])[0]
                    if (minimum_price is None or contract['lastTradePrice'] >= minimum_price) and \
                       (maximum_price is None or contract['lastTradePrice'] <= maximum_price):
                        if not candidates or any([s for s in normalized_candidates if s in normalized_contract_name]):
                            filtered.append(contract)

                if filtered:
                    item['contracts'] = filtered
                    filtered_contracts.append(item)

            if filtered_contracts:
                results.append({
                    "market": m.capitalize(),
                    "timestamp": document['timestamp'],
                    "data": filtered_contracts
                })

    if not results:
        return {"message": "No data found for the given parameters"}

    return results
