import re
from fastapi import FastAPI, Query, HTTPException
from typing import List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz

app = FastAPI()

# MongoDB setup
# Replace with your MongoDB URI if necessary
client = MongoClient("mongodb://localhost:27017/")
db = client.prediction_db

# List of valid markets
valid_markets = [
    "Predictit", "Polymarket", "Manifolds", "Pinnacle",
    "Fairplay", "Betfair", "Smarkets", "Metaculus", "Kalshi"
]


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
    if lookback:
        start_time = parse_lookback(lookback)
    collection = get_collection('realtime')
    print(collection)
    # Query for data in the specified time range if lookback is provided
    if start_time:
        query = {"timestamp": {"$gte": start_time}}
    else:
        query = {}
    # Fetch data from the collection
    data_cursor = collection.find(query).sort("timestamp", -1)

    for document in data_cursor:
        return {"timestamp": document['timestamp'], "data": document['data']}


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
                if eventUrlFilter is not '' and eventUrlFilter is not None:
                    if eventUrl is None or eventUrl != eventUrlFilter:
                        continue

                if eventFilter is not '' and eventFilter is not None:
                    title = item.get('title', '')
                    similarity = fuzz.ratio(title, eventFilter)
                    if similarity < 50:
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
