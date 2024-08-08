from fastapi import FastAPI, Query, HTTPException
from typing import List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from datetime import datetime, timedelta
import re

app = FastAPI()

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB URI if necessary
db = client.prediction_db

# List of valid markets
valid_markets = [
    "Predictit", "Polymarket", "Manifolds", "Pinnacle", 
    "Fairplay", "Betfair", "Smarkets", "Metaculus"
]

def get_collection(market: str) -> Collection:
    collection_name = f"{market.lower()}_collection"
    return db[collection_name]

def parse_lookback(lookback: str) -> datetime:
    """Parse the lookback string into a datetime object representing the start time."""
    pattern = r"(\d+)([hmsDMY])"
    match = re.fullmatch(pattern, lookback)
    
    if not match:
        raise HTTPException(status_code=400, detail="Invalid lookback format. Use format: number + period_unit (e.g., 3h, 2D)")

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
        return now - timedelta(days=30 * amount)  # Approximate 1 month as 30 days
    elif unit == 'Y':
        return now - timedelta(days=365 * amount)  # Approximate 1 year as 365 days

@app.get("/markets")
async def get_markets(
    market: List[str] = Query(..., description="List of markets", min_items=1),
    minimum_price: Optional[float] = Query(None, ge=0, description="Minimum price"),
    maximum_price: Optional[float] = Query(None, ge=0, description="Maximum price"),
    lookback: Optional[str] = Query(None, description="Lookback period (e.g., 3h, 2D)")
):
    # Validate that each market is in the valid_markets list
    for m in market:
        if m not in valid_markets:
            raise HTTPException(status_code=400, detail=f"Invalid market: {m}. Must be one of {valid_markets}")

    # Calculate the start time if lookback is provided
    start_time = None
    if lookback:
        start_time = parse_lookback(lookback)

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
                contracts = item.get('contracts', [])
                filtered = [
                    contract for contract in contracts
                    if (minimum_price is None or contract['lastTradePrice'] >= minimum_price) and
                       (maximum_price is None or contract['lastTradePrice'] <= maximum_price)
                ]
                if filtered:
                    item['contracts'] = filtered
                    filtered_contracts.append(item)

            if filtered_contracts:
                results.append({
                    "market": m,
                    "timestamp": document['timestamp'],
                    "data": filtered_contracts
                })

    if not results:
        return {"message": "No data found for the given parameters"}

    return results
