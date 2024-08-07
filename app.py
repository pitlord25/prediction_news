import streamlit as st
import json
import pandas as pd
from difflib import SequenceMatcher
from MongoDBManger import MongoDBManager
import re
import datetime

# Load JSON data
def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

# Save matched events to JSON
def save_matched_events(matched_events):
    db_manager.insert_document("matched_events", {
        "timestamp" : datetime.datetime.now(),
        "data" : matched_events
    })
    # with open('matched_events.json', 'w') as f:
    #     json.dump(matched_events, f, indent=4)

# Load matched events from JSON
def load_matched_events():
    try:
        res = db_manager.find_latest_document("matched_events")['data']
        return res
        with open('matched_events.json', 'r') as f:
            return json.load(f)
    except :
        return []

# Function to automatically match events based on their names
def auto_match_events(events1, events2, events3, events4, events5, events6, events7, events8, threshold=0.6):
    auto_matched_events = []

    for event1 in events1:
        best_matches = []
        best_ratio = 0

        for event2 in events2:
            match_ratio1 = SequenceMatcher(None, event1["title"], event2["title"]).ratio()

            for event3 in events3:
                match_ratio2 = SequenceMatcher(None, event1["title"], event3["title"]).ratio()
                match_ratio3 = SequenceMatcher(None, event2["title"], event3["title"]).ratio()

                for event4 in events4:
                    match_ratio4 = SequenceMatcher(None, event1["title"], event4["title"]).ratio()
                    match_ratio5 = SequenceMatcher(None, event2["title"], event4["title"]).ratio()
                    match_ratio6 = SequenceMatcher(None, event3["title"], event4["title"]).ratio()

                    for event5 in events5:
                        match_ratio7 = SequenceMatcher(None, event1["title"], event5["title"]).ratio()
                        match_ratio8 = SequenceMatcher(None, event2["title"], event5["title"]).ratio()
                        match_ratio9 = SequenceMatcher(None, event3["title"], event5["title"]).ratio()
                        match_ratio10 = SequenceMatcher(None, event4["title"], event5["title"]).ratio()

                        for event6 in events6:
                            match_ratio11 = SequenceMatcher(None, event1["title"], event6["title"]).ratio()
                            match_ratio12 = SequenceMatcher(None, event2["title"], event6["title"]).ratio()
                            match_ratio13 = SequenceMatcher(None, event3["title"], event6["title"]).ratio()
                            match_ratio14 = SequenceMatcher(None, event4["title"], event6["title"]).ratio()
                            match_ratio15 = SequenceMatcher(None, event5["title"], event6["title"]).ratio()

                            for event7 in events7:
                                match_ratio16 = SequenceMatcher(None, event1["title"], event7["title"]).ratio()
                                match_ratio17 = SequenceMatcher(None, event2["title"], event7["title"]).ratio()
                                match_ratio18 = SequenceMatcher(None, event3["title"], event7["title"]).ratio()
                                match_ratio19 = SequenceMatcher(None, event4["title"], event7["title"]).ratio()
                                match_ratio20 = SequenceMatcher(None, event5["title"], event7["title"]).ratio()
                                match_ratio21 = SequenceMatcher(None, event6["title"], event7["title"]).ratio()
                                
                                for event8 in events8:
                                    match_ratio22 = SequenceMatcher(None, event1["title"], event8["title"]).ratio()
                                    match_ratio23 = SequenceMatcher(None, event2["title"], event8["title"]).ratio()
                                    match_ratio24 = SequenceMatcher(None, event3["title"], event8["title"]).ratio()
                                    match_ratio25 = SequenceMatcher(None, event4["title"], event8["title"]).ratio()
                                    match_ratio26 = SequenceMatcher(None, event5["title"], event8["title"]).ratio()
                                    match_ratio27 = SequenceMatcher(None, event6["title"], event8["title"]).ratio()
                                    match_ratio28 = SequenceMatcher(None, event7["title"], event8["title"]).ratio()

                                average_ratio = (match_ratio1 + match_ratio2 + match_ratio3 + match_ratio4 + match_ratio5 + match_ratio6 + match_ratio7 + match_ratio8 + match_ratio9 + match_ratio10 + match_ratio11 + match_ratio12 + match_ratio13 + match_ratio14 + match_ratio15 + match_ratio16 + match_ratio17 + match_ratio18 + match_ratio19 + match_ratio20 + match_ratio21 + match_ratio22 + match_ratio23 + match_ratio24 + match_ratio25 + match_ratio26 + match_ratio27 + match_ratio28) / 28

                                if average_ratio >= threshold:
                                    best_ratio = average_ratio
                                    best_matches = [event2, event3, event4, event5, event6, event7, event8]

        if best_matches:
            auto_matched_events.append({
                "title": event1["title"],
                "predictit": event1,
                "polymarket": best_matches[0],
                "manifold": best_matches[1],
                "pinnacle": best_matches[2],
                "fairplay": best_matches[3],
                "betfair": best_matches[4],
                "smarket": best_matches[5],
                "metaculus": best_matches[6]
            })
        else:
            # Check for matches with fewer sources if no 7-event match found
            for event2 in events2:
                match_ratio1 = SequenceMatcher(None, event1["title"], event2["title"]).ratio()

                if match_ratio1 >= threshold:
                    auto_matched_events.append({
                        "title": event1["title"],
                        "predictit": event1,
                        "polymarket": event2,
                        "manifold": None,
                        "pinnacle": None,
                        "fairplay": None,
                        "betfair": None,
                        "smarket": None,
                        "metaculus": None
                    })
                    break

                for event3 in events3:
                    match_ratio2 = SequenceMatcher(None, event1["title"], event3["title"]).ratio()
                    match_ratio3 = SequenceMatcher(None, event2["title"], event3["title"]).ratio()

                    if (match_ratio1 + match_ratio2 + match_ratio3) / 3 >= threshold:
                        auto_matched_events.append({
                            "title": event1["title"],
                            "predictit": event1,
                            "polymarket": event2,
                            "manifold": event3,
                            "pinnacle": None,
                            "fairplay": None,
                            "betfair": None,
                            "smarket": None,
                            "metaculus": None
                        })
                        break

    return auto_matched_events

# Function to match participants within matched events
def match_participants(event1, event2, event3, event4, event5, event6, event7, event8):
    participants = {}
    if event1:
        for c in event1['contracts']:
            normalized_name, original_name = normalize_name(c['contractName'])
            participants[normalized_name] = {"original_name": original_name}
    if event2:
        for c in event2['contracts']:
            normalized_name, original_name = normalize_name(c['contractName'])
            participants[normalized_name] = {"original_name": original_name}
    if event3:
        for c in event3['contracts']:
            normalized_name, original_name = normalize_name(c['contractName'])
            participants[normalized_name] = {"original_name": original_name}
    if event4:
        for c in event4['contracts']:
            normalized_name, original_name = normalize_name(c['contractName'])
            participants[normalized_name] = {"original_name": original_name}
    if event5:
        for c in event5['contracts']:
            normalized_name, original_name = normalize_name(c['contractName'])
            participants[normalized_name] = {"original_name": original_name}
    if event6:
        for c in event6['contracts']:
            normalized_name, original_name = normalize_name(c['contractName'])
            participants[normalized_name] = {"original_name": original_name}
    if event7:
        for c in event7['contracts']:
            normalized_name, original_name = normalize_name(c['contractName'])
            participants[normalized_name] = {"original_name": original_name}
    if event8:
        for c in event8['contracts']:
            normalized_name, original_name = normalize_name(c['contractName'])
            participants[normalized_name] = {"original_name": original_name}

    data = []
    for normalized_name, details in participants.items():
        match_data = {"Participant": details["original_name"]}
        total = 0
        count = 0
        
        if event1:
            p1 = next((c['lastTradePrice'] for c in event1['contracts'] if normalize_name(c['contractName'])[0] == normalized_name), None)
            if p1 is not None:
                match_data["PredictIt"] = p1
                total += p1
                count += 1
        
        if event2:
            p2 = next((c['lastTradePrice'] for c in event2['contracts'] if normalize_name(c['contractName'])[0] == normalized_name), None)
            if p2 is not None:
                match_data["Polymarket"] = p2
                total += p2
                count += 1
        
        if event3:
            p3 = next((c['lastTradePrice'] for c in event3['contracts'] if normalize_name(c['contractName'])[0] == normalized_name), None)
            if p3 is not None:
                match_data["Manifold"] = p3
                total += p3
                count += 1
        
        if event4:
            p4 = next((c['lastTradePrice'] for c in event4['contracts'] if normalize_name(c['contractName'])[0] == normalized_name), None)
            if p4 is not None:
                match_data["Pinnacle"] = p4
                total += p4
                count += 1

        if event5:
            p5 = next((c['lastTradePrice'] for c in event5['contracts'] if normalize_name(c['contractName'])[0] == normalized_name), None)
            if p5 is not None:
                match_data["Fairplay"] = p5
                total += p5
                count += 1

        if event6:
            p6 = next((c['lastTradePrice'] for c in event6['contracts'] if normalize_name(c['contractName'])[0] == normalized_name), None)
            if p6 is not None:
                match_data["Betfair"] = p6
                total += p6
                count += 1
        
        if event7:
            p7 = next((c['lastTradePrice'] for c in event7['contracts'] if normalize_name(c['contractName'])[0] == normalized_name), None)
            if p7 is not None:
                match_data["Smarket"] = p7
                total += p7
                count += 1

        if event8:
            p8 = next((c['lastTradePrice'] for c in event8['contracts'] if normalize_name(c['contractName'])[0] == normalized_name), None)
            if p8 is not None:
                match_data["Metaculus"] = p8
                total += p8
                count += 1
                
        # Calculate the average
        if count > 0:
            match_data["PredictionNews"] = round(total / count, 1)
        else:
            match_data["PredictionNews"] = None

        data.append(match_data)
    df = pd.DataFrame(data)
    col_to_move = 'PredictionNews'
    cols = [col for col in df.columns if col != col_to_move] + [col_to_move]
    df = df[cols]
    return df

# Function to normalize a participant name by removing punctuation, spaces, and making lowercase
def normalize_name(name):
    # Remove non-alphanumeric characters, spaces, and convert to lowercase
    normalized_name = re.sub(r'[^\w\s]', '', name).replace(" ", "").lower()
    return normalized_name, name  # Return both normalized and original name

# Initialize the MongoDB manager
db_manager = MongoDBManager()

predictit_events = db_manager.find_latest_document("predictit_collection")['data']
polymarket_events = db_manager.find_latest_document("polymarket_collection")['data']
manifold_events = db_manager.find_latest_document("manifolds_collection")['data']
pinnacle_events = db_manager.find_latest_document("pinnacle_collection")['data']
fairplay_events = db_manager.find_latest_document("fairplay_collection")['data']
betfair_events = db_manager.find_latest_document("betfair_collection")['data']
smarket_events = db_manager.find_latest_document("smarkets_collection")['data']
metaculus_events = db_manager.find_latest_document("metaculus_collection")['data']

# predictit_events = load_json('predictit.json')
# polymarket_events = load_json('polymarket.json')
# manifold_events = load_json('manifold.json')
# pinnacle_events = load_json('pinnacle.json')
# fairplay_events = load_json('fairplay.json')
# betfair_events = load_json('betfair.json')
# smarket_events = load_json('smarkets.json')  # Load new data source

# Initialize or load matched events
matched_events = load_matched_events()

# Define tabs
tab1, tab2, tab3 = st.tabs(["Matched events", "Match Events", "Auto Match Events"])

with tab1:
    st.title("Matched events")
    for match in matched_events:
        st.subheader(match["title"])
        df = match_participants(match["predictit"], match.get("polymarket"), match.get("manifold"), match.get("pinnacle"), match.get("fairplay"), match.get("betfair"), match.get("smarket"), match.get("metaculus"))
        st.dataframe(df)

    if st.button("Clear Matched Events"):
        matched_events.clear()
        save_matched_events(matched_events)
        st.rerun()
        st.success("All matched events have been cleared!")

with tab2:
    st.title("Match Events")

    matched_predictit_titles = {match["predictit"]["title"] for match in matched_events if match.get("predictit")}
    matched_polymarket_titles = {match["polymarket"]["title"] for match in matched_events if match.get("polymarket")}
    matched_manifold_titles = {match["manifold"]["title"] for match in matched_events if match.get("manifold")}
    matched_pinnacle_titles = {match["pinnacle"]["title"] for match in matched_events if match.get("pinnacle")}
    matched_fairplay_titles = {match["fairplay"]["title"] for match in matched_events if match.get("fairplay")}
    matched_betfair_titles = {match["betfair"]["title"] for match in matched_events if match.get("betfair")}
    matched_smarket_titles = {match["smarket"]["title"] for match in matched_events if match.get("smarket")}
    matched_metaculus_titles = {match["metaculus"]["title"] for match in matched_events if match.get("metaculus")}

    available_predictit_events = ["None"] + [event["title"] for event in predictit_events if event["title"] not in matched_predictit_titles]
    available_polymarket_events = ["None"] + [event["title"] for event in polymarket_events if event["title"] not in matched_polymarket_titles]
    available_manifold_events = ["None"] + [event["title"] for event in manifold_events if event["title"] not in matched_manifold_titles]
    available_pinnacle_events = ["None"] + [event["title"] for event in pinnacle_events if event["title"] not in matched_pinnacle_titles]
    available_fairplay_events = ["None"] + [event["title"] for event in fairplay_events if event["title"] not in matched_fairplay_titles]
    available_betfair_events = ["None"] + [event["title"] for event in betfair_events if event["title"] not in matched_betfair_titles]
    available_smarket_events = ["None"] + [event["title"] for event in smarket_events if event["title"] not in matched_smarket_titles]
    available_metaculus_events = ["None"] + [event["title"] for event in metaculus_events if event["title"] not in matched_metaculus_titles]

    selected_predictit_event = st.selectbox("Select PredictIt Event", options=available_predictit_events)
    selected_polymarket_event = st.selectbox("Select Polymarket Event", options=available_polymarket_events)
    selected_manifold_event = st.selectbox("Select Manifold Event", options=available_manifold_events)
    selected_pinnacle_event = st.selectbox("Select Pinnacle Event", options=available_pinnacle_events)
    selected_fairplay_event = st.selectbox("Select Fairplay Event", options=available_fairplay_events)
    selected_betfair_event = st.selectbox("Select Betfair Event", options=available_betfair_events)
    selected_smarket_event = st.selectbox("Select Smarket Event", options=available_smarket_events)
    selected_metaculus_event = st.selectbox("Select Metaculus Event", options=available_metaculus_events)

    if st.button("Match Events"):
        # Find the selected events
        predictit_event = next((event for event in predictit_events if event["title"] == selected_predictit_event), None) if selected_predictit_event != "None" else None
        polymarket_event = next((event for event in polymarket_events if event["title"] == selected_polymarket_event), None) if selected_polymarket_event != "None" else None
        manifold_event = next((event for event in manifold_events if event["title"] == selected_manifold_event), None) if selected_manifold_event != "None" else None
        pinnacle_event = next((event for event in pinnacle_events if event["title"] == selected_pinnacle_event), None) if selected_pinnacle_event != "None" else None
        fairplay_event = next((event for event in fairplay_events if event["title"] == selected_fairplay_event), None) if selected_fairplay_event != "None" else None
        betfair_event = next((event for event in betfair_events if event["title"] == selected_betfair_event), None) if selected_betfair_event != "None" else None
        smarket_event = next((event for event in smarket_events if event["title"] == selected_smarket_event), None) if selected_smarket_event != "None" else None
        metaculus_event = next((event for event in metaculus_events if event["title"] == selected_metaculus_event), None) if selected_metaculus_event != "None" else None
        
        # Add the matched events to the list
        matched_events.append({
            "title": " - ".join(filter(None, [
                predictit_event["title"] if predictit_event else None,
                polymarket_event["title"] if polymarket_event else None,
                manifold_event["title"] if manifold_event else None,
                pinnacle_event["title"] if pinnacle_event else None,
                fairplay_event["title"] if fairplay_event else None,
                betfair_event["title"] if betfair_event else None,
                smarket_event["title"] if smarket_event else None,
                metaculus_event["title"] if metaculus_event else None
            ])),
            "predictit": {"title": predictit_event['title'], "contracts": predictit_event['contracts']} if predictit_event else None,
            "polymarket": {"title": polymarket_event['title'], "contracts": polymarket_event['contracts']} if polymarket_event else None,
            "manifold": {"title": manifold_event['title'], "contracts": manifold_event['contracts']} if manifold_event else None,
            "pinnacle": {"title": pinnacle_event['title'], "contracts": pinnacle_event['contracts']} if pinnacle_event else None,
            "fairplay": {"title": fairplay_event['title'], "contracts": fairplay_event['contracts']} if fairplay_event else None,
            "betfair": {"title": betfair_event['title'], "contracts": betfair_event['contracts']} if betfair_event else None,
            "smarket": {"title": smarket_event['title'], "contracts": smarket_event['contracts']} if smarket_event else None,
            "metaculus": {"title": metaculus_event['title'], "contracts": metaculus_event['contracts']} if metaculus_event else None
        })

        # Save the matched events to JSON
        save_matched_events(matched_events)

        st.success("Events matched successfully!")
        st.rerun()

with tab3:
    st.title("Auto Match Events")

    threshold = st.slider("Similarity Threshold", 0.0, 1.0, 0.6, 0.01)

    if st.button("Auto Match Events"):
        auto_matched_events = auto_match_events(predictit_events, polymarket_events, manifold_events, pinnacle_events, fairplay_events, betfair_events, smarket_events, metaculus_events, threshold)

        # Update matched events and save to JSON
        for match in auto_matched_events:
            if match not in matched_events:
                matched_events.append(match)
        
        save_matched_events(matched_events)
        
        st.success(f"{len(auto_matched_events)} events auto-matched successfully!")
        st.experimental_rerun()

# Close MongoDB connection when done
db_manager.close_connection()

# # Stop the thread when the main program is interrupted
# scraping_thread.stop()
# scraping_thread.join()
# print("Program terminated")


# # Display the counter
# st.write(f"Counter: {st.session_state['counter']}")

# st.button("Rerun")