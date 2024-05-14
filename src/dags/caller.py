import requests
from Utils import *
import time
import json

API_URL = "http://localhost:8000"
SEND_EVENTS_ENDPOINT = "/events"
PURCHASED_ITEMS_ENDPOINT = "/items"

while True:
    #event = create_fake_event()
    purchasedItems = create_fake_purchased_items()

    #sendEventsPayload = event.to_dict()
    purchasedItemsPayload = purchasedItems.to_dict()

    #sendEventsResponse = requests.put(API_URL + SEND_EVENTS_ENDPOINT, json=sendEventsPayload)
    purchasedItemsResponse = requests.post(API_URL + PURCHASED_ITEMS_ENDPOINT, data=json.dumps(purchasedItems.__dict__))

    time.sleep(1)