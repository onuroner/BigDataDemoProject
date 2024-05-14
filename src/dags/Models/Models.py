from enum import Enum
from typing import List
from pydantic import BaseModel
import json

class SendEvent(BaseModel):
    user_id: int
    session_id: int 
    event_name: str
    timestamp: str
    product_id: int
    price: float
    discount: float
    
    def __init__(self, user_id, session_id, event_name, timestamp, product_id, price, discount):
        super().__init__(user_id=user_id, session_id=session_id, event_name=event_name, timestamp=timestamp, product_id=product_id, price=price, discount=discount)
        # self.user_id = user_id
        # self.session_id = session_id
        # self.event_name = event_name
        # self.timestamp = timestamp
        # self.product_id = product_id
        # self.price = price
        # self.discount = discount

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
            "product_id": self.product_id,
            "price": self.price,
            "discount": self.discount
        }

    def __str__(self):
        return f"Event(user_id={self.user_id}, session_id={self.session_id}, event_name={self.event_name}, timestamp={self.timestamp}, product_id={self.product_id}, price={self.price}, discount={self.discount})"

class Product(BaseModel):
    product_id : int
    item_count : int
    item_price : float
    item_discount : float

    def __init__(self, product_id, item_count, item_price, item_discount):
        super().__init__(product_id = product_id, item_count = item_count, item_price=item_price, item_discount=item_discount)
        
    def to_dict(self):
        return {                
            "product_id": self.product_id,                
            "item_count": self.item_count,
            "item_price": self.item_price,
            "item_discount": self.item_discount
        }

    def __str__(self):
        return f"Product(product_id={self.product_id}, item_count={self.item_count}, item_price={self.item_price}, item_discount={self.item_discount})"

class PurchasedItems(BaseModel):
    sessionId : int
    timestamp : str
    userId : int
    totalPrice : float
    orderId : int
    products  : List[dict]
    def __init__(self, sessionId,timestamp, userId, totalPrice, orderId, products):
        super().__init__(sessionId=sessionId,timestamp=timestamp, userId=userId, totalPrice=totalPrice, orderId=orderId, products=products)

    def to_dict(self):
        return {                
            "sessionId": self.sessionId,                
            "timestamp": self.timestamp,
            "userId": self.userId,
            "totalPrice": self.totalPrice,
            "orderId": self.orderId,
            "products": self.products
    }

class SendEventNames(Enum):
    PageVisited = "PageVisited"
    AddedBasket = "AddedBasket"
    CheckedProductReviews = "CheckedProductReviews"