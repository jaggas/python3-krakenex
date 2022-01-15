# Import WebSocket client library (and others)
import websocket
import _thread
import time
import json

from enum import Enum
from typing import Union, Any


class EventType(str, Enum):
    PING = "ping"
    PONG = "pong"
    HEARTBEAT = "heartbeat"
    SYSTEM_STATUS = "systemStatus"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    SUBSCRIPTION_STATUS = "subscriptionStatus"
    TICKER = "ticker"
    OHLC = "ohlc"
    TRADE = "trade"
    SPREAD = "spread"
    BOOK = "book"
    OWN_TRADES = "ownTrades"
    OPEN_ORDERS = "openOrders"
    ADD_ORDER = "addOrder"
    CANCEL_ORDER = "cancelOrder"
    CANCEL_ALL = "cancelAll"
    CANCEL_ALL_ORDERS_AFTER = "cancelAllOrdersAfter"
    ERROR = "error"
    default = ERROR

class AssetPair:
    def __init__(self, pair: str):
        pieces = pair.split("/")
        self.base = pieces[0]
        self.quote = pieces[1]

class SystemStatus(Enum):
    ONLINE = "online"
    MAINTENANCE = "maintenance"
    CANCEL_ONLY = "cancel_only"
    LIMIT_ONLY = "limit_only"
    POST_ONLY = "post_only"
    default = CANCEL_ONLY

class SubscriptionInterval(Enum):
    X_1MIN  = 1
    X_5MIN  = 5
    X_15MIN = 15
    X_30MIN = 30
    X_1HOUR = 60
    X_4HOUR = 240
    X_1DAY  = 1440
    X_7DAY  = 10080
    X_15DAY = 21600
    default = X_1MIN

class SubscriptionDepth(Enum):
    DEPTH_10   = 10
    DEPTH_25   = 25
    DEPTH_100  = 100
    DEPTH_500  = 500
    DEPTH_1000 = 1000
    default = DEPTH_10

class SubscriptionName(Enum):
    BOOK = "book"
    OHLC = "ohlc"
    OPEN_ORDERS = "openOrders"
    OWN_TRADES = "ownTrades"
    SPREAD = "spread"
    TICKER = "ticker"
    TRADE = "trade"
    ERROR = "error"
    default = ERROR

class SubscriptionStatus(Enum):
    SUBSCRIBED   = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    ERROR        = "error"
    default = ERROR

class ChannelName:
    def __init__(self, channel_name: str):
        pieces = channel_name.split('-')
        self.channel = SubscriptionName(pieces[0])
        self.depth = None
        self.interval = None
        if self.channel == 'ohlc':
            self.interval = SubscriptionInterval(pieces[1])
        elif self.channel == 'book':
            self.depth = SubscriptionDepth(pieces[1])

class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"

class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop-loss"
    TAKE_PROFIT = "take-profit"
    STOP_LOSS_LIMIT = "stop-loss-limit"
    TAKE_PROFIT_LIMIT = "take-profit-limit"
    SETTLE_POSITION = "settle-position"

class TimeInForce(Enum):
    GTC = "GCT"
    IOC = "IOC"
    GTD = "GTD"

class OrderStatus(Enum):
    PENDING = "pending"
    OPEN = "open"
    CLOSED = "closed"
    CANCELED = "canceled"
    EXPIRED = "expired"

class Message:
    def __init__(self, payload: dict):
        self._raw = payload

    def __getattr__(self, key) -> Union[Any, None]:
        return self._raw.get(key, None)
    
class SystemStatusMessage(Message):
    """
    https://docs.kraken.com/websockets-beta/#message-systemStatus
    """
    def __init__(self, payload: dict):
        super().__init__(payload)
    
    @property
    def status(self) -> Union[str, None]:
        return self._raw.get('status', None)
    
    @property
    def version(self) -> Union[str, None]:
        return self._raw.get('version', None)

class SubscriptionStatusMessage(Message):
    """
    https://docs.kraken.com/websockets-beta/#message-subscriptionStatus
    """
    def __init__(self, payload: dict):
        super().__init__(payload)
    
    @property
    def channel_id(self) -> Union[int, None]:
        return self._raw.get('channelID', None)

    @property
    def channel_name(self) -> Union[ChannelName, None]:
        return ChannelName(self._raw['channelName']) if 'channelName' in self._raw else None
        
    @property
    def pair(self) -> Union[AssetPair, None]:
        return AssetPair(self._raw['pair']) if 'pair' in self._raw else None
    
    @property
    def status(self) -> Union[SubscriptionStatus, None]:
        return SubscriptionStatus(self._raw['status']) if 'status' in self._raw else None

class SubscribeMessage:
    """
    https://docs.kraken.com/websockets-beta/#message-subscribe
    """
    def __init__(self, name: SubscriptionName, )


def parse_message(msg: str) -> dict:
    try:
        d = json.loads(msg)
        if isinstance(d, dict):
            if d.get('event', '') == 'subscriptionStatus':
                sm = SubscriptionStatus(d)
                sub_status = sm.status
                print()
        else:
            payload = d[1]
            out = {'channel': d[-2],
                   'pair': d[-1]}
            if 'ohlc' in out['channel']:
                out['data'] = parse_ohlc(payload)
            elif out['channel'] == 'trade':
                out['data'] = parse_trade(payload)
            elif out['channel'] == 'spread':
                pass
    except Exception as e:
        print(str(e))
    return out

def parse_trade(payload: list) -> dict:
    out = []
    for t in payload:
        out.append({
            'price': float(t[0]),
            'volume': float(t[1]),
            'timestamp': float(t[2]),
            'side': 'buy' if t[3] == 'b' else 'sell',
            'type': 'market' if t[4] == 'm' else 'limit'
        })
    return out

def parse_ohlc(payload: list) -> dict:
    return {
        'start': float(payload[0]),
        'end': float(payload[1]),
        'o': float(payload[2]),
        'h': float(payload[3]),
        'l': float(payload[4]),
        'c': float(payload[5]),
        'vwap': float(payload[6]),
        'v': float(payload[7]),
        'count': int(payload[8])
    }

def parse_spread(payload: list) -> dict:
    return {
        'bid': float(payload[0]),
        'ask': float(payload[0]),
        'timestamp': float(payload[0]),
        'bidVolume': float(payload[0]),
        'askVolume': float(payload[0])
    }

# Define WebSocket callback functions
def ws_message(ws, message):
    msg = parse_message(message)
    if 'event' not in msg.keys():
        print("WebSocket thread: %s" % msg)

def ws_open(ws):
    ws.send('{"event":"subscribe", "subscription":{"name":"ohlc"}, "pair":["BTC/USD","DOGE/USD"]}')
    ws.send('{"event":"subscribe", "subscription":{"name":"trade"}, "pair":["BTC/USD","DOGE/USD"]}')

def ws_thread(ws):
    ws.run_forever()

# Start a new thread for the WebSocket interface
ws = websocket.WebSocketApp("wss://ws.kraken.com/", on_open=ws_open, on_message=ws_message)
_thread.start_new_thread(ws_thread, (ws,))

# Continue other (non WebSocket) tasks in the main thread
subscribe = False
while True:
    time.sleep(10)
    if subscribe == False:
        print("Unsubscribing")
        ws.send('{"event":"unsubscribe", "subscription":{"name":"trade"}, "pair":["DOGE/USD"]}')
        subscribe = True
    else:
        print("Subscribing")
        ws.send('{"event":"subscribe", "subscription":{"name":"trade"}, "pair":["DOGE/USD"]}')
        subscribe = False

class KrakenWebsocketClient:
    def __init__(self):
        pass