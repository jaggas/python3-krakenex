# Import WebSocket client library (and others)
import websocket
import _thread
import time
import json
import pandas as pd

from enum import Enum
from typing import Union, Any


class EventType(str, Enum):
    """Message event type. Describes contents of each message.
    https://docs.kraken.com/websockets/#message-ping
    Args:
        Enum (str): 
    """
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
        """Currency pair A/B with base currency A and quote currency B.

        Args:
            pair (str): currency pair string
        """
        pieces = pair.split("/")
        self.base = pieces[0]
        self.quote = pieces[1]

class SystemStatus(str, Enum):
    """Status of connection.
    https://docs.kraken.com/websockets/#message-systemStatus

    Args:
        Enum (str): connection system status
    """
    ONLINE = "online"
    MAINTENANCE = "maintenance"
    CANCEL_ONLY = "cancel_only"
    LIMIT_ONLY = "limit_only"
    POST_ONLY = "post_only"
    default = CANCEL_ONLY

class SubscriptionInterval(int, Enum):
    """Time frame interval in minutes.
    https://docs.kraken.com/rest/#operation/getOHLCData

    Args:
        Enum (int): width of OHLC bars
    """
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

class SubscriptionDepth(int, Enum):
    """Depth associated with book subscription in numbers of levels for each side.

    Args:
        Enum (int): order book depth. Default DEPTH_10
    """
    DEPTH_10   = 10
    DEPTH_25   = 25
    DEPTH_100  = 100
    DEPTH_500  = 500
    DEPTH_1000 = 1000
    default = DEPTH_10

class SubscriptionName(str, Enum):
    """Name of subscription.

    Args:
        Enum (str): subscription name
    """
    BOOK = "book"
    OHLC = "ohlc"
    OPEN_ORDERS = "openOrders"
    OWN_TRADES = "ownTrades"
    SPREAD = "spread"
    TICKER = "ticker"
    TRADE = "trade"
    ERROR = "error"
    default = ERROR

class SubscriptionStatus(str, Enum):
    """Status of subscription. 

    Args:
        Enum (str): status of subscription
    """
    SUBSCRIBED   = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    ERROR        = "error"
    default = ERROR

class ChannelName:
    def __init__(self, channel_name: str):
        """Parses channel name string.

        Args:
            channel_name (str): channel name
        """
        pieces = channel_name.split('-')
        self.event = EventType(pieces[0])
        self.depth = None
        self.interval = None
        if self.event == 'ohlc':
            self.interval = SubscriptionInterval(int(pieces[1]))
        elif self.event == 'book':
            self.depth = SubscriptionDepth(int(pieces[1]))

    def __repr__(self) -> dict:
        return json.dumps(self.__dict__)
class OrderSide(Enum):
    """Order side.

    Args:
        Enum (str): orders side
    """
    BUY = "buy"
    SELL = "sell"
    ERROR = "error"
    default = ERROR

class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop-loss"
    TAKE_PROFIT = "take-profit"
    STOP_LOSS_LIMIT = "stop-loss-limit"
    TAKE_PROFIT_LIMIT = "take-profit-limit"
    SETTLE_POSITION = "settle-position"
    ERROR = "error"
    default = ERROR

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
    def __init__(self, name: SubscriptionName):
        pass


class ArrayMessage:
    def __init__(self, payload: list):
        self.pair = AssetPair(payload[-1])
        self.channel = ChannelName(payload[-2])
        self._raw = payload[1]

class TickerMessage(ArrayMessage):
    def __init__(self, payload: list):
        super().__init__(payload)

        self.ask = {
            "price": float(self._raw['a'][0]),
            "wholeLotVolume": int(self._raw['a'][1]),
            "lotVolume": float(self._raw['a'][2])
        }

        self.bid = {
            "price": float(self._raw['b'][0]),
            "wholeLotVolume": int(self._raw['b'][1]),
            "lotVolume": float(self._raw['b'][2])
        }

        self.open = {
            "today": float(self._raw['o'][0]),
            "last24Hours": float(self._raw['o'][1])
        }

        self.high = {
            "today": float(self._raw['h'][0]),
            "last24Hours": float(self._raw['h'][1])
        }

        self.low = {
            "today": float(self._raw['l'][0]),
            "last24Hours": float(self._raw['l'][1])
        }

        self.close = {
            "price": float(self._raw['c'][0]),
            "lotVolume": float(self._raw['c'][1])
        }

        self.volume = {
            "today": float(self._raw['v'][0]),
            "last24Hours": float(self._raw['v'][1])
        }

        # Number of trades
        self.trades = {
            "today": float(self._raw['t'][0]),
            "last24Hours": float(self._raw['t'][1])
        }

        # Volume weighted average price
        self.vwap = {
            "today": float(self._raw['p'][0]),
            "last24Hours": float(self._raw['p'][1])
        }

class OHLCMessage(ArrayMessage):
    def __init__(self, payload: list):
        super().__init__(payload)
        self.start_time = pd.to_datetime(float(self._raw[0]), unit='s')
        self.end_time = pd.to_datetime(float(self._raw[1]), unit='s')
        self.open = float(self._raw[2])
        self.high = float(self._raw[3])
        self.low = float(self._raw[4])
        self.close = float(self._raw[5])
        self.vwap = float(self._raw[6])
        self.volume = float(self._raw[7])
        self.count = int(self._raw[8])

class TradeMessage(ArrayMessage):
    def __init__(self, payload: list):
        super().__init__(payload)
        self.price = float(self._raw[0])
        self.price = float(self._raw[1])
        self.time = pd.to_datetime(float(self._raw[2]), unit='s')
        self.side = OrderSide.BUY if self._raw[3].startswith('b') else OrderSide.SELL
        order_type = self._raw[4]
        if order_type.startswith('m'):
            self.order_type = OrderType.MARKET
        elif order_type.startswith('l'):
            self.order_type = OrderType.LIMIT
        else:
            self.order_type = OrderType.ERROR
        print()

def parse_message2(msg: str):
    try:
        d = json.loads(msg)
        if isinstance(d, dict):
            event = EventType(d['event'])
            if event == EventType.PONG:
                pass
            elif event == EventType.HEARTBEAT:
                pass
            elif event == EventType.SYSTEM_STATUS:
                pass
            elif event == EventType.SUBSCRIPTION_STATUS:
                pass
            else:
                pass
        elif isinstance(d, list):
            channel = ChannelName(d[-2])
            event = channel.event
            if event == EventType.TICKER:
                TickerMessage(d)
            elif event == EventType.OHLC:
                ohlc = OHLCMessage(d)
            elif event == EventType.TRADE:
                trade = TradeMessage(d)
                print(trade.__dict__)
            elif event == EventType.SPREAD:
                pass
            elif event == EventType.BOOK:
                pass
        else:
            pass
    except Exception as e:
        print(e)


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
    msg = parse_message2(message)
    if 'event' not in msg.keys():
        print("WebSocket thread: %s" % msg)

def ws_open(ws):
    ws.send('{"event":"subscribe", "subscription":{"name":"ohlc", "interval": 5}, "pair":["BTC/USD","DOGE/USD"]}')
    ws.send('{"event":"subscribe", "subscription":{"name":"book"}, "pair":["BTC/USD","DOGE/USD"]}')
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